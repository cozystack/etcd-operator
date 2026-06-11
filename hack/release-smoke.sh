#!/usr/bin/env bash
# Release-install smoke test: exercises a release install path end to end on a
# throwaway kind cluster, then proves the installed operator actually works.
#
#   1. build the operator image at $IMG          (what docker-publish.yml ships)
#   2. kind load it                              (stand-in for the GHCR push/pull)
#   3. install it one of two ways (INSTALL_MODE):
#        manifest (default) — make build-dist-manifests + kubectl apply, the
#                             path release-assets.yml ships
#        helm               — helm install charts/etcd-operator, the path
#                             helm-publish.yml ships
#   4. assert the operator Deployment goes Available
#   5. create a 1-node EtcdCluster and assert it reaches READY
#
# Why this is the right test (vs. grepping the workflow/chart files): the
# contract under test is "the image the release publishes == the image the
# install deploys, and that artifact actually runs and reconciles". The single
# $IMG threaded through build, load, and install makes a tag mismatch
# impossible by construction; step 4 is where a broken mismatch WOULD surface
# (wrong tag => ImagePullBackOff => never Available). It also catches subtler
# failures static checks can't: a broken OPERATOR_IMAGE wiring (the operator
# refuses to start on the placeholder) fails step 4, and a missing RBAC rule in
# the chart fails step 5 (the cluster never goes READY).
set -euo pipefail

# Always build/load for the host architecture; a stray
# DOCKER_DEFAULT_PLATFORM=linux/amd64 would pull an emulated kind node whose
# control plane never goes healthy. (Same rationale as hack/e2e.sh.)
unset DOCKER_DEFAULT_PLATFORM

# ── Pinned component versions (kept in sync with hack/e2e.sh) ─────────────
KIND_VERSION=v0.32.0
KIND_NODE_IMAGE=kindest/node:v1.34.0

INSTALL_MODE=${INSTALL_MODE:-manifest}   # manifest | helm
KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-etcd-operator-release-smoke-$INSTALL_MODE}
NAMESPACE=etcd-operator-system
# A registry-qualified, non-:latest tag: mirrors the real release ref
# (ghcr.io/<owner>/etcd-operator:<tag>) and, being non-latest, makes the
# default imagePullPolicy IfNotPresent — so the kind-loaded image is used
# instead of attempting a registry pull.
IMG=${IMG:-ghcr.io/cozystack/etcd-operator:v0.0.0-smoke}
KEEP_CLUSTER=${KEEP_CLUSTER:-}

case "$INSTALL_MODE" in
    manifest|helm) ;;
    *) echo "ERROR: INSTALL_MODE must be 'manifest' or 'helm', got '$INSTALL_MODE'"; exit 2 ;;
esac

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
LOCALBIN="$ROOT/bin"
mkdir -p "$LOCALBIN"
export PATH="$LOCALBIN:$PATH"

if ! command -v kind >/dev/null 2>&1; then
    echo "--- installing kind $KIND_VERSION into $LOCALBIN"
    GOBIN="$LOCALBIN" go install sigs.k8s.io/kind@"$KIND_VERSION"
fi
if [ "$INSTALL_MODE" = helm ] && ! command -v helm >/dev/null 2>&1; then
    echo "ERROR: INSTALL_MODE=helm requires helm on PATH"; exit 2
fi

dump_diagnostics() {
    echo "--- release smoke ($INSTALL_MODE) failed; dumping cluster state before teardown"
    kubectl get etcdclusters,etcdmembers,pods -A || true
    kubectl -n "$NAMESPACE" get deploy -o wide || true
    kubectl -n "$NAMESPACE" describe deploy || true
    kubectl -n "$NAMESPACE" logs -l app.kubernetes.io/name=etcd-operator \
        --all-containers --tail=200 || true
    kubectl -n "$NAMESPACE" logs -l control-plane=controller-manager \
        --all-containers --tail=200 || true
    # The most informative failure signals: an ImagePullBackOff (tag mismatch)
    # or the operator's OPERATOR_IMAGE-placeholder refusal show up here.
    kubectl -n "$NAMESPACE" get events --sort-by=.lastTimestamp | tail -30 || true
    for p in $(kubectl get pods -l etcd-operator.cozystack.io/cluster=smoke -o name 2>/dev/null); do
        echo "--- logs: $p"
        kubectl logs "$p" --all-containers --tail=100 || true
    done
}

cleanup() {
    status=$?
    [ "$status" -ne 0 ] && dump_diagnostics
    if [ -n "$KEEP_CLUSTER" ]; then
        echo "--- KEEP_CLUSTER set; kind cluster '$KIND_CLUSTER_NAME' left running"
        return
    fi
    echo "--- deleting kind cluster '$KIND_CLUSTER_NAME'"
    kind delete cluster --name "$KIND_CLUSTER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "--- creating kind cluster '$KIND_CLUSTER_NAME' ($KIND_NODE_IMAGE)"
kind create cluster --name "$KIND_CLUSTER_NAME" --image "$KIND_NODE_IMAGE" --wait 5m
kubectl config use-context "kind-$KIND_CLUSTER_NAME"

echo "--- building operator image ($IMG) and loading it into kind"
docker build -t "$IMG" .
kind load docker-image "$IMG" --name "$KIND_CLUSTER_NAME"

if [ "$INSTALL_MODE" = manifest ]; then
    echo "--- [manifest] rendering release install manifests (IMG=$IMG)"
    make build-dist-manifests IMG="$IMG"
    # build-dist-manifests must render purely (it is `helm template` piped through
    # yq): it writes only dist/ (gitignored) and must not mutate any tracked file.
    # A dirty tree here means a regression reintroduced an in-place edit, which
    # would also spuriously trip ci.yml's codegen-drift gate. Assert cleanliness
    # (skipped if this isn't a git checkout, e.g. a release tarball).
    if command -v git >/dev/null 2>&1 && git rev-parse --git-dir >/dev/null 2>&1 \
        && [ -n "$(git status --porcelain --untracked-files=no)" ]; then
        echo "ERROR: 'make build-dist-manifests' modified tracked files (it must render purely):"
        git --no-pager status --porcelain --untracked-files=no
        git --no-pager diff
        exit 1
    fi
    echo "--- [manifest] installing from the rendered release manifest"
    # Server-side apply: the consolidated manifest embeds the full CRD schemas,
    # whose size can exceed the client-side last-applied-config annotation
    # limit. This is also the documented release-install path.
    kubectl apply --server-side -f dist/etcd-operator.yaml
else
    echo "--- [helm] installing the chart (image=$IMG)"
    # CRDs are templated into the chart and committed (drift-gated), so no sync
    # step is needed. Split $IMG into repo:tag for image.repository / image.tag.
    helm upgrade --install etcd-operator charts/etcd-operator \
        --namespace "$NAMESPACE" --create-namespace \
        --set image.repository="${IMG%:*}" \
        --set image.tag="${IMG##*:}" \
        --wait --timeout 5m
fi

echo "--- waiting for the operator to become Available"
# Fails (times out) on either a tag mismatch (ImagePullBackOff) or a broken
# OPERATOR_IMAGE substitution (operator refuses to start on the placeholder).
# Select by the label both install paths set, so this is mode-agnostic.
kubectl -n "$NAMESPACE" wait deploy \
    -l control-plane=controller-manager \
    --for=condition=Available --timeout=5m

echo "--- bootstrapping a 1-node EtcdCluster to prove the operator reconciles"
kubectl apply -f - <<'EOF'
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: smoke
  namespace: default
spec:
  replicas: 1
  version: 3.6.11
  storage:
    size: 256Mi
EOF

echo "--- waiting for EtcdCluster 'smoke' to reach READY=1"
# Poll readyMembers rather than `kubectl wait --for=condition`: the cluster's
# Available condition may not be registered on the object until the first
# status write, which makes an early `wait` error out ("no matching condition").
deadline=$(( $(date +%s) + 300 ))
until [ "$(kubectl get etcdcluster smoke -o jsonpath='{.status.readyMembers}' 2>/dev/null || echo 0)" = "1" ]; do
    if [ "$(date +%s)" -ge "$deadline" ]; then
        echo "ERROR: EtcdCluster 'smoke' did not reach READY=1 within 5m"
        exit 1
    fi
    sleep 5
done

echo "--- release-install smoke PASSED (mode=$INSTALL_MODE, operator Available, cluster READY=1, IMG=$IMG)"
