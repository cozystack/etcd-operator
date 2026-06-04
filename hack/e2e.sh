#!/usr/bin/env bash
# End-to-end harness: provisions a kind cluster with cert-manager and Kamaji,
# builds and deploys the operator from the working tree, then runs the e2e
# suite (test/e2e). Destroys the kind cluster on exit unless KEEP_CLUSTER=1.
#
# Requirements on the host: docker, kubectl, helm, go. kind is installed
# into ./bin automatically when missing.
set -euo pipefail

# Always build/pull for the host architecture. A user-level
# DOCKER_DEFAULT_PLATFORM=linux/amd64 (common on Apple Silicon for x86-only
# tooling) would otherwise pull an emulated kind node whose control plane
# never becomes healthy.
unset DOCKER_DEFAULT_PLATFORM

# ── Pinned component versions ────────────────────────────────────────────
# v0.32.0+ required with Docker 29 ("failed to detect containerd
# snapshotter" on `kind load` with older releases).
KIND_VERSION=v0.32.0
KIND_NODE_IMAGE=kindest/node:v1.34.0
CERT_MANAGER_VERSION=v1.18.2
KAMAJI_CHART_VERSION=1.0.0
# The TenantControlPlane Kubernetes version lives in
# test/e2e/testdata/04-tenantcontrolplane.yaml and must stay within the skew
# supported by KAMAJI_CHART_VERSION.

KIND_CLUSTER_NAME=${KIND_CLUSTER_NAME:-etcd-operator-e2e}
IMG=${IMG:-etcd-operator:e2e}
KEEP_CLUSTER=${KEEP_CLUSTER:-}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
LOCALBIN="$ROOT/bin"
mkdir -p "$LOCALBIN"
export PATH="$LOCALBIN:$PATH"

if ! command -v kind >/dev/null 2>&1; then
    echo "--- installing kind $KIND_VERSION into $LOCALBIN"
    GOBIN="$LOCALBIN" go install sigs.k8s.io/kind@"$KIND_VERSION"
fi

# dump_diagnostics prints cluster state for post-mortem debugging. It MUST run
# from the EXIT trap, before the kind cluster is deleted — a separate CI step
# after this script cannot do it, because by the time the step runs the trap
# has already torn the cluster down and every kubectl call would fail.
dump_diagnostics() {
    echo "--- e2e failed; dumping cluster state before teardown"
    kubectl get etcdclusters,etcdmembers,pods,certificates,secrets -A || true
    kubectl get datastores,tenantcontrolplanes -A || true
    kubectl -n etcd-operator-system logs deploy/etcd-operator-controller-manager --tail=200 || true
    kubectl -n kamaji-system logs deploy/kamaji --tail=200 || true
    # The tenant namespace is where the longest wait (TenantControlPlane
    # Ready) fails — dump every pod's logs there, or the one failure mode
    # the suite is most likely to hit leaves no trace.
    for p in $(kubectl -n kamaji-e2e get pods -o name 2>/dev/null); do
        echo "--- logs: kamaji-e2e/$p"
        kubectl -n kamaji-e2e logs "$p" --all-containers --tail=100 || true
    done
}

cleanup() {
    status=$?
    if [ "$status" -ne 0 ]; then
        dump_diagnostics
    fi
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

echo "--- installing cert-manager $CERT_MANAGER_VERSION"
kubectl apply -f "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
kubectl -n cert-manager wait deploy --all --for=condition=Available --timeout=5m

echo "--- installing Kamaji (chart $KAMAJI_CHART_VERSION)"
# etcd.deploy=false / datastore.enabled=false: Kamaji's bundled kamaji-etcd
# stays out — the DataStore under test is the operator-managed cluster the
# suite creates (test/e2e/testdata/03-datastore.yaml), whose name the
# manager receives as its default via datastore.nameOverride.
helm repo add clastix https://clastix.github.io/charts --force-update >/dev/null
# The kamaji manager exits at startup while its default DataStore is
# missing, and the DataStore cannot be created later because the chart's
# validating webhook (failurePolicy=Fail) is served by that same crashing
# pod. Break the cycle the way the chart itself does for its bundled
# datastore (a pre-install hook): install the CRDs first and create the
# DataStore before the webhook configuration exists. The suite re-applies
# the same fixture later, which is a no-op.
helm show crds clastix/kamaji --version "$KAMAJI_CHART_VERSION" | kubectl apply -f -
kubectl apply -f test/e2e/testdata/03-datastore.yaml
helm upgrade --install kamaji clastix/kamaji \
    --version "$KAMAJI_CHART_VERSION" \
    --namespace kamaji-system --create-namespace \
    --set etcd.deploy=false \
    --set datastore.enabled=false \
    --set datastore.nameOverride=kamaji-e2e \
    --wait --timeout 5m
# NOTE: the literal "kamaji-e2e" above must stay in sync with its two other
# copies: e2eNamespace in test/e2e/kamaji_datastore_test.go and the
# metadata.namespace/DataStore name in test/e2e/testdata/*.yaml.

echo "--- building and deploying the operator ($IMG)"
docker build -t "$IMG" .
kind load docker-image "$IMG" --name "$KIND_CLUSTER_NAME"
make install deploy IMG="$IMG"
kubectl -n etcd-operator-system wait deploy/etcd-operator-controller-manager \
    --for=condition=Available --timeout=5m

echo "--- running e2e suite"
make test-e2e
