#!/usr/bin/env bash
# Live migration demo against the dev-stand (talm) cluster — NOT kind.
#
# Adapted for a shared Cozystack cluster that already runs the real legacy
# operator (cozy-etcd-operator) managing live tenant etcds. Hard safety rules
# baked in:
#   * etcd-migrate is ALWAYS run with -n demo-migration (never cluster-wide),
#     so it can only ever touch the throwaway demo cluster, never tenant-*.
#   * the legacy operator is only scaled to 0 for the brief migration window
#     (its HelmRelease is already suspend=true, so Flux won't fight us) and is
#     restored to 1 by the cleanup trap no matter how the script exits.
#   * the new operator installs with crds.enabled=false (the cozystack CRDs are
#     shared and already present) under a distinct release name so its
#     cluster-scoped RBAC can't collide with the legacy release.
#   * cleanup runs on ANY exit (success, error, Ctrl-C) and tears down only the
#     demo namespaces; it never deletes shared CRDs or touches tenant-* clusters.
#
set -uo pipefail   # NOT -e: we want cleanup to run even past a failed step

DEMO_NS=demo-migration
CLUSTER=demo

# Phase 5 (kill a member, watch a native demo-<hash> replacement join) is OFF by
# default: it does NOT work on a default legacy cluster. The legacy operator
# forces etcd --peer-auto-tls (self-signed, no shared CA) and the new operator
# has no auto-tls mode, so a replacement member comes up plaintext and can't
# join the still-TLS peers (CrashLoopBackOff — this is exactly what etcd-migrate
# now warns about). Making it work needs FULL mTLS on the legacy cluster (server
# + operator-client + peer certs from a shared CA; peer-only is impossible
# because the legacy operator requires a client cert whenever any security.tls
# is set). The migration itself (Phases 1-4) works cleanly without any of that.
RUN_KILL=${RUN_KILL:-}   # set RUN_KILL=1 only on a full-mTLS legacy cluster
NEW_OP_NS=etcd-operator-demo
NEW_RELEASE=etcd-operator-demo           # -> deploy etcd-operator-demo, RBAC etcd-operator-demo-*
NEW_DEPLOY=etcd-operator-demo
IMG=ghcr.io/cozystack/etcd-operator:v0.5.0   # public release image; the cluster pulls it directly
LEGACY_NS=cozy-etcd-operator
LEGACY_DEPLOY=etcd-operator-controller-manager

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$ROOT/bin:$PATH"

banner() { printf '\n\033[1;36m========== %s ==========\033[0m\n' "$1"; }
note()   { printf '\033[1;33m%s\033[0m\n' "$1"; }

# Interactive step gate. Auto-skips when NONINTERACTIVE=1 or stdin is not a TTY
# (background/piped runs), so automated runs never hang on read.
pause() {
    { [ -n "${NONINTERACTIVE:-}" ] || [ ! -t 0 ]; } && return 0
    printf '\n\033[1;32m>>> NEXT: %s\033[0m\n    [Enter to continue, Ctrl-C to abort] ' "$1"
    read -r _ || true
}

# Echo a command in blue, then run it. Plain commands only — no pipes, heredocs
# or redirections (those are shell syntax, not args, and won't be captured).
run() {
    printf '\033[1;34m$ %s\033[0m\n' "$*"
    "$@"
}

etcdctl_any() {
    local pod
    pod=$(kubectl -n "$DEMO_NS" get pod \
        -l etcd-operator.cozystack.io/cluster="$CLUSTER" \
        -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
    [ -z "$pod" ] && { echo "(no member pod)"; return 1; }
    run kubectl -n "$DEMO_NS" exec "$pod" -- \
        etcdctl --endpoints=http://127.0.0.1:2379 "$@"
}

wait_ready() {   # wait_ready <target-readyMembers> <timeout-sec>
    local target=$1 deadline=$(( $(date +%s) + $2 ))
    until [ "$(kubectl -n "$DEMO_NS" get etcdcluster.etcd-operator.cozystack.io "$CLUSTER" \
            -o jsonpath='{.status.readyMembers}' 2>/dev/null || echo 0)" = "$target" ]; do
        [ "$(date +%s)" -ge "$deadline" ] && { echo "TIMEOUT waiting READY=$target"; return 1; }
        sleep 5
    done
}

cleanup() {
    banner "CLEANUP (runs on any exit)"
    # 1) Restore the legacy operator FIRST — re-arms reconciliation for the
    #    real tenant etcds as fast as possible.
    note "--- restoring legacy operator $LEGACY_NS/$LEGACY_DEPLOY to replicas=1"
    kubectl -n "$LEGACY_NS" scale deploy "$LEGACY_DEPLOY" --replicas=1 2>/dev/null || true

    # 2) Make sure the new operator is up so cozystack finalizers can drain AND
    #    so the cluster stays live for inspection if the operator kept it.
    kubectl -n "$NEW_OP_NS" scale deploy "$NEW_DEPLOY" --replicas=1 2>/dev/null || true

    # Interactive runs: offer to KEEP the demo resources for debugging (e.g. a
    # CrashLoopBackOff replacement member) instead of tearing them down. The
    # controllers above are already restored, so keeping is safe for tenants.
    if { [ -z "${NONINTERACTIVE:-}" ] && [ -t 0 ]; }; then
        printf '\n\033[1;31m>>> Tear down %s + new operator now?\033[0m\n' "$DEMO_NS"
        printf '    [Enter = tear down]   [type k then Enter = KEEP for inspection] '
        read -r _ans || true
        if [ "$_ans" = k ]; then
            note "--- KEEPING demo resources. Inspect, then clean up manually:"
            echo "      kubectl -n $DEMO_NS get etcdmember.etcd-operator.cozystack.io,pods -o wide"
            echo "      kubectl -n $DEMO_NS logs <crashing-pod>            # e.g. the new demo-<hash>"
            echo "      kubectl -n $DEMO_NS describe pod <crashing-pod>"
            echo "    teardown when done:"
            echo "      kubectl delete ns $DEMO_NS; helm uninstall $NEW_RELEASE -n $NEW_OP_NS; kubectl delete ns $NEW_OP_NS"
            banner "END (demo resources KEPT for inspection; legacy operator restored)"
            return
        fi
    fi

    # 3) Delete the demo cluster CRs (both API groups in case of partial state),
    #    then the namespace. Give finalizers a moment.
    note "--- deleting demo cluster + namespace $DEMO_NS"
    kubectl -n "$DEMO_NS" delete etcdclusters.etcd-operator.cozystack.io --all --ignore-not-found --timeout=90s 2>/dev/null || true
    kubectl -n "$DEMO_NS" delete etcdclusters.etcd.aenix.io --all --ignore-not-found --timeout=90s 2>/dev/null || true
    kubectl delete ns "$DEMO_NS" --ignore-not-found --timeout=120s 2>/dev/null || true
    # Backstop: if the ns is wedged on member finalizers, strip them.
    if kubectl get ns "$DEMO_NS" >/dev/null 2>&1; then
        note "--- ns stuck; stripping finalizers on leftover etcdmembers"
        for m in $(kubectl -n "$DEMO_NS" get etcdmembers.etcd-operator.cozystack.io -o name 2>/dev/null); do
            kubectl -n "$DEMO_NS" patch "$m" --type=merge -p '{"metadata":{"finalizers":null}}' 2>/dev/null || true
        done
        kubectl delete ns "$DEMO_NS" --ignore-not-found --timeout=120s 2>/dev/null || true
    fi

    # 4) Remove the new operator (leaves the SHARED cozystack CRDs untouched —
    #    crds.enabled=false means this release never owned them).
    note "--- uninstalling new operator + namespace $NEW_OP_NS"
    helm uninstall "$NEW_RELEASE" -n "$NEW_OP_NS" 2>/dev/null || true
    kubectl delete ns "$NEW_OP_NS" --ignore-not-found --timeout=120s 2>/dev/null || true

    note "--- cleanup done. Legacy operator restored; shared CRDs/tenants untouched."
    banner "END"
}
trap cleanup EXIT

# ── Phase 1: legacy PVC-backed cluster (the running legacy operator builds it) ─
banner "Phase 1 — legacy v1alpha1 cluster in $DEMO_NS"
printf '\033[1;34m$ kubectl create namespace %s\033[0m\n' "$DEMO_NS"
kubectl create namespace "$DEMO_NS" --dry-run=client -o yaml | kubectl apply -f -

printf '\033[1;34m$ kubectl apply -f -   # EtcdCluster etcd.aenix.io/v1alpha1 %s (replicas=3, 1Gi PVC)\033[0m\n' "$CLUSTER"
kubectl apply -f - <<EOF
apiVersion: etcd.aenix.io/v1alpha1
kind: EtcdCluster
metadata:
  name: ${CLUSTER}
  namespace: ${DEMO_NS}
spec:
  replicas: 3
  storage:
    volumeClaimTemplate:
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 1Gi
EOF

# The legacy operator needs a few reconcile seconds to create the StatefulSet;
# `rollout status` errors hard if the STS does not exist yet, so wait for it
# to appear first (also surfaces a wedged/non-reconciling legacy operator).
note "--- waiting for the legacy operator to create StatefulSet ${CLUSTER}"
sts_deadline=$(( $(date +%s) + 120 ))
until kubectl -n "$DEMO_NS" get statefulset "$CLUSTER" >/dev/null 2>&1; do
    [ "$(date +%s)" -ge "$sts_deadline" ] && { echo "legacy operator did not create StatefulSet ${CLUSTER} within 120s (is it reconciling?)"; exit 1; }
    sleep 3
done
note "--- waiting for the legacy StatefulSet ${CLUSTER}-0/1/2 to be Ready"
run kubectl -n "$DEMO_NS" rollout status "statefulset/${CLUSTER}" --timeout=5m || { echo "legacy cluster failed to form"; exit 1; }
run kubectl -n "$DEMO_NS" get pod -o wide

note "--- writing demo data"
run kubectl -n "$DEMO_NS" exec "${CLUSTER}-0" -- etcdctl --endpoints=http://127.0.0.1:2379 put /demo/hello world
run kubectl -n "$DEMO_NS" exec "${CLUSTER}-0" -- etcdctl --endpoints=http://127.0.0.1:2379 endpoint status -w table --cluster

# ── Phase 2: install the new operator (release image from GHCR) ───────────────
pause "Phase 2 — install the new operator ($NEW_RELEASE) + build the migrate CLI"
banner "Phase 2 — install new operator ($NEW_RELEASE) + build migrate CLI"
run helm upgrade --install "$NEW_RELEASE" charts/etcd-operator \
    -n "$NEW_OP_NS" --create-namespace \
    --set image.repository="${IMG%:*}" \
    --set image.tag="${IMG##*:}" \
    --set crds.enabled=false \
    --wait --timeout 5m || { echo "new operator install failed"; exit 1; }
run kubectl -n "$NEW_OP_NS" get deploy "$NEW_DEPLOY"

note "--- building etcd-migrate (go build; no docker)"
run make etcd-migrate
run bin/etcd-migrate version

# ── Phase 3: migrate (scope strictly to $DEMO_NS) ─────────────────────────────
pause "Phase 3 — scale both controllers to 0 (incl. the real legacy operator) and migrate $DEMO_NS"
banner "Phase 3 — migrate $DEMO_NS"
note "--- scaling both etcd controllers to 0 (legacy HR is already suspended; etcd pods keep serving)"
run kubectl -n "$LEGACY_NS" scale deploy "$LEGACY_DEPLOY" --replicas=0
run kubectl -n "$NEW_OP_NS"  scale deploy "$NEW_DEPLOY"   --replicas=0
run kubectl -n "$LEGACY_NS" rollout status deploy/"$LEGACY_DEPLOY" --timeout=2m
run kubectl -n "$NEW_OP_NS"  rollout status deploy/"$NEW_DEPLOY"   --timeout=2m

MIGRATE_FLAGS=(-n "$DEMO_NS"
    --legacy-controller "${LEGACY_NS}/${LEGACY_DEPLOY}"
    --new-controller    "${NEW_OP_NS}/${NEW_DEPLOY}")

note "--- dry-run (mutates nothing)"
run bin/etcd-migrate "${MIGRATE_FLAGS[@]}" || { echo "dry-run failed"; exit 1; }

note "--- APPLY (--skip-backup is for THIS DEMO ONLY)"
run bin/etcd-migrate "${MIGRATE_FLAGS[@]}" --apply --skip-backup --yes || { echo "apply failed"; exit 1; }

note "--- scaling the new operator up to take ownership"
run kubectl -n "$NEW_OP_NS" scale deploy "$NEW_DEPLOY" --replicas=1
run kubectl -n "$NEW_OP_NS" rollout status deploy/"$NEW_DEPLOY" --timeout=2m
# Restore the legacy operator immediately — tenant reconciliation no longer needs
# to wait for the rest of the demo.
note "--- restoring legacy operator to replicas=1 (tenant reconciliation resumes)"
run kubectl -n "$LEGACY_NS" scale deploy "$LEGACY_DEPLOY" --replicas=1

# ── Phase 4: verify the migrated cluster ──────────────────────────────────────
pause "Phase 4 — verify the migrated cluster (READY=3, quorum, data)"
banner "Phase 4 — verify migrated cluster"
run kubectl -n "$DEMO_NS" get etcdcluster.etcd-operator.cozystack.io "$CLUSTER"
run kubectl -n "$DEMO_NS" get etcdmember.etcd-operator.cozystack.io
note "--- waiting for READY=3"
wait_ready 3 300 || { echo "did not reach READY=3"; exit 1; }
note "--- quorum + data survived (adopted in place; cluster ID preserved)"
etcdctl_any endpoint health --cluster
printf -- "--- /demo/hello = "; etcdctl_any get /demo/hello --print-value-only

# ── Phase 5: kill one member -> native demo-<hash> replacement, quorum holds ──
# OFF by default — only works on a full-mTLS legacy cluster (see RUN_KILL note).
if [ -z "$RUN_KILL" ]; then
    banner "Phase 5 — SKIPPED (migration demo complete)"
    note "Member-replacement is skipped: a default legacy cluster runs etcd --peer-auto-tls,"
    note "which the new operator can't reproduce, so a replacement member can't rejoin."
    note "Set RUN_KILL=1 on a full-mTLS legacy cluster to exercise it."
else
pause "Phase 5 — kill one member and watch the operator replace it"
banner "Phase 5 — kill one member"
VICTIM="${CLUSTER}-1"
note "--- members before:"; run kubectl -n "$DEMO_NS" get etcdmember.etcd-operator.cozystack.io
note "--- deleting EtcdMember '$VICTIM' (the CR, not the Pod): MemberRemove + scale-up via GenerateName"
run kubectl -n "$DEMO_NS" delete etcdmember.etcd-operator.cozystack.io "$VICTIM"
# Don't trust readyMembers alone right after the delete: it still reads 3 until
# the operator observes the removal (3->2), so a naive wait matches the STALE
# count and exits before the replacement has even joined. Require the victim CR
# gone AND exactly 3 EtcdMembers all Ready=True — i.e. the fresh demo-<hash> has
# joined as a learner and been promoted to a voter.
note "--- waiting for '$VICTIM' removal + a fresh ${CLUSTER}-<hash> to join and be promoted (3 members all Ready)"
rec_deadline=$(( $(date +%s) + 300 ))
while :; do
    kubectl -n "$DEMO_NS" get etcdmember.etcd-operator.cozystack.io "$VICTIM" >/dev/null 2>&1 && victim_gone=false || victim_gone=true
    ready=$(kubectl -n "$DEMO_NS" get etcdmember.etcd-operator.cozystack.io -o json 2>/dev/null \
        | jq '[.items[]|select(.status.conditions[]?|.type=="Ready" and .status=="True")]|length' 2>/dev/null || echo 0)
    total=$(kubectl -n "$DEMO_NS" get etcdmember.etcd-operator.cozystack.io -o json 2>/dev/null | jq '.items|length' 2>/dev/null || echo 0)
    [ "$victim_gone" = true ] && [ "$ready" = 3 ] && [ "$total" = 3 ] && break
    [ "$(date +%s)" -ge "$rec_deadline" ] && { echo "did not converge to 3 ready members after killing $VICTIM (victim_gone=$victim_gone ready=$ready total=$total)"; exit 1; }
    sleep 5
done
note "--- members after ('$VICTIM' gone; a native ${CLUSTER}-<hash> took its place):"
run kubectl -n "$DEMO_NS" get etcdmember.etcd-operator.cozystack.io
run kubectl -n "$DEMO_NS" get pod -l etcd-operator.cozystack.io/cluster="$CLUSTER"
note "--- final proof: 3-member quorum healthy + data intact"
etcdctl_any member list -w table
etcdctl_any endpoint health --cluster
printf -- "--- /demo/hello = "; etcdctl_any get /demo/hello --print-value-only
fi

banner "DEMO COMPLETE — cleanup will now run automatically"
