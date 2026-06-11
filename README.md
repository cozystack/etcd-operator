# etcd-operator

A Kubernetes operator for running [etcd](https://etcd.io/) clusters. Status: **early alpha** — API is `etcd-operator.cozystack.io/v1alpha2` and will likely change.

## What it does

The operator manages etcd clusters via two custom resources:

- **`EtcdCluster`** — what the user creates. Captures cluster-wide intent: replica count, etcd version, per-member storage size, a progress deadline.
- **`EtcdMember`** — what the operator creates. One per etcd member. Owns its Pod and PVC. Operator-managed; users should not edit these directly.

There is no StatefulSet. Each member's Pod and PVC are reconciled independently so the operator can model protocol-aware lifecycle (learner-mode joins, member-id assignment, graceful removal, scale-to-zero pause/resume) without fighting StatefulSet's "all replicas are one workload" assumption.

The full design rationale is in [docs/concepts.md](docs/concepts.md).

## What's supported today

- **Bootstrap** of new clusters. Single seed first, learner-mode adds afterwards.
- **Scale up / down**: cluster controller adds members one at a time as learners and promotes them; scale-down picks the most-recently-created member, runs `MemberRemove` via a finalizer, then GCs the Pod and PVC.
- **Scale to zero (pause/resume)**: `spec.replicas: 0` parks the surviving member via `spec.dormant=true`; the Pod is deleted, the PVC stays owned by the `EtcdMember`. Scaling back up to ≥ 1 flips `spec.dormant=false` on the same member; etcd resumes from the existing data dir with the same cluster ID and member ID.
- **Pod restart / node failure**: data PVC is preserved, the new Pod reads the existing WAL and rejoins with the same member ID.
- **Memory-backed storage (opt-in)**: `spec.storage.medium: Memory` switches each member's data dir to a tmpfs `emptyDir` whose lifetime is bound to the Pod. Members that lose their Pod (eviction, node failure) lose their data; the operator detects this, removes the member from etcd, and replaces it via the existing scale-up path. Suits scenarios where the etcd state is reconstructable and replication absorbs single-member losses. For production, set `spec.affinity` and `spec.resources.limits.memory` explicitly — neither is defaulted ([#16](https://github.com/lllamnyp/etcd-operator/issues/16)); see [docs/concepts.md](docs/concepts.md#storage).
- **Apiserver-enforced validation**: CEL rules on the CRD (k8s 1.29+) reject `replicas: 0` with `storage.medium: Memory`, `storage.size: 0` with `storage.medium: Memory`, `storage.medium` changes after creation, and `storage.size` shrinks. No webhook / cert-manager dependency.
- **PodDisruptionBudget**: per-cluster PDB selects voting members only (`role=voter`); `maxUnavailable = (voters-1)/2` so `kubectl drain` cannot voluntarily push the cluster below quorum.
- **TLS (BYO Secrets or cert-manager)**: `spec.tls.client` / `spec.tls.peer` enable TLS on each surface independently. Material comes from either user-provided Secrets (`serverSecretRef` / `operatorClientSecretRef` / `secretRef`) or operator-emitted `cert-manager.io/v1` Certificates (`certManager.{serverIssuerRef,operatorClientIssuerRef,issuerRef}`) — mutually exclusive per subtree, enforced by CEL. mTLS is the implicit mode when an operator-client source is supplied; server-TLS-only when it isn't. The whole `tls` subtree is CEL-locked immutable post-create. cert-manager-emitted certs auto-renew via cert-manager; Pod-side rotation is a manual one-at-a-time `kubectl delete pod` either way. See [docs/concepts.md](docs/concepts.md#tls).
- **Resource sizing**: `spec.resources` (a `corev1.ResourceRequirements`) sets the etcd container's CPU/memory requests and limits. Unset uses a conservative 100m/128Mi-request default. Updates take effect on newly-created members; pair with a `VerticalPodAutoscaler` targeting the cluster for live recommendation/rollout.
- **Scheduling & extra metadata**: `spec.affinity` and `spec.topologySpreadConstraints` pass through to every member Pod (anti-affinity is not defaulted — set it for production); `spec.additionalMetadata` merges user labels/annotations onto every object the operator creates (member Pods, data PVCs, Services, PDB, `EtcdMember` CRs), with operator-owned keys winning on collision. All three apply on object creation and are latched like the rest of the spec. See [docs/concepts.md](docs/concepts.md#pod-scheduling-and-additional-metadata).
- **Monitoring / autoscaling hooks**: every member Pod always exposes a plaintext `metrics` container port at `2381` (etcd's `/health` + Prometheus `/metrics`) for `VMPodScrape` / `PodMonitor`. The `EtcdCluster` CRD exposes the `/scale` subresource with a populated `status.selector`, making it a valid target for `kubectl scale` and `VerticalPodAutoscaler.targetRef`.
- **Locking pattern**: `status.observed` snapshots the in-flight target so mid-flight spec edits don't corrupt consensus; `progressDeadline` bounds how long the operator will spend trying to reach a target.
- **Cluster deletion**: cascading owner refs clean up everything; finalizers detect "the whole cluster is going away" and skip etcd-side removal to avoid deadlock.
- **Snapshots & restore**: `EtcdSnapshot` captures a one-shot snapshot of a cluster to S3 (or a PVC) via a Job running the operator image as a snapshot agent; `status.artifact` records the stored object's URI, size, and checksum. A new cluster restores from a snapshot at first bootstrap via `spec.bootstrap.restore.source` (the seed Pod runs a restore initContainer before etcd starts). TLS and `spec.auth` auth are honored automatically. No scheduled snapshots (`EtcdSnapshotSchedule` is intentionally out of scope) — drive recurring snapshots with a `CronJob`/`kubectl apply` from outside. See [docs/concepts.md](docs/concepts.md#snapshots--restore) and the [restore runbook](docs/operations.md#restoring-a-cluster-from-a-snapshot).

## What's not supported (yet)

No multi-user / per-tenant RBAC inside etcd — single-user `root` auth is available via `spec.auth.enabled` (BYO credentials Secret; see [docs/concepts.md](docs/concepts.md#authentication)), but every authenticated client is `root`. No in-place version upgrades (changing `spec.version` only affects newly-created members). No PVC resizing — see [#2](https://github.com/lllamnyp/etcd-operator/issues/2). No automatic broken-member replacement for PVC-backed clusters (memory-backed members do auto-replace on Pod loss; `status.brokenMembers` reads 0 in practice — see [docs/concepts.md](docs/concepts.md#storage)). One-shot snapshots and restore-on-bootstrap are supported (see above), but there is no *scheduled* snapshot CRD. No defragmentation scheduling. PodAntiAffinity is supported via `spec.affinity` but not applied by default (defaulting tracked in [#16](https://github.com/lllamnyp/etcd-operator/issues/16)). See the [issue tracker](https://github.com/lllamnyp/etcd-operator/issues) for the running follow-up list.

## Quick start

```sh
# 1. Install the operator (CRDs + RBAC + manager) with Helm. Builds an image and
#    pushes it to your registry; substitute IMG= for a prebuilt tag if you have
#    one. The cluster must be able to pull from <your-registry> — for local
#    clusters (kind / minikube / k3d) sideload the image or use an ephemeral
#    registry such as ttl.sh, otherwise the Deployment sits in ImagePullBackOff.
#    `make deploy` runs `helm upgrade --install` (needs helm v3.16+ on PATH).
make docker-build docker-push deploy IMG=<your-registry>/etcd-operator:<tag>

# 2. Create a cluster.
cat <<'EOF' | kubectl apply -f -
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: my-etcd
  namespace: default
spec:
  replicas: 3
  version: 3.6.11
  storage:
    size: 1Gi
EOF

# 3. Wait for ready and inspect.
kubectl get etcdcluster.etcd-operator.cozystack.io my-etcd -w
POD=$(kubectl get pod -l etcd-operator.cozystack.io/cluster=my-etcd \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it "$POD" -- etcdctl --endpoints=http://localhost:2379 \
  member list -w table
```

Member names are apiserver-assigned (`GenerateName="<cluster>-"`) — don't hard-code them; use the cluster label selector.

For step-by-step setup, RBAC, image versions, and teardown see [docs/installation.md](docs/installation.md).

## Documentation

- **[Installation](docs/installation.md)** — deploy the operator, create your first cluster, networking pitfalls, upgrades.
- **[Concepts](docs/concepts.md)** — design rationale: locking pattern, single-seed bootstrap, GenerateName naming, scale-to-zero mechanics, conditions reference.
- **[Operations](docs/operations.md)** — runbook for day-2: scaling, pausing/resuming, decoding conditions, escalating stuck reconciles, broken-member recovery.
- **[Migration](docs/migration.md)** — moving onto this operator from the legacy aenix operator; tracks behavioural changes that need an explicit migration step — currently the BYO root-credentials requirement when enabling auth.

## Testing

```sh
go test ./controllers/...
```

The suite uses controller-runtime's fake client and a fake etcd client; no envtest assets needed at the unit level. Pinned behaviours:

- **Bootstrap** — single-seed creation, idempotent recovery, `GenerateName`-assigned names.
- **Locking pattern** — `status.observed` / `progressDeadline` lock the in-flight target; bootstrap-deadline is terminal.
- **Scale up** — learner-mode add, readiness gate before the next step, crash-recovery branches between `Create` / `MemberAddAsLearner` / `Patch(initialCluster)`.
- **Scale down** — `CreationTimestamp` DESC (name DESC tiebreak) victim selection, finalizer-driven `MemberRemove`.
- **Scale to zero** — 1→0 Patches `spec.dormant=true`; 0→1 flips it back; dormant member's Pod is gone but its PVC is preserved.
- **Discovery** — seed found via `spec.bootstrap=true`; etcd client endpoints filtered to voters (`MemberReady=True`) so `MemberList` doesn't route to a learner.
- **Status no-churn** — steady-state reconciles don't repeatedly mutate status.

## License

Apache 2.0. See `LICENSE`.
