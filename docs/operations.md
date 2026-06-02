# Operations

Runbook for an operator running an `EtcdCluster` in production. Assumes k8s fluency and a working operator deployment ([installation](installation.md) covers the deploy side). Cross-references [concepts](concepts.md) for the "why".

## Daily inventory

The two queries you'll use most:

```sh
# Cluster-level view: replicas, ready count, cluster ID, conditions.
kubectl get etcdcluster.etcd-operator.cozystack.io -A
kubectl get etcdcluster.etcd-operator.cozystack.io <name> -n <ns> -o yaml

# Member-level view: per-member bootstrap flag, dormancy, ready state.
kubectl get etcdmember.etcd-operator.cozystack.io -n <ns> -o custom-columns=\
'NAME:.metadata.name,BOOTSTRAP:.spec.bootstrap,DORMANT:.spec.dormant,READY:.status.conditions[?(@.type=="Ready")].status,MEMBERID:.status.memberID'
```

Member names are apiserver-assigned random suffixes (`<cluster>-<5-char>`); don't hard-code them in scripts. Always use the cluster label:

```sh
kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns>
kubectl get pvc -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns>
```

To talk to etcd directly, pick any Pod via the label and exec:

```sh
POD=$(kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns> \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n <ns> "$POD" -- etcdctl --endpoints=http://localhost:2379 \
  endpoint health --cluster
```

## Scaling

The operator commits to a target on the first reconcile that sees the new spec — see the locking pattern in [concepts](concepts.md#locking-pattern). Spec edits during an in-flight reconcile are noticed but not acted on until the target is reached or the deadline expires.

### Scale up

```sh
kubectl patch etcdcluster.etcd-operator.cozystack.io <name> -n <ns> --type=merge \
  -p '{"spec":{"replicas":5}}'
```

The operator adds one member at a time as a learner, waits for it to report `Ready=True`, then promotes it before adding the next. Each step is gated on the previous learner reaching `Ready`. On a fresh cluster with no data each step completes in well under a second on etcd's side and the operator's reconcile cadence (~30 s requeue) dominates wall time; on clusters with non-trivial data volumes the learner-sync time can become the dominant factor (etcd has to ship the data dir before `MemberPromote` is accepted). Watch progress with:

```sh
kubectl get etcdcluster.etcd-operator.cozystack.io <name> -n <ns> \
  -o jsonpath='{.status.readyMembers}/{.status.observed.replicas}{"\n"}'
```

### Scale down

```sh
kubectl patch etcdcluster.etcd-operator.cozystack.io <name> -n <ns> --type=merge \
  -p '{"spec":{"replicas":3}}'
```

Picks the most-recently-created member as the victim (`CreationTimestamp` DESC, name DESC tiebreak). The finalizer calls `MemberRemove` against the remaining peers before the Pod and PVC are garbage-collected. No special seed-protection — the seed (the original bootstrap member) has no permanent special role and can be removed like any other member.

### Pause (scale to 0)

```sh
kubectl patch etcdcluster.etcd-operator.cozystack.io <name> -n <ns> --type=merge \
  -p '{"spec":{"replicas":0}}'
```

For an N>1 cluster this is a staged descent: each intermediate step (`MemberRemove` + Pod/PVC GC) until one member remains, then a 1→0 "pause" — the surviving member's `spec.dormant` is patched to `true`. The Pod goes away; the PVC stays owned by the `EtcdMember`, which itself stays alive. `etcdctl` from outside is no longer reachable (no Pod) but the data is intact.

Observable state once paused:

```sh
# One EtcdMember CR with spec.dormant=true:
kubectl get etcdmember.etcd-operator.cozystack.io -n <ns> \
  -o custom-columns=NAME:.metadata.name,DORMANT:.spec.dormant

# One PVC remaining (data-<member-name>):
kubectl get pvc -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns>

# No Pods:
kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns>

# Available=False, Reason=Paused, message names the PVC:
kubectl get etcdcluster.etcd-operator.cozystack.io <name> -n <ns> \
  -o jsonpath='{.status.conditions[?(@.type=="Available")]}{"\n"}'
```

### Resume (scale to 1+)

```sh
kubectl patch etcdcluster.etcd-operator.cozystack.io <name> -n <ns> --type=merge \
  -p '{"spec":{"replicas":3}}'
```

The cluster controller spots the dormant member, patches `spec.dormant=false`, and the member controller's next pass recreates the Pod against the existing PVC. Etcd reads the data dir and resumes with the **same cluster ID and member ID** as before the pause — verify with:

```sh
kubectl get etcdcluster.etcd-operator.cozystack.io <name> -n <ns> -o jsonpath='{.status.clusterID}'
# Should match the value you saw before the pause.
```

Further scale-up (1→3 in the example) proceeds normally from that single-member starting point.

## Conditions: what they mean and what to do

The full table is in [concepts](concepts.md#conditions). The actionable subset:

### `Available=False/Paused`

Expected when `spec.replicas=0`. Inspect the message for whether data is preserved:

- "data is preserved on PVC data-`<name>`" → dormant member exists; scaling up resumes the same etcd cluster.
- "no data has been written (cluster never bootstrapped)" → cluster was created with `replicas=0` from the start; scaling up triggers a fresh bootstrap.

### `Available=False/QuorumLost`

Less than half of `observed.replicas` are ready. The cluster cannot serve writes. Check:

```sh
kubectl get etcdmember.etcd-operator.cozystack.io -n <ns> \
  -o custom-columns=NAME:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status
kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns> -o wide
kubectl describe pod -n <ns> <unhealthy-pod>
```

Common causes: PVC binding stuck (no nodes match the storage class's topology), node drained without rescheduling, etcd OOM (look at `kubectl logs --previous`), DNS resolution failing inside the cluster network.

If quorum is recoverable (Pods come back), the cluster heals on its own. If a member's data dir is gone, see [Broken member](#broken-member).

### `Available=False/ClusterUnreachable`

Bootstrap discovery couldn't dial the seed. The message comes via `stableErrorMessage(err)` which strips per-call variable portions (timestamps, port numbers), so the same root cause reads consistently across retries.

```sh
kubectl get etcdcluster.etcd-operator.cozystack.io <name> -n <ns> \
  -o jsonpath='{.status.conditions[?(@.type=="Available")].message}{"\n"}'
kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns>
kubectl logs -n <ns> <seed-pod>
```

If the seed Pod is `Running 1/1` and the controller still reports `ClusterUnreachable`, suspect a Service/DNS issue:

```sh
# Headless service exists?
kubectl get svc -n <ns> <cluster>
# Resolves to a pod IP?
kubectl run dns-debug --rm -it --image=busybox -n <ns> -- \
  nslookup <seed-pod-name>.<cluster>.<ns>.svc.cluster.local
```

### `Available=False/BootstrapFailed`

Terminal. The deadline expired before `clusterID` was latched. The partial seed's Pod carries an `--initial-cluster` flag baked in; the operator cannot change it in-place. Recovery is:

```sh
kubectl delete etcdcluster.etcd-operator.cozystack.io <name> -n <ns>
# Wait for all dependents to be GC'd:
kubectl get etcdmember,pvc -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns>
# Once that returns no resources, re-create.
kubectl apply -f <your-cluster-manifest>.yaml
```

The PVC GC step is important: re-creating before the prior PVCs are gone causes the new EtcdMember to refuse to adopt them (`pvcOwnedBy` UID check fails — see [concepts](concepts.md#api-model)). The operator's check is a safety feature; the right answer is to wait.

### `Available=False/DeadlineExceeded`

Terminal. The deadline expired after bootstrap (the cluster itself is healthy; only the most recent operation got stuck). The operator parks and waits for a spec edit:

```sh
# Inspect what's stuck:
kubectl get etcdcluster.etcd-operator.cozystack.io <name> -n <ns> -o yaml
# observed shows what the operator was trying to reach;
# spec shows what you originally asked for.

# Edit spec to something sane (often: revert to the previous working value):
kubectl edit etcdcluster.etcd-operator.cozystack.io <name> -n <ns>
```

The next reconcile notices `spec != observed`, treats your edit as the intervention signal, snapshots the new spec into `observed`, sets a fresh deadline, and resumes.

### `Progressing=True/WaitingForSeed`

The seed `EtcdMember` CR exists but the member controller hasn't yet created its Pod — this is the gap between the cluster controller creating the CR and the member controller's next reconcile pass. `kubectl describe pod` is **not** useful here: there is no Pod yet, so it returns "not found" and obscures the actual state. Inspect the CR and the namespace's events instead:

```sh
SEED=$(kubectl get etcdmember.etcd-operator.cozystack.io -n <ns> \
  -o jsonpath='{.items[?(@.spec.bootstrap==true)].metadata.name}')
kubectl describe etcdmember.etcd-operator.cozystack.io -n <ns> "$SEED"
kubectl get events -n <ns> --field-selector involvedObject.name=$SEED
```

In normal operation this state clears within one or two reconcile cycles. If it persists, the operator controller is wedged — check its logs (see [Operator logs](#operator-logs)).

Once the seed Pod is created, `WaitingForSeed` clears and any *Pod*-side problems (`StorageClass` missing, image-pull failure, `PodSecurity` admission rejection of the `restricted`-compatible spec) surface separately. At that point `kubectl describe pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns>` is the right tool.

## Forcing escalation: shortening the deadline

The default `progressDeadlineSeconds` is 600 (10 minutes). If a reconcile is wedged and you don't want to wait out the window, force the deadline now:

```sh
kubectl patch etcdcluster.etcd-operator.cozystack.io <name> -n <ns> --subresource=status --type=merge \
  -p "{\"status\":{\"progressDeadline\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ -d '1 second ago')\"}}"
```

This pushes the cluster into the terminal-error state immediately. Recovery follows the relevant condition arm above (delete-and-recreate for pre-bootstrap, spec-edit for post-bootstrap).

## Broken member

Recovery from a permanently broken **PVC-backed** member (e.g. PVC lost, node retired) is currently manual. Memory-backed members are auto-replaced on Pod loss — see [Memory-backed clusters](#memory-backed-clusters) above. For PVC-backed clusters the `isBroken` predicate stays a stub; auto-replacement is not wired up (see [concepts](concepts.md#what-is-not-in-the-design)).

Manual recovery:

```sh
# 1. Identify the broken member.
kubectl get etcdmember.etcd-operator.cozystack.io -n <ns>
# 2. Delete it — the finalizer runs MemberRemove against peers, then GC takes
#    the Pod and PVC. Quorum holds because we remove before adding.
kubectl delete etcdmember.etcd-operator.cozystack.io <broken-member> -n <ns>
# 3. The cluster controller's next reconcile observes current < desired and
#    scales up automatically — a new member is added with GenerateName and
#    fresh storage.
```

This sequence preserves quorum if you have an odd number of voters and only one is broken. If multiple voters are broken simultaneously, quorum is lost and you can't `MemberRemove` cleanly. In that case the recovery is to delete the EtcdCluster, recreate it, and restore from a snapshot — see [Restoring a cluster from a snapshot](#restoring-a-cluster-from-a-snapshot). Snapshots only exist if you have been taking `EtcdBackup`s, so set that up *before* you need it.

## Taking a backup

`EtcdBackup` captures a one-shot snapshot of a running cluster and stores it in S3 (or on a PVC). The operator runs a Job using its own image as a backup agent; it dials the cluster's client Service (honoring TLS and `spec.auth` auth automatically) and uploads the snapshot.

```sh
# S3 credentials Secret in the cluster's namespace. The agent reads exactly
# these two keys.
kubectl create secret generic s3-creds -n <ns> \
  --from-literal=AWS_ACCESS_KEY_ID=<key> \
  --from-literal=AWS_SECRET_ACCESS_KEY=<secret>

cat <<'EOF' | kubectl apply -f -
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdBackup
metadata:
  name: my-etcd-2026-06-02
  namespace: <ns>
spec:
  clusterRef:
    name: my-etcd
  destination:
    s3:
      endpoint: https://s3.example.com   # MinIO/Ceph endpoint also fine
      bucket: etcd-backups
      key: my-etcd                       # optional prefix; agent appends "<name>.db"
      region: us-east-1                  # optional
      forcePathStyle: true               # MinIO/Ceph typically require this
      credentialsSecretRef:
        name: s3-creds
EOF

# Watch it reach Complete; status.snapshot records where it landed.
kubectl get etcdbackup.etcd-operator.cozystack.io my-etcd-2026-06-02 -n <ns> -w
kubectl get etcdbackup.etcd-operator.cozystack.io my-etcd-2026-06-02 -n <ns> \
  -o jsonpath='{.status.snapshot}{"\n"}'
# -> {"uri":"s3://etcd-backups/my-etcd/my-etcd-2026-06-02.db","sizeBytes":...,"checksum":"sha256:..."}
```

For a PVC destination use `destination.pvc.{claimName,subPath}` instead of `s3`. Exactly one of `s3`/`pvc` is required (CEL-enforced). The backup is immutable: to re-snapshot, create a new `EtcdBackup`. There is no scheduled-backup CRD — drive recurring snapshots with a `CronJob` that `kubectl apply`s a fresh `EtcdBackup` (e.g. with a date-stamped name).

> **Object names must be unique.** The stored object is keyed by the `EtcdBackup` *name* (`<key-prefix>/<name>.db` for S3, `<name>.db` on a PVC). The agent **refuses to overwrite an existing snapshot it did not write** — for **both** S3 and PVC destinations — so a backup whose key/path already exists fails rather than silently clobbering an earlier snapshot. Give each backup a distinct name (the date-stamped CronJob pattern above does this) or a distinct destination `key`/`subPath`. (A *retry* of the same `EtcdBackup` is exempt: each snapshot is stamped with the backup's UID — S3 object metadata, or a sibling `<name>.db.uid` file on a PVC — so the agent recognizes its own prior write and the retry is idempotent.) The CRD's "immutability" is about the object: it does not version snapshots, so reusing a name after deleting the object replaces it.
>
> **S3 credentials need `s3:ListBucket` (or equivalent), not just `s3:GetObject`/`PutObject`.** The overwrite guard does a `HeadObject` on the target key; with `GetObject` but no `ListBucket`, S3 (and some MinIO/Ceph policies) returns **403 AccessDenied** for a *missing* key instead of 404. The agent cannot distinguish that from a real permission problem, so it **fails closed** (refuses the backup) rather than risk an overwrite. Grant the backup credentials `ListBucket` on the bucket so a HEAD on a missing key returns 404.

If a backup ends up `Failed`, inspect the agent Job's Pod logs (the Job is `<backup-name>-backup` and is GC'd a few minutes after finishing via `ttlSecondsAfterFinished`):

```sh
kubectl logs -n <ns> job/my-etcd-2026-06-02-backup
```

## Restoring a cluster from a snapshot

Restore is a **first-bootstrap-only** path: a *new* cluster initializes its seed member's data dir from a snapshot instead of starting empty. You cannot restore into an existing, already-bootstrapped cluster (`spec.bootstrap` is immutable post-create) — delete and recreate.

> **⚠️ Restore requires the cluster's etcd version to match the operator's `etcdutl`.** The restore agent rebuilds the data dir with the `etcdutl` vendored into the operator image (currently **etcd 3.6.x**), and that data dir carries 3.6's on-disk storage semantics — an etcd container of a different minor (e.g. 3.5.x) booting on it is unvalidated and can fail at the seed. The agent enforces this: it **fails the restore early with a clear message** if `spec.version`'s major.minor differs from its `etcdutl`. So a restored cluster must run a **3.6.x** `spec.version` (the example below uses `3.6.11`). This applies only to restore-on-bootstrap; non-restore clusters can run any supported version. To restore into a different minor, use an operator build whose `etcdutl` matches.

> **⚠️ Restoring a snapshot from an auth-enabled cluster.** An etcd snapshot captures the data store *including its auth state* — users, roles, and the auth-enabled flag. A snapshot taken while auth was on restores into a cluster where **etcd boots with auth already ON**. You must therefore set `spec.auth` on the new `EtcdCluster` to match, or the operator can never manage it:
>
> - Set `spec.auth.enabled: true` and `spec.auth.rootCredentialsSecretRef` (which also requires `spec.tls.client` — same CEL rules as any auth-enabled cluster; see [Authentication](concepts.md#authentication)).
> - The referenced Secret's `password` **must equal the root password that was in effect when the snapshot was taken** — etcd stores the bcrypt hash of the password in the data store, so a fresh/random password will not authenticate against the restored hash. Reuse the original credentials Secret if you still have it.
>
> If you omit `spec.auth` (as the example below does — it restores a *non*-auth snapshot), the restored auth-enabled etcd rejects the operator's anonymous dials. The operator detects this and surfaces `Available=False` / `Degraded=True` with reason **`AuthRequiredNotConfigured`** and an actionable message — it does **not** silently loop. Because auth is immutable post-create, the fix is to delete and recreate with `spec.auth` set (there is no auto-detection of a snapshot's auth state at restore time; this contract is on you). When `spec.auth` *is* set correctly the operator detects auth is already enabled (via `AuthStatus`) and latches `status.authEnabled` without re-provisioning the root user.

```sh
cat <<'EOF' | kubectl apply -f -
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: my-etcd          # may reuse the old name once the old cluster is gone
  namespace: <ns>
spec:
  replicas: 3
  version: 3.6.11          # must match the operator's etcdutl minor (3.6.x) — see warning above
  storage:
    size: 1Gi
  bootstrap:
    restore:
      source:
        s3:
          endpoint: https://s3.example.com
          bucket: etcd-backups
          key: my-etcd/my-etcd-2026-06-02.db   # EXACT object key (not a prefix)
          region: us-east-1
          forcePathStyle: true
          credentialsSecretRef:
            name: s3-creds
EOF

# The seed Pod runs an init container named "restore" before etcd starts.
kubectl get etcdcluster.etcd-operator.cozystack.io my-etcd -n <ns> -w
kubectl logs -n <ns> <seed-pod> -c restore
```

Notes:

- For a restore **source** the locator is exact: `s3.key` is the full object key (what `status.snapshot.uri` reported, minus the `s3://bucket/` prefix), and for a PVC source `pvc.subPath` is the full path to the `.db` file within the volume.
- The restore agent rebuilds the data dir with `etcdutl` using the seed's member identity (name / initial-cluster / token / peer URL), then etcd starts from it. Scale-up members added afterwards join the live cluster normally — only the seed restores.
- The restore is idempotent: once the data dir is initialized, the init container no-ops, so Pod restarts after first boot never re-download or clobber live data.
- After restore, etcd assigns a **new** cluster ID — this is a fresh cluster seeded with the old data, not a continuation of the old one.
- **Size the data volume for ~2x the snapshot during restore.** The agent stages the snapshot and the rebuilt data dir on the data volume (an S3 download is staged there too, not on the container's ephemeral `/tmp`), so it transiently holds roughly twice the snapshot size. The agent runs a pre-flight free-space check and fails early with a clear message if `spec.storage.size` is too small — resize before retrying rather than letting the restore die mid-way. For an S3 source the size is learned via a `HeadObject` **before** the download starts, so an undersized volume fails on the check rather than ENOSPC-ing partway through the transfer. (A PVC source reads from a separate read-only mount, so only the rebuild consumes data-dir space.)

## Reading etcd state directly

The operator only surfaces what it needs for its own decisions. For deeper inspection talk to etcd:

```sh
POD=$(kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns> \
  -o jsonpath='{.items[0].metadata.name}')

# Cluster ID, leader, members:
kubectl exec -n <ns> "$POD" -- etcdctl --endpoints=http://localhost:2379 \
  member list -w table
kubectl exec -n <ns> "$POD" -- etcdctl --endpoints=http://localhost:2379 \
  endpoint status -w table --cluster

# Health (per-member):
kubectl exec -n <ns> "$POD" -- etcdctl --endpoints=http://localhost:2379 \
  endpoint health --cluster

# Database size, last revision, raft term:
kubectl exec -n <ns> "$POD" -- etcdctl --endpoints=http://localhost:2379 \
  endpoint status --cluster -w json | jq
```

`IS LEARNER=true` in `member list` indicates a member that hasn't been promoted yet — expected during a scale-up step, abnormal in steady state. The operator's `promotePendingLearner` runs from both `scaleUp` and the `current == desired` branch of `Reconcile`, so a learner that stays as a learner for more than a few reconcile cycles either has a sync problem (check the etcd pod logs) or the operator is wedged (check the operator pod logs).

## Operator logs

The operator runs in `etcd-operator-system` by default. Log lines you'll see most often:

```sh
kubectl logs -n etcd-operator-system deploy/etcd-operator-controller-manager \
  -c manager --tail=200
```

Key signals:

| Log message | Meaning |
|---|---|
| `bootstrapping single-node cluster` | First-reconcile bootstrap is creating the seed. |
| `waiting for bootstrap member to form cluster` | Seed created, waiting for its Pod + etcd discovery. |
| `cluster declared paused from the start; not bootstrapping` | `spec.replicas=0` on a never-bootstrapped cluster. |
| `completing pending scale-up member before further action` | Crash recovery: a previous reconcile left a CR with empty `spec.initialCluster`. |
| `added member as learner` | `MemberAddAsLearner` succeeded; next reconcile will Patch `spec.initialCluster`. |
| `promoted learner` | `MemberPromote` succeeded; the member is now a voter. |
| `waking dormant member` | Resume: Patching `spec.dormant=false`. |
| `waiting for existing members to become Ready before next scale-up step` | Scale-up is single-stepping; the previous learner isn't ready yet. |
| `learner not yet promotable; will retry` | `MemberPromote` returned an "in sync with leader" error; benign during scale-up. |
| `MemberList failed` ERROR with `rpc not supported for learner` | The endpoint-filtering fix (issue #12) should prevent this; if you see it, file a bug. |

## Memory-backed clusters

Opt-in via `spec.storage.medium: Memory`. Each member's data dir is a tmpfs `emptyDir` whose lifetime is the Pod's. Suits reconstructable workloads only — see [concepts](concepts.md#storage) for the model and trade-offs.

### Create a memory-backed cluster

```sh
cat <<'EOF' | kubectl apply -f -
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: my-mem-etcd
  namespace: default
spec:
  replicas: 3
  version: 3.6.11
  storage:
    size: 256Mi
    medium: Memory
EOF
```

`storage.size` now defines the tmpfs `SizeLimit`, not a PVC capacity. Pick it generously — etcd's WAL plus the keyspace plus a buffer for compaction. 256Mi is enough for sub-MB keyspaces; bump it for anything load-bearing.

### Verify it's actually using tmpfs

The Pod's volume tells you:

```sh
kubectl get pod -l etcd-operator.cozystack.io/cluster=my-mem-etcd -n default \
  -o jsonpath='{.items[0].spec.volumes[?(@.name=="data")]}' | jq
# Expect: {"emptyDir": {"medium": "Memory", "sizeLimit": "256Mi"}, ...}
```

And inside the Pod:

```sh
POD=$(kubectl get pod -l etcd-operator.cozystack.io/cluster=my-mem-etcd -n default \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -n default "$POD" -- mount | grep /var/lib/etcd
# Expect: tmpfs on /var/lib/etcd type tmpfs (...)
```

No PVCs should exist for the cluster:

```sh
kubectl get pvc -l etcd-operator.cozystack.io/cluster=my-mem-etcd -n default
# No resources found.
```

### What you should configure before going to production

The `PodDisruptionBudget` is auto-emitted now (see [Draining nodes](#draining-nodes-poddisruptionbudget) for the day-to-day picture). The remaining production gaps are tracked in [#16](https://github.com/lllamnyp/etcd-operator/issues/16):

1. **Pod anti-affinity** — pre-deploy a mutating webhook (e.g. `pod-topology-spread` admission controller, or your own) that adds:

   ```yaml
   spec:
     affinity:
       podAntiAffinity:
         requiredDuringSchedulingIgnoredDuringExecution:
           - labelSelector:
               matchLabels:
                 etcd-operator.cozystack.io/cluster: my-mem-etcd
             topologyKey: kubernetes.io/hostname
   ```

2. **Container memory limit** — set `spec.resources.limits.memory` on the cluster so tmpfs writes account against the Pod's cgroup, not node memory:

   ```yaml
   spec:
     storage:
       size: 256Mi
       medium: Memory
     resources:
       requests:
         memory: 384Mi
       limits:
         memory: 512Mi   # >= spec.storage.size + ~128Mi for etcd headroom
   ```

   Without a limit, tmpfs writes count against node memory rather than the Pod's cgroup, the Pod runs in BestEffort/Burstable QoS, and it is first in line for eviction under pressure — the exact failure mode that destroys memory members. `spec.resources` updates take effect on newly-created members; existing Pods keep their original sizing until rolled.

### Pod loss and auto-replacement

The operator detects Pod loss via `Status.PodUID`. The two scenarios:

- **Single Pod lost while quorum holds**: operator deletes the EtcdMember CR, finalizer runs `MemberRemove` against peers, cluster controller's scale-up creates a fresh replacement with a new `GenerateName` and a new etcd member ID. The cluster heals automatically.
- **More than quorum lost simultaneously**: `MemberRemove` against the surviving peers fails (no quorum), the dying members sit in `Terminating`. The cluster is dead and the user has to recreate.

**Detection latency depends on what killed the Pod.** A `kubectl delete pod` or kubelet eviction transitions the Pod to NotFound within seconds — auto-replacement starts on the next reconcile (~5 s). A node going NotReady is slower: the kubelet on a healthy node would clean up immediately, but the kube-controller-manager's `--pod-eviction-timeout` (default 5 minutes) gates the Pod's transition out of Terminating. Until then the operator's loss check sees a Pod with the same UID (status reports it as Terminating but it still exists from the API's perspective) and waits — better than racing the kubelet GC. So budget **up to 5 minutes of degraded quorum** when an etcd-hosting node fails unannounced. Tune `kube-controller-manager --pod-eviction-timeout` cluster-wide if you need it shorter; this is outside the operator's control.

Watch the auto-replacement happen:

```sh
kubectl get etcdmember.etcd-operator.cozystack.io -n default -w
# Original: my-mem-etcd-abc12, my-mem-etcd-def34, my-mem-etcd-ghi56.
# Force-delete one Pod: kubectl delete pod -n default my-mem-etcd-abc12
# Observe the EtcdMember CR get deleted, a new one with a fresh GenerateName
# appear, and READY=3 restore within a minute or so.
```

### Pause is not supported

Setting `spec.replicas: 0` on a memory cluster is **rejected by the apiserver** (CEL validation rule on `EtcdClusterSpec`):

```
kubectl patch etcdcluster.etcd-operator.cozystack.io my-mem-etcd -n default --type=merge \
  -p '{"spec":{"replicas":0}}'
# The EtcdCluster "my-mem-etcd" is invalid: spec: Invalid value: ...:
#   spec.replicas=0 with spec.storage.medium=Memory is unsupported: ...
```

Pausing a memory cluster would wedge it on resume (Pod deleted → tmpfs gone → wake path treats the empty data dir as preserved → etcd refuses to start). To tear a memory cluster down, delete the `EtcdCluster` and recreate it.

## Draining nodes (PodDisruptionBudget)

Every `EtcdCluster` carries a per-cluster `PodDisruptionBudget` named after the cluster. The full design is in [concepts](concepts.md#poddisruptionbudget); the day-to-day picture:

```sh
kubectl get pdb -n <ns> <cluster>
# NAME       MIN AVAILABLE   MAX UNAVAILABLE   ALLOWED DISRUPTIONS   AGE
# my-etcd    N/A             1                 1                     12m
```

`MAX UNAVAILABLE` is the budget. `ALLOWED DISRUPTIONS` is how many voter evictions are still in budget right now (= max unavailable − currently unavailable). When it reaches 0, `kubectl drain` of any node hosting a voter Pod blocks:

```
error when evicting pods/"my-etcd-7xq2k" -n my-ns:
  Cannot evict pod as it would violate the pod's disruption budget.
```

That's the intended behaviour — your drain just refused to break quorum. Resolve by waiting for the unavailable voter to come back, or by understanding that more nodes need to be ready before this drain can proceed.

### Which Pods are voters

Voter Pods carry the label `etcd-operator.cozystack.io/role=voter`. Learners do not. To find them:

```sh
kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster>,etcd-operator.cozystack.io/role=voter -n <ns>
```

Cross-reference against `kubectl get etcdmember.etcd-operator.cozystack.io -n <ns>` — voters there have `Status.IsVoter: true` (written by the cluster controller from etcd's `MemberList`).

### Why drains might block during scale events

The PDB updates **one reconcile after** etcd's view changes (cluster controller's next pass picks up the new voter count from `MemberList`). This is intentional — see [concepts](concepts.md#transient-races) for the safety analysis. The race window is one reconcile cycle wide (steady-state `RequeueAfter` is 30 s, so up to ~30 s in the worst case); a drain attempted in that window fails closed (refuses the eviction) rather than open, which is the correct direction.

If you're doing a planned rolling node maintenance, scale down to the resilient quorum size first, drain, scale back up.

## Recipes

### Find the dormant member

```sh
kubectl get etcdmember.etcd-operator.cozystack.io -n <ns> \
  -o jsonpath='{range .items[?(@.spec.dormant==true)]}{.metadata.name}{"\n"}{end}'
```

### Find the seed of a running cluster

```sh
kubectl get etcdmember.etcd-operator.cozystack.io -n <ns> \
  -o jsonpath='{range .items[?(@.spec.bootstrap==true)]}{.metadata.name}{"\n"}{end}'
```

Note: the seed has no special operational role post-bootstrap — see [concepts](concepts.md#member-naming). This is for historical lookup, not for routing.

### Tail logs from the etcd leader

```sh
POD=$(kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns> \
  -o jsonpath='{.items[0].metadata.name}')
LEADER=$(kubectl exec -n <ns> "$POD" -- etcdctl \
  --endpoints=http://localhost:2379 endpoint status --cluster -w json \
  | jq -r '.[] | select(.Status.leader == .Status.header.member_id) | .Endpoint' \
  | sed 's|http://||;s|:.*||;s|\..*||')
kubectl logs -n <ns> "$LEADER" --tail=200
```

### Drain a node holding an etcd member

Just `kubectl cordon` + `kubectl drain` works. The Pod gets evicted, reschedules onto another node, the PVC reattaches (if storage class supports relocation) or stays put (if not — then the Pod stays Pending until the node is back). Etcd is fine with this — `MemberAddAsLearner` isn't involved because the member ID and data dir are preserved in the PVC.

PodAntiAffinity is not configured by default (see [What's not supported](../README.md#whats-not-supported-yet)). Two etcd pods can land on the same node, which means a single node drain can take out two voters simultaneously and lose quorum on a 3-member cluster. Recommended workaround: add a PodAntiAffinity rule via a `PodTopologySpread` mutating webhook or pre-deploy a `Deployment`-level affinity wrapper. (A native operator option is a future feature.)

## TLS-enabled clusters

Two paths to a TLS-enabled cluster, mutually exclusive per subtree:

- **BYO Secrets** — you create the Secrets out-of-band and reference them from `spec.tls.{client,peer}.{serverSecretRef,operatorClientSecretRef,secretRef}`. The section below covers this.
- **cert-manager** — point `spec.tls.{client,peer}.certManager` at an Issuer/ClusterIssuer, the operator emits `cert-manager.io/v1` Certificate resources and cert-manager produces the Secrets. Skip to [cert-manager-managed clusters](#cert-manager-managed-clusters).

See [concepts: TLS](concepts.md#tls) for what each Secret needs to contain and the EKU / CA-bundle constraints.

### Creating the Secrets

A typical full-mTLS cluster needs three Secrets in the cluster's namespace:

```sh
# Server cert+key, plus the CA bundle that doubles as the client trust
# bundle. Server cert SANs MUST cover:
#   *.<cluster>.<ns>.svc,
#   *.<cluster>.<ns>.svc.<cluster-domain>,
#   <cluster>.<ns>.svc, <cluster>-client.<ns>.svc,
#   localhost (DNS SAN), 127.0.0.1 (IP SAN).
# The doubled-up wildcards aren't redundant: the second covers the
# fully-qualified PTR record kube-dns returns for pod IPs, which
# etcd's peer-mTLS validator looks up. <cluster-domain> defaults to
# cluster.local; Cozystack and a few others use cozy.local. See
# installation.md for how to identify it.
# Server cert EKU MUST include both serverAuth and clientAuth.
kubectl create secret generic my-cluster-server-tls -n <ns> \
  --from-file=tls.crt=server.crt \
  --from-file=tls.key=server.key \
  --from-file=ca.crt=ca.crt

# Operator's etcd-client identity. EKU MUST include clientAuth. Signed by
# a CA whose public cert is in the trust bundle above (typically the same
# CA, in which case the bundle is just that one cert).
kubectl create secret generic my-cluster-operator-client-tls -n <ns> \
  --from-file=tls.crt=op-client.crt \
  --from-file=tls.key=op-client.key

# Peer cert+key+CA. SANs MUST cover *.<cluster>.<ns>.svc AND
# *.<cluster>.<ns>.svc.<cluster-domain> — the second covers the
# fully-qualified PTR record etcd's peer-mTLS verifier compares against
# the connecting peer's reverse-DNS. Peer cert EKU MUST include both
# serverAuth and clientAuth (peer is symmetric).
kubectl create secret generic my-cluster-peer-tls -n <ns> \
  --from-file=tls.crt=peer.crt \
  --from-file=tls.key=peer.key \
  --from-file=ca.crt=peer-ca.crt
```

Then reference them in the cluster spec:

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: my-cluster
spec:
  replicas: 3
  version: 3.6.11
  storage:
    size: 1Gi
  tls:
    client:
      serverSecretRef:
        name: my-cluster-server-tls
      operatorClientSecretRef:
        name: my-cluster-operator-client-tls
    peer:
      secretRef:
        name: my-cluster-peer-tls
```

Server-TLS-only (encryption without client identity) drops the `operatorClientSecretRef` line. Peer-plaintext drops the whole `peer:` block. The two surfaces are independently optional.

### Talking to a TLS cluster from `etcdctl`

The hardcoded `--endpoints=http://localhost:2379` examples elsewhere in this doc become:

```sh
POD=$(kubectl get pod -l etcd-operator.cozystack.io/cluster=<cluster> -n <ns> \
  -o jsonpath='{.items[0].metadata.name}')

# Stage a client cert+key inside the Pod for an exec-side etcdctl. Any
# client cert signed by a CA in /etc/etcd/tls/client/ca.crt works; the
# operator-client cert is a convenient one because it's already issued.
# `kubectl cp` takes a local source path (the directory holding tls.crt
# and tls.key) and writes into the Pod at the destination path.
kubectl cp ./client-cert-dir "<ns>/$POD:/tmp/cli"

kubectl exec -n <ns> "$POD" -- etcdctl \
  --endpoints=https://localhost:2379 \
  --cacert=/etc/etcd/tls/client/ca.crt \
  --cert=/tmp/cli/tls.crt \
  --key=/tmp/cli/tls.key \
  member list -w table
```

For server-TLS-only clusters, drop `--cert` and `--key`.

### cert-manager-managed clusters

When `spec.tls.{client,peer}.certManager` is set, the operator emits three `cert-manager.io/v1 Certificate` resources per cluster (server, optional operator-client, peer) at reconcile time. cert-manager then produces the matching Secrets. Inspect them like any other cert-manager resource:

```sh
kubectl get certificate -n <ns> -l app.kubernetes.io/instance=<cluster>
kubectl describe certificate -n <ns> <cluster>-server
```

A Certificate stuck at `Ready=False` is the most common failure mode. `kubectl describe certificate <cluster>-server` surfaces cert-manager's `Issuer`-side error (no matching `Issuer`, signing back-end unreachable, CA cert expired, etc.). If the operator probe didn't find cert-manager at startup, the EtcdCluster's status will read `Available=False / CertManagerNotInstalled` and no Certificates will be created — install cert-manager, then restart the operator (the discovery probe re-runs at every operator start).

`--cluster-domain` mismatches are silent and the failure mode is "second member of the cluster crashloops with `discovery failed` / `EOF`". The operator auto-discovers its DNS suffix from `/etc/resolv.conf`'s `search` line at startup, so for normal cluster-pod deployments — including Cozystack's `cozy.local` — no flag is required. If your operator runs with `hostNetwork: true` or a custom `dnsPolicy`, auto-discovery returns nothing and the operator falls back to `cluster.local`; set `--cluster-domain` explicitly in that case.

### Cert rotation

Go's `crypto/tls` re-reads some files per handshake and bakes others into an `*x509.CertPool` at config time. The rotation procedure depends on which material changed; for cert-manager-managed clusters, cert-manager auto-renews the Certificate (the Secret content updates) and only the Pod-side trust-bundle case still needs a manual restart.

| Material | Re-read live? | Rotation procedure |
|---|---|---|
| `tls.crt` / `tls.key` in `serverSecretRef` (BYO) or `<cluster>-server-tls` (cert-manager) — etcd's `--cert-file` / `--key-file` | **Yes.** Wired through `tls.Config.GetCertificate`; etcd re-loads the files on every new TLS handshake. | BYO: update the Secret. cert-manager: nothing — cert-manager handles renewal automatically. New connections pick up the new cert within a kubelet refresh cycle (~60s for the mounted volume to project). Existing keepalive connections continue with the old cert until they cycle naturally. |
| `tls.crt` / `tls.key` in peer Secret (`--peer-cert-file` / `--peer-key-file`) | **Yes**, same mechanism. | Same as above. |
| `ca.crt` in server Secret (etcd's `--trusted-ca-file`) | **No.** Loaded into an `x509.CertPool` at etcd startup and held in memory. | Update the trust bundle (BYO: edit the Secret; cert-manager: rotate the Issuer's CA), then bounce each Pod one at a time (`kubectl delete pod <member-pod-name>`; wait for `Ready` and `member list` to confirm the rejoin before continuing). |
| `ca.crt` in peer Secret (etcd's `--peer-trusted-ca-file`) | **No**, same reason. | Same as above. |
| `tls.crt` / `tls.key` / `ca.crt` in operator-client Secret (operator-side, not mounted into etcd Pods) | **Yes** — the operator rebuilds its `*tls.Config` from the Secret on every reconcile, so all three rotate without an operator restart. | Update the Secret (BYO) or let cert-manager renew (cert-manager). The change takes effect on the next reconcile (~30s on idle, immediately on event-driven triggers). |

When in doubt: certs + keys are hot-swappable; trust bundles (the `ca.crt` consumed *as a trust anchor*) need a Pod restart. A future operator version will watch the Secrets and roll Pods on RV change so the trust-bundle case is automated too.

### Toggling TLS on or off after create

Not supported (see the `spec.tls` immutability rules in [concepts](concepts.md#apiserver-enforced-validation)). The apiserver rejects the change directly:

```sh
$ kubectl patch etcdcluster my-cluster --type=merge -p '{"spec":{"tls":null}}'
Error from server (Invalid): ... spec.tls cannot be added to or removed from
  an existing cluster; delete and recreate
```

The same applies to mTLS-toggle (adding/removing `operatorClientSecretRef`) and to swapping in a different secret. The path is delete and recreate.
