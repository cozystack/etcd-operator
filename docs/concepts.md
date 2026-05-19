# Concepts

This document explains the operator's design choices and the contracts they create. The goal is to give an operator running etcd clusters a working mental model — enough to predict what the controller will do, debug it when it doesn't, and read the conditions correctly.

The reader is assumed to be k8s-fluent. For deployment steps see [installation](installation.md); for kubectl recipes see [operations](operations.md).

## API model

Two custom resources, one of them user-facing.

**`EtcdCluster`** — the user-facing object. It captures cluster-wide intent: replica count, etcd version, per-member storage size, a progress deadline. This is the only resource users normally touch.

**`EtcdMember`** — one per etcd member. Created and deleted by the cluster controller. Each `EtcdMember` owns its Pod and PVC. Users should not create or edit these directly.

There is **no StatefulSet**. Each member's Pod and PVC are reconciled independently by the member controller. The motivation is protocol awareness: scale-up adds a member as a learner first and only promotes once it's caught up; scale-down runs `MemberRemove` via a finalizer before reclaiming the Pod; pod restarts reuse the existing data dir and rejoin with the same etcd-side member ID. None of these flows fit StatefulSet's "all replicas are one fungible workload" model.

The cluster controller decides *which* members exist and orchestrates the etcd-side state machine (`MemberAddAsLearner` / `MemberPromote` / `MemberRemove`). The member controller decides *how* a member becomes real — Pod, PVC, etcd flags — and reports observed facts (member ID, readiness) back up to its CR's status.

## Member naming

`EtcdMember` CRs are created with `ObjectMeta.GenerateName="<cluster>-"`. Each member's name is an apiserver-assigned random suffix (e.g. `mycluster-7xq2k`). Names are not predictable, and that is deliberate — the previous design used `<cluster>-<ordinal>` and tied cluster identity to ordinal reuse across incarnations, which is exactly the trap to avoid for stateful systems. Now:

- Deleting an `EtcdCluster` and recreating one with the same name produces fresh member names (different suffixes).
- The `--initial-cluster-token` is derived as `<namespace>-<cluster>-<uid>`, so the token also differs across incarnations.
- Together: two incarnations of "same-named EtcdCluster" never look alike to etcd, to k8s, or to a stale PVC trying to mount itself back into the new cluster.

The seed (the original `EtcdMember` created during bootstrap) carries `spec.bootstrap=true`. That flag is the discovery anchor (see [bootstrap](#bootstrap-and-discovery)) and is otherwise just historical metadata — the seed has no permanent special role in raft and can be removed like any other member.

## Locking pattern

A naive operator re-reads `spec` every reconcile and acts on whatever it sees. That breaks etcd in two well-known ways:

1. **Mid-bootstrap replica change.** Etcd requires every bootstrapping member to start with the same `--initial-cluster` flag. Editing `spec.replicas` mid-bootstrap would have two members agreeing on different cluster shapes; etcd refuses to form.
2. **Scale-up followed immediately by scale-down.** `MemberAdd` registers the new peer with etcd before its pod is Ready. Reverting `spec.replicas` in that window leaves the operator deleting a member it can't yet identify.

Both failures share a root cause: the user's "desired state" mutates on a faster cadence than the operator can converge.

**The fix.** The operator commits to a target. The first time it sees an `EtcdCluster`, it copies the spec into `status.observed` and stamps `status.progressDeadline = now + spec.progressDeadlineSeconds`. From then on the controller reconciles against `status.observed`, not `spec`. Spec changes are *noticed* but not *acted on*; they only get adopted into `observed` when:

- the cluster has reached the current `observed` (the in-flight reconcile finished cleanly), or
- the deadline has elapsed (the in-flight reconcile gave up).

This is the same pattern Deployments use with `progressDeadlineSeconds`, applied at a coarser granularity. The trade-off is responsiveness: a spec edit takes effect on the next "complete" boundary, not immediately. In practice this is exactly the property you want for stateful workloads.

### What "complete" means

`reconciliationComplete` returns true when:

- `status.observed` has been populated (first-reconcile init has happened),
- the number of non-dormant active members equals `observed.replicas`,
- all those members report `MemberReady=True`, and
- if `observed.replicas > 0`, `status.clusterID` is latched.

The `observed.replicas == 0` case relaxes the ClusterID requirement — a paused or fresh-zero cluster has no running etcd process to source one from. Without this relaxation, a fresh-zero cluster scaled up to 1 would never complete and the spec-change-adoption path would never fire.

### Deadlines as terminal errors

An expired `ProgressDeadline` is a **terminal error**, not a "try again with the latest spec" signal. The operator stops acting on its own and waits for the user. The shape of "intervention" depends on whether the cluster ever bootstrapped:

- **Before bootstrap finished** (`status.clusterID == ""`): the partial members carry an `--initial-cluster` flag baked into their pod specs. There is no in-place recovery — recovery is to delete the EtcdCluster and recreate. The condition stays `Available=False, Reason=BootstrapFailed`.
- **After bootstrap finished**: the cluster itself is healthy; only the most recent operation got stuck (e.g. a scale-up to a replica count the cluster can't schedule). The user's spec edit is the intervention — when `spec != status.observed`, the operator treats that as "I'm fixing this", snapshots the new spec, sets a fresh deadline, and resumes. Until that edit, the operator sits in `Available=False, Reason=DeadlineExceeded`.

The operator never silently auto-pivots on deadline expiry. Silent recovery is the wrong default for stateful workloads where the failure modes include data divergence.

You can force a deadline by patching `status.progressDeadline` to a past time. This is the documented escalation when a slow reconcile is wedged and the standard 10-minute window hasn't elapsed yet.

## Create-then-Patch

Because each member's `--initial-cluster` flag contains its own name, and that name isn't known until the apiserver fills in `GenerateName`, every member moves through three steps in order:

1. **Create** the `EtcdMember` CR with `GenerateName` and an empty `spec.initialCluster` (for the seed: also `spec.bootstrap=true`).
2. **MemberAddAsLearner** with the assigned name's peer URL. Skipped for the seed; skipped on scale-up if the peer URL is already registered (crash recovery — see below).
3. **Patch** `spec.initialCluster` from etcd's authoritative member list.

The member controller refuses to start a Pod while `spec.initialCluster` is empty, so a transient "pending" CR (between steps 1 and 3) never reaches the data plane.

### Crash recovery

If a reconcile crashes between steps 1 and 2, the next reconcile sees a pending CR with no matching peer URL in etcd, and calls `MemberAddAsLearner` (then completes the patch).

If a reconcile crashes between steps 2 and 3, the next reconcile sees a pending CR whose peer URL is already registered, skips `MemberAddAsLearner`, and completes the patch. This must happen *before* any promotion attempt — the orphan learner cannot sync without its pod, the pod cannot start until `spec.initialCluster` is set, and a promote attempt would block forever on the un-synced learner. The control flow in `scaleUp` orders these steps explicitly.

Operators inspecting a cluster mid-reconcile may see CRs with empty `spec.initialCluster`. That is the intentional transient state, not corruption.

## Bootstrap and discovery

The cluster forms from a single seed member. Multi-seed bootstrap (multiple members agreeing on `--initial-cluster` upfront) is historically the source of the "mid-flight replica change corrupts consensus" bug. Single-seed bootstrap eliminates that class of failure: the seed forms a one-member cluster with itself in `--initial-cluster`, the operator latches `clusterID` once that member is up, and every subsequent member joins via `MemberAddAsLearner`.

**Discovery** is the bridge between "seed pod is up" and "operator knows the cluster ID". The cluster controller calls `MemberList` against the seed's client URL, validates the response (exactly one member, matching the seed's name or peer URL), and latches `status.clusterID`. Once latched, discovery is never run again.

The seed is identified by `spec.bootstrap=true`. Member names being random precludes a name-based lookup, and trusting list order (`members[0]`) silently anchors discovery to the wrong member when scale-up CRs land in front of the seed. Once `clusterID` is set, the operator never re-reads `spec.bootstrap` for any decision — the seed is, from that point on, just a regular member.

If the seed's pod hasn't been created yet (between Create and Pod-up), the controller surfaces `Progressing=True/WaitingForSeed` rather than dialing a nonexistent endpoint and burning the reconcile budget.

## Scale to zero

`spec.replicas: 0` parks the cluster rather than dismantling it.

### Pause (1→0)

When the cluster controller's `scaleDown` observes `desired==0 && len(running)==1`, it Patches `spec.dormant=true` on the surviving member. The CR is **not** deleted. On the next reconcile of that member, the member controller observes `spec.dormant=true` and runs `ensurePodAbsent` — deletes the Pod, clears `status.podName`, surfaces `Ready=False/Paused`. The PVC is not touched. It keeps its existing owner-ref to the `EtcdMember`, which still exists. So nothing reparents, nothing cascade-deletes.

Intermediate steps of a multi-member descent (3→2, 2→1) are normal scale-downs: pick newest, Delete CR, finalizer runs `MemberRemove`. Only the final 1→0 step flips dormant.

### Wake (0→1+)

When the user sets `spec.replicas >= 1`, the cluster controller's spec-change-adoption path snapshots the new spec into `observed`. On the next reconcile, `scaleUp` finds the dormant member and Patches `spec.dormant=false`. No name lookup, no Create, no etcd RPC at this stage. The member controller then runs the normal `ensurePVC` (which finds the existing PVC by UID match and accepts it) + `ensurePod` (which creates the Pod). Etcd resumes from the data dir with the same `ClusterID` and member ID.

Further scale-up proceeds normally via `MemberAddAsLearner` + `MemberPromote`.

### Why "dormant on the member" instead of "delete the CR + reparent the PVC"

An earlier iteration of this feature deleted the CR, reparented the PVC to the `EtcdCluster`, latched the member's name in `status.dormantMember`, and recreated the member by name on resume. Every iteration accumulated edge cases: cross-resource cache races on the pause trigger (Status update visible before Delete event delivery, or vice versa), foreign-CR-by-fixed-name adoption on resurrection, stale status field after a missed update, fresh-zero-vs-dormant message divergence. The redesigned mechanism — pause is a Patch, resume is a Patch, CR is never deleted — removes all those failure modes by construction. The mechanism the operator needs to support is "the EtcdMember CR is preserved across the pause and the user can scale to 0 and back without external coordination". It now is.

### Replica accounting

`current` is computed from `filterRunningMembers(...)` — non-deleted, non-dormant. A dormant member contributes zero capacity, so it must not count against `desired`, otherwise:

- A 1-member cluster paused at replicas=0 would look like "we have 1 member, target is 0" and the cluster controller would try to scale down again, finding nothing valid to do.
- Scaling a paused cluster back up to >=1 would never decide to wake the dormant member because `current==desired` would already be satisfied.

The single exception is the steady-state call to `updateStatus`, which receives the full active set (including dormant). `updateStatus`'s Paused branch uses `findDormantMember(members)` to name the parked PVC in the `Available=False/Paused` message. Stripping the dormant member at that call site would silently fall back to the fresh-zero "no data has been written" message even on real dormant clusters. The asymmetry is deliberate and the call site is commented.

## Storage

Each member's data dir is configured via `spec.storage`, a struct with `size`, `medium`, and an optional `storageClassName`. The medium chooses between a PVC and a tmpfs `emptyDir`; the locking pattern protects size and medium just like `replicas` and `version` — a mid-flight flip is locked out until the current target is reached or the deadline expires.

| `spec.storage.medium` | Backend | Lifetime | Pod loss → |
|---|---|---|---|
| `""` (default) | PVC; `spec.storage.storageClassName` if set, else the namespace default; `ReadWriteOnce` | Survives Pod restart, eviction, node failure (re-attached to new Pod). | Same Pod / new Pod re-uses existing data dir; etcd rejoins with the same member ID and `ClusterID`. |
| `"Memory"` | `emptyDir{medium: Memory}` with `sizeLimit: spec.storage.size` | Bound to the Pod. Container restart preserves tmpfs; Pod deletion / eviction / node failure destroys it. | Operator detects Pod loss via recorded `Status.PodUID`, self-deletes the `EtcdMember`, finalizer calls `MemberRemove`, scale-up gap-fill creates a replacement with a fresh member ID. |

`spec.storage.storageClassName` mirrors the corev1 PVC field of the same name: **nil** uses the namespace's default `StorageClass`, the **empty string** explicitly disables dynamic provisioning (a pre-provisioned PV must already match), any other value names a specific `StorageClass`. It's immutable post-create — `PersistentVolumeClaim.spec.storageClassName` is itself immutable, so there is no in-place change a controller could honour without rolling every PVC. Ignored when `medium=Memory` (no PVC is created).

### Why memory-backed is opt-in

It trades durability for speed and isolation from node-level storage. Suits:

- Kubernetes-in-Kubernetes apiservers whose state is GitOps-managed and reconstructable.
- Throwaway test clusters.
- Workloads where etcd is a transient cache, not the system of record.

It is **not** appropriate as a general-purpose etcd backend. A node drain or simultaneous evictions of more-than-quorum members destroys the cluster permanently — there is no data to restart from.

### Pod-loss detection (memory only)

On every reconcile of a memory-backed member the controller stamps `Status.PodUID` with the live Pod's UID. On a subsequent reconcile:

- Pod present, UID matches → steady state.
- Pod absent (or UID differs) with a previously recorded UID → loss confirmed.

The member controller self-deletes the `EtcdMember`. The existing finalizer runs `MemberRemove` against quorum-reachable peers and the Pod / PVC owner-refs handle the rest of GC. The cluster controller's normal `current < desired` arm then scales up: a fresh `EtcdMember` is created with a new `GenerateName` and a new etcd-side member ID. There is no in-place "rejoin with empty data dir" — that path would require lying to raft.

If quorum is already lost across multiple simultaneous failures, `MemberRemove` will fail and the dying members stay in `Terminating` until quorum returns. That is the correct outcome: the cluster is dead and the user has to recreate it. The operator does not try to be clever about restoring a quorum from inconsistent half-states.

`Status.BrokenMembers` stays at 0 in normal operation, including across a memory pod-loss + auto-replacement cycle. The `isBroken` predicate is implemented for memory members (lost-Pod state), but the member controller intercepts the loss and self-deletes the member in the same reconcile pass — by the time the cluster controller computes the count, the lost member is already `Terminating` and excluded from the running set. The field exists as a future hook for broken-member detection policies that don't immediately tear the member down (e.g. PVC corruption with a grace period). For PVC-backed members today, `isBroken` stays a stub; richer detection is a future concern.

### What is missing from memory clusters today

Two things are not yet auto-emitted and matter for production memory clusters — both tracked in [issue #16](https://github.com/lllamnyp/etcd-operator/issues/16):

- **Pod anti-affinity**. Without it, scheduling can co-locate voters on one node; a single node failure then loses quorum on a 3-member cluster.
- **Container memory limits**. Without `limits.memory`, tmpfs writes count against node memory rather than the pod's cgroup and the etcd container ends up in BestEffort/Burstable QoS — first to be evicted under pressure. Set `spec.resources.limits.memory` ≥ `spec.storage.size` + ~128Mi for etcd headroom on memory-backed clusters.

The `PodDisruptionBudget` *is* auto-emitted now — see the [PodDisruptionBudget section](#poddisruptionbudget) below.

### Apiserver-enforced validation

Four CEL `x-kubernetes-validations` rules on `EtcdClusterSpec` are evaluated at admission time. **k8s 1.29+ is the safe floor**: CEL CRD validation (`CustomResourceValidationExpressions`) went GA in 1.29, and the `quantity()` extension function used by two of the rules was added in 1.28. The CEL gate was beta-on-by-default from 1.25, so 1.28 *may* work in practice — but 1.29 is the first version where both pieces are GA and the project doesn't have to chase feature-gate state across releases.

| Rule | When | Why |
|---|---|---|
| `storage.medium` immutable | UPDATE | Flipping the medium would orphan the previous PVC (or tmpfs); rolling-migrate is not implemented. |
| `replicas: 0` + `storage.medium: Memory` rejected | CREATE + UPDATE | The pause path deletes the Pod, the tmpfs evaporates, and resume would silently produce an empty data dir; etcd refuses to start. |
| `storage.size > 0` when `storage.medium: Memory` | CREATE + UPDATE | Zero `storage.size` produces an unbounded tmpfs `SizeLimit` against node memory. |
| `storage.size` cannot shrink | UPDATE | PVCs cannot shrink and tmpfs `SizeLimit` reduction does not free allocated memory. |
| `storage.storageClassName` cannot be added or removed | UPDATE | `PersistentVolumeClaim.spec.storageClassName` is immutable; honouring a mid-life add/remove would require rolling every PVC. |
| `storage.storageClassName` value immutable | UPDATE | Same reason — the StorageClass chosen at cluster creation is the only one PVCs will ever carry. |
| `tls` cannot be added or removed | UPDATE | Toggling TLS on an existing cluster is a rolling restart that has to land on the operator's etcd client and every member Pod in lockstep; not implemented. |
| `tls` subtree immutable | UPDATE | Same reason — secret-ref swaps, mTLS-flip via `operatorClientSecretRef`, peer-only ↔ both toggles are all in-place rolling changes that v1 doesn't perform. |

These rules live in the CRD itself; the apiserver enforces them with no separate webhook, no cert-manager, no extra Deployment. Errors come back as standard apiserver admission rejections (`kubectl apply` prints the rule's `message` field).

## TLS

`spec.tls` configures transport-layer security for the cluster's two etcd surfaces: the client API (port 2379) and the peer API (port 2380). Each subtree is independently optional — you can opt one surface into TLS without the other. The whole `tls` subtree is immutable post-create (see the validation table above): toggling TLS on an existing cluster is a rolling change that v1 doesn't perform, so the policy is delete-and-recreate.

### Census of certs

| Artifact | Mount / location | etcd flag | Source |
|---|---|---|---|
| Server cert + key (client API) | each etcd Pod, `/etc/etcd/tls/client/{tls.crt,tls.key}` | `--cert-file`, `--key-file` | `spec.tls.client.serverSecretRef` |
| Client trust bundle (client API) | each etcd Pod, `/etc/etcd/tls/client/ca.crt` | `--trusted-ca-file` (mTLS only) | `ca.crt` key of `serverSecretRef`'s Secret |
| Operator's client cert + key | operator Pod (not mounted into etcd Pods — read by the operator's etcd client) | n/a (Go `tls.Config.Certificates`) | `spec.tls.client.operatorClientSecretRef` |
| Peer cert + key + trust | each etcd Pod, `/etc/etcd/tls/peer/{tls.crt,tls.key,ca.crt}` | `--peer-cert-file`, `--peer-key-file`, `--peer-trusted-ca-file` | `spec.tls.peer.secretRef` |

The cluster controller propagates the per-Pod secret references (server, peer) onto each `EtcdMember` at creation, so the member controller builds Pods without re-reading the cluster spec. Operator-side material (the operator-client secret) stays on the parent cluster — the member controller fetches the cluster only when it needs an etcd client itself.

### Modes from field presence

Two boolean knobs on the client plane, each derived from which fields are populated:

- **Client TLS off** → `spec.tls.client` absent. Plaintext on 2379.
- **Server TLS only** → `serverSecretRef` set, `operatorClientSecretRef` absent. Encryption, no client identity. `serverSecretRef`'s `ca.crt` is required for the operator to verify the server.
- **Full mTLS** → both refs set. `--client-cert-auth=true` and `--trusted-ca-file` pointed at the server-secret's `ca.crt`. Operator presents its own cert when dialing.

Peer is simpler — it's a closed mesh:

- **Peer TLS off** → `spec.tls.peer` absent. Plaintext on 2380.
- **Peer TLS on** → `secretRef` set. Always mTLS (`--peer-client-cert-auth=true`); there is no useful encrypt-only mode for a symmetric peer plane.

### Constraints on the client-plane CA topology

The trust bundle in `serverSecretRef.ca.crt` is consumed twice when mTLS is on: as the operator's `RootCAs` (for verifying the server) and as etcd's `--trusted-ca-file` (for verifying incoming client certs). The trust bundle MUST therefore include both **the CA that signed the server cert** and **the CA that signed the operator-client cert**. In the common one-CA-per-cluster topology these are the same content; with two CAs on the client plane the user bundles both PEM blocks into a single `ca.crt`.

This isn't a documentation preference — etcd's grpc-gateway loopback dials its own client API and presents the **server cert as a client cert** for that self-dial. If the server's issuing CA isn't in `--trusted-ca-file`, the loopback fails chain validation and the server logs become a steady stream of `x509: certificate signed by unknown authority` errors. For the same reason the server cert MUST carry `clientAuth` in its EKU alongside `serverAuth` — Go's `crypto/tls` enforces `ExtKeyUsageClientAuth` server-side when verifying client certs.

### Peer-plane SAN constraint

The peer-plane verification in etcd does *more* than the standard Go TLS chain check: it reverse-DNS-looks-up the connecting peer's source IP and matches the resulting PTR record against the cert's DNS SANs (`client/pkg/transport/listener_tls.go`'s `checkCertSAN`). Kubernetes' DNS returns the fully-qualified `<pod>.<svc>.<ns>.svc.<cluster-domain>` form for pod IPs, so the peer cert SAN list MUST include `*.<cluster>.<ns>.svc.<cluster-domain>` (the wildcard with the cluster DNS suffix appended) in addition to `*.<cluster>.<ns>.svc`. Without the second wildcard the seed will silently EOF every incoming peer-TLS connection from a non-seed member with a hard-to-diagnose `rejected connection on peer endpoint` log line, and the new pods crashloop on `discovery failed`.

The cluster DNS suffix is environment-dependent — `cluster.local` on most upstream k8s, `cozy.local` on Cozystack — see [installation: TLS-enabled variant](installation.md#tls-enabled-variant) for how to identify it.

### Readiness probe under TLS

Every member Pod exposes a plaintext metrics listener on container port 2381 (named `metrics`) regardless of TLS state. etcd is started with `--listen-metrics-urls=http://0.0.0.0:2381`; the readiness probe targets `:2381/health` unconditionally. Bound to `0.0.0.0` rather than `localhost` because kubelet's HTTPGet probe dials the Pod IP, not loopback — a `127.0.0.1`-only listener would be unreachable from the kubelet. The same listener is what Prometheus-style scrapers (Cozystack's `VMPodScrape`, kube-prometheus's `PodMonitor`, etc.) target via the named `metrics` port. `/health` and `/metrics` are the only things exposed on this port; neither is sensitive (both are already reachable via the TLS-protected client API).

### What the operator does NOT manage (Phase 1)

- **Cert issuance.** The user creates the Secrets out-of-band. cert-manager integration is a Phase 2 follow-up.
- **Cert rotation.** Renewing a cert means updating the Secret and bouncing the Pods. Until the operator watches the referenced Secrets and rolls Pods on RV change, rotation is a manual one-Pod-at-a-time `kubectl delete pod`.
- **SAN validation.** The operator does not parse the user's cert to verify SANs cover the required DNS / IP names; etcd will fail to start (or self-dial loops will spam logs) if the cert is wrong. Required SANs are listed in `docs/installation.md`.

## PodDisruptionBudget

Every `EtcdCluster` gets a per-cluster `PodDisruptionBudget` (`policy/v1`) named after the cluster. The PDB is what makes `kubectl drain` safe: it tells the apiserver "this many of my Pods may be voluntarily unavailable at once". Without it, a drain can evict more-than-quorum voters before the operator can react and the cluster loses consensus.

### Selector and budget

- **Selector**: `etcd.lllamnyp.su/cluster=<name>, etcd.lllamnyp.su/role=voter`. Only voting members are protected; learners can be evicted freely (a learner-only loss does not affect quorum, and the operator's existing scale-up flow will re-add a learner if the cluster was mid-promotion).
- **MaxUnavailable**: `(votingMembers - 1) / 2`, integer-divided so the result floors automatically. For 1 voter → 0 (any disruption is quorum loss). For 3 → 1, 4 → 1, 5 → 2, 7 → 3.

### Where the `role=voter` label comes from

The cluster controller is the source of truth for whether a member is a voter; it learns this from etcd's `MemberList` (specifically `IsLearner=false`). It writes `Status.IsVoter` onto each `EtcdMember`. The member controller reads `Status.IsVoter` and patches its Pod's `etcd.lllamnyp.su/role=voter` label accordingly. The new label is visible to the PDB by the next cluster-controller reconcile after promotion — three reconcile cycles end-to-end (cluster writes `IsVoter` → member patches Pod label → cluster's next pass picks up the new voter Pod via `reconcilePDB`). The controller boundaries stay clean: the cluster controller never patches a Pod directly.

The seed is **pre-stamped** with `Status.IsVoter=true` at creation — it's never a learner, so the operator skips the round-trip and the Pod gets the role label on the very first reconcile, closing the bootstrap-window protection gap.

### Transient races

Two windows exist; both are safe:

- **Scale-up (after promote).** Etcd's `MemberList` reports N+1 voters but `Status.IsVoter` for the freshly-promoted member hasn't been patched yet. The PDB therefore protects N voter Pods. A drain in this window could evict the unlabelled new voter (no PDB protection) — etcd is left with N voters running of N+1 registered. Etcd's write quorum for an M-voter cluster is `⌊M/2⌋+1`, so for M=N+1 the cluster still tolerates one missing voter as long as N ≥ 1.
- **Scale-down (after `MemberRemove`).** Etcd has N-1 voters but the victim's Pod is briefly Terminating. The PDB's own selector still matches the Terminating Pod, but the k8s PDB controller's `currentHealthy` counts only Pods whose `Ready` condition is `True` — once kubelet flips the Terminating Pod's `Ready` to `False` (which happens at the start of graceful shutdown, before the Pod is gone), it stops counting toward the budget's healthy total. The in-flight removal is naturally accounted for and the budget shrinks accordingly.

Both windows are one reconcile cycle wide.

### What happens with zero voters

Pre-bootstrap, paused (PVC clusters at `replicas: 0`), or wedged: voter count is 0 and the operator **deletes** the PDB entirely. A PDB with zero matching Pods and a stale `MaxUnavailable` from a prior state would mislead `kubectl get pdb`; better to leave nothing than to leave noise.

## Conditions

The cluster surfaces three conditions: `Available`, `Progressing`, `Degraded`. The interesting state space is on `Available`:

| `Available` | `Reason` | Meaning |
|---|---|---|
| `True` | `QuorumHealthy` | All members ready, target reached. The good state. |
| `True` | `QuorumAvailable` | More than half ready, less than all. Cluster serves; some members unhealthy. Paired with `Degraded=True/MembersUnhealthy`. |
| `True` | `ClusterDiscovered` | Bootstrap discovery just succeeded; `clusterID` latched. Transient. |
| `False` | `Paused` | `spec.replicas=0`. Message names the parked PVC if a dormant member exists, otherwise says no data was ever written. |
| `False` | `QuorumLost` | Less than half ready. Cluster cannot make progress. |
| `False` | `ClusterUnreachable` | Discovery couldn't dial etcd (DNS failure, network partition, etcd not yet listening). |
| `False` | `BootstrapFailed` | Deadline expired before `clusterID` was latched. Terminal — recovery is delete and recreate. |
| `False` | `DeadlineExceeded` | Deadline expired after bootstrap. Terminal — recovery is to edit spec. |

`Progressing` distinguishes "actively reconciling" from "we hit a wall":

| `Progressing` | `Reason` | Meaning |
|---|---|---|
| `True` | `InitialSnapshot` | First-reconcile token + observed latch just happened. |
| `True` | `SpecChanged` | Previous target reached; adopting the new spec. |
| `True` | `WaitingForSeed` | Bootstrap seed CR exists but its Pod hasn't been created yet. |
| `True` | `RetryAfterDeadline` | Deadline-exceeded recovery: user edited spec after a steady-state deadline. |
| `False` | `Reconciled` | At steady state with the current `observed`. |
| `False` | `Paused` | Same as Available; emitted when `desired==0`. |
| `False` | `BootstrapFailed` / `DeadlineExceeded` | Terminal states; see Available. |

`Degraded` is `True` whenever `Available=True/QuorumAvailable` (partial outage) or `Available=False/QuorumLost`. `False` in healthy or paused states. In other words, `Degraded` means "the cluster is not delivering its full intended capacity right now"; reading `Degraded` alone tells an alerting layer whether to page someone.

All conditions carry `observedGeneration` so consumers can tell whether a condition reflects the latest spec. Status writes are gated on "did anything actually change" — the operator does not bump `resourceVersion` every 30 s just because of the periodic reconcile.

## What is not in the design

A few things that recur in similar operators but are intentionally absent here:

- **No automatic broken-member replacement for PVC clusters.** `isBroken` is a real predicate only for memory-backed members (Pod lost → memory gone → member replaced); for PVC-backed members it stays a stub. The replacement policy (corruption? irrecoverable crashloop? quorum-loss handling?) is a richer decision and not yet wired up. Broken PVC members stay broken and require an explicit user action (see [operations.md](operations.md#broken-member)).
- **No leader-aware client routing.** Each etcd-client call balanced by clientv3 lands on whatever endpoint is first responsive. Filtering to non-learner endpoints (the issue #12 fix) handles the "rpc not supported for learner" case, but heavy `MemberList` traffic can still spread across followers. A leader-aware proxy or a sidecar that intercepts apiserver→etcd traffic is the proper fix; not in scope here.
- **No TLS, no auth/RBAC inside etcd.** v1alpha2 ships HTTP-only. Adding TLS means wiring cert-manager (or equivalent), rotating certs, and threading them through the Pod spec. Doable but a separate concern.

See [`What's not supported`](../README.md#whats-not-supported-yet) in the README for the running follow-up list.
