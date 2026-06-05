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

Two things are not auto-defaulted and matter for production memory clusters — both tracked in [issue #16](https://github.com/lllamnyp/etcd-operator/issues/16):

- **Pod anti-affinity**. Configurable via `spec.affinity` (see [Pod scheduling and additional metadata](#pod-scheduling-and-additional-metadata)) but not defaulted. Without it, scheduling can co-locate voters on one node; a single node failure then loses quorum on a 3-member cluster.
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

## Pod scheduling and additional metadata

Three spec fields shape where member Pods land and what metadata the operator's child objects carry. All three are latched through `status.observed` like the rest of the target spec (see [Locking pattern](#locking-pattern)) and apply **at object creation only** — the operator does not roll existing Pods or re-stamp existing objects when they change.

### `spec.affinity` and `spec.topologySpreadConstraints`

Passed through verbatim to each member Pod's `spec.affinity` / `spec.topologySpreadConstraints`. The common production use is a required pod anti-affinity keyed on the cluster label, so two voters never share a node:

```yaml
spec:
  affinity:
    podAntiAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        - labelSelector:
            matchLabels:
              etcd-operator.cozystack.io/cluster: my-etcd
          topologyKey: kubernetes.io/hostname
```

Changes take effect on newly-created members (scale-up, replacement). To apply a scheduling change to an existing cluster, delete one Pod at a time and let the operator recreate it with the new constraints.

### `spec.additionalMetadata`

Extra labels and annotations the operator merges onto **every object it creates** for the cluster: member Pods, the per-member data PVCs (`data-<member>`), the client and headless Services, the PodDisruptionBudget, and the `EtcdMember` CRs. Typical uses are backup-tool selectors on the PVCs and cost-allocation/tenancy labels across the board.

Merge semantics:

- **Operator-owned keys win.** A user-supplied key that collides with a key the operator already sets (the `app.kubernetes.io/*` set, the cluster/role labels, or any operator-set annotation) is silently ignored — the field cannot shadow the operator's own selectors or metadata. The rule holds symmetrically for labels and annotations.
- **Apply-on-create.** Objects are stamped when created; editing the field re-stamps nothing retroactively. Newly-created objects (scale-up members and their PVCs, replacements) pick up the latest latched value.
- **Latched.** A mid-flight edit only takes effect once the current `status.observed` target is reached, like every other latched field.

## etcd tuning options

`spec.options` carries the etcd server tuning flags the operator renders onto each member's command line:

```yaml
spec:
  options:
    quotaBackendBytes: 10200547328   # --quota-backend-bytes
    autoCompactionMode: periodic     # --auto-compaction-mode (periodic | revision)
    autoCompactionRetention: "5m"    # --auto-compaction-retention
    snapshotCount: 10000             # --snapshot-count (raft entries, not backups)
```

Every field is optional; an unset field emits no flag, leaving etcd's built-in default in force.

The set is deliberately **closed and typed**. The legacy aenix operator exposed `spec.options` as a free-form `map[string]string`, which let users inject arbitrary flags — including ones that conflict with flags the operator itself manages (listen URLs, initial-cluster wiring, TLS paths). This operator types exactly the keys Cozystack's etcd package actually used; a new tuning knob lands as a new typed field with validation, not via an escape hatch.

Like `spec.resources`, options are latched through `status.observed` and apply **to newly-created members only** (scale-up, replacement) — the operator does not roll existing Pods when they change. To apply a tuning change to an existing cluster, delete one Pod at a time and let the operator recreate it with the new flags. A transient mix of old- and new-flag members is harmless: these are per-member settings (backend quota, compaction cadence, raft snapshot interval), the same heterogeneity any manual rolling flag change passes through.

## TLS

`spec.tls` configures transport-layer security for the cluster's two etcd surfaces: the client API (port 2379) and the peer API (port 2380). Each subtree is independently optional — you can opt one surface into TLS without the other. The whole `tls` subtree is immutable post-create (see the validation table above): toggling TLS on an existing cluster is a rolling change that v1 doesn't perform, so the policy is delete-and-recreate.

Material can come from one of two sources per subtree, mutually exclusive:

- **BYO Secrets** — user provides `kubernetes.io/tls`-shaped Secrets and points `serverSecretRef` / `operatorClientSecretRef` / `secretRef` at them.
- **cert-manager** — user provides an Issuer or ClusterIssuer and the operator emits `cert-manager.io/v1` `Certificate` resources at reconcile time. See [cert-manager-driven TLS](#cert-manager-driven-tls) below.

### Census of certs

| Artifact | Mount / location | etcd flag | Source (BYO) | Source (cert-manager) |
|---|---|---|---|---|
| Server cert + key (client API) | each etcd Pod, `/etc/etcd/tls/client/{tls.crt,tls.key}` | `--cert-file`, `--key-file` | `spec.tls.client.serverSecretRef` | Secret `<cluster>-server-tls` produced by a Certificate signed by `serverIssuerRef` |
| Client trust bundle (client API) | each etcd Pod, `/etc/etcd/tls/client/ca.crt` | `--trusted-ca-file` (mTLS only) | `ca.crt` key of `serverSecretRef`'s Secret | `ca.crt` key of `<cluster>-server-tls` (the Issuer's CA cert) |
| Operator's client cert + key | operator Pod (not mounted into etcd Pods — read by the operator's etcd client) | n/a (Go `tls.Config.Certificates`) | `spec.tls.client.operatorClientSecretRef` | Secret `<cluster>-operator-client-tls` produced by a Certificate signed by `operatorClientIssuerRef` |
| Peer cert + key + trust | each etcd Pod, `/etc/etcd/tls/peer/{tls.crt,tls.key,ca.crt}` | `--peer-cert-file`, `--peer-key-file`, `--peer-trusted-ca-file` | `spec.tls.peer.secretRef` | Secret `<cluster>-peer-tls` produced by a Certificate signed by `issuerRef` |

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

### cert-manager-driven TLS

Instead of authoring Secrets out-of-band, point each subtree at a cert-manager Issuer (or ClusterIssuer) under `spec.tls.{client,peer}.certManager`:

```yaml
spec:
  tls:
    client:
      certManager:
        serverIssuerRef:         { name: my-ca, kind: Issuer }
        operatorClientIssuerRef: { name: my-ca, kind: Issuer }   # presence ⇒ mTLS on
    peer:
      certManager:
        issuerRef: { name: my-peer-ca, kind: Issuer }
```

The cluster controller emits up to three `cert-manager.io/v1` `Certificate` resources per cluster (server, optional operator-client, peer). Each `Certificate`:

- Is owned by the `EtcdCluster` (cascading GC on cluster delete, which also GCs the resulting Secret).
- Specifies the SANs, EKUs, and `ipAddresses` etcd needs (server gets the wildcard short + FQDN, the headless and client service DNS, `localhost`, `127.0.0.1`; peer gets the wildcard short + FQDN; operator-client has no SAN).
- Writes the cert into a conventionally-named Secret (`<cluster>-server-tls`, `<cluster>-operator-client-tls`, `<cluster>-peer-tls`) which the rest of the operator consumes the same way it consumes a BYO Secret — `buildPod` and `buildOperatorTLSConfig` are source-agnostic.

The CRD shape enforces exactly one source per subtree via CEL (`secretRef` XOR `certManager`), and `tls.client.operatorClientSecretRef` cannot coexist with `tls.client.certManager` — the mTLS toggle in cert-manager mode lives at `certManager.operatorClientIssuerRef`.

**Cluster DNS suffix.** The FQDN form of the emitted SANs (`*.<cluster>.<ns>.svc.<cluster-domain>`) needs the cluster's actual DNS suffix. The operator auto-discovers it from `/etc/resolv.conf`'s `search` line at startup — covering `cluster.local`, Cozystack's `cozy.local`, and any other kubelet-injected suffix for normal cluster-pod deployments — and falls back to `cluster.local` only when auto-discovery yields nothing (hostNetwork pods, custom `dnsPolicy`). Override explicitly with `--cluster-domain=<suffix>` when neither path finds the right value. See [installation: prerequisites for cert-manager mode](installation.md#prerequisites-for-cert-manager-mode).

**Single-Issuer assumption.** In the happy path the same Issuer signs both the server cert and the operator-client cert, so the CA visible in each Secret's `ca.crt` is the same content — doubling as etcd's `--trusted-ca-file`. Splitting the two Issuers across different root CAs would require the user to ensure both root CAs reach the server's trust bundle; that case is an edge to discuss later.

**cert-manager not installed.** The operator probes the discovery API for `cert-manager.io/v1` at startup. When it isn't registered, a cluster whose `spec.tls` references `certManager` is parked at `Available=False / CertManagerNotInstalled` and the operator never touches the cert-manager.io GVK — avoiding the controller-runtime cached-client failure mode where a missing CRD traps the reflector in a permanent LIST retry. Recovery is "install cert-manager, restart the operator"; the discovery probe re-runs at every operator start.

### What the operator does NOT manage (yet)

- **Cert rotation.** cert-manager handles renewal of operator-emitted Certificates automatically (the resulting Secret gets new bytes); the operator does NOT yet watch the Secret and roll Pods. In-place rotation requires manual one-Pod-at-a-time `kubectl delete pod`.
- **Trust-bundle separate ref.** Use cases like multi-CA trust during rotation or cert-manager `trust-manager` `Bundle` resources still require a custom BYO Secret with a hand-constructed `ca.crt`. Not in the happy path.
- **SAN validation on BYO certs.** The operator does not parse the user's cert to verify SANs cover the required DNS / IP names; etcd will fail to start (or self-dial loops will spam logs) if the cert is wrong. Required SANs are listed in [`docs/installation.md`](installation.md#tls-enabled-variant).

## Authentication

Transport TLS encrypts the wire; etcd's own **authentication** layer controls *who* may talk to the store. Set `spec.auth.enabled: true` and point at a Secret holding the root credentials:

```yaml
spec:
  tls:
    client:
      serverSecretRef: { name: my-server-tls }   # required — see below
  auth:
    enabled: true
    rootCredentialsSecretRef: { name: my-etcd-root }   # required
---
apiVersion: v1
kind: Secret
metadata:
  name: my-etcd-root
type: kubernetes.io/basic-auth
stringData:
  username: root        # for consumers; the etcd user is always root
  password: <choose>
```

When enabled, the operator provisions a single `root` user — with the **`password` from the referenced Secret** — granted etcd's built-in `root` role, and runs `auth enable`, after which the client API rejects anonymous access. This is a **single-user** model at parity with the legacy operator. Per-tenant users / RBAC are out of scope for now (a future `spec.auth.users`-style extension).

Mechanics worth knowing:

- **Requires `spec.tls.client`** (CEL-enforced). Auth credentials must not cross a plaintext wire, so server-TLS is the minimum; full mTLS also satisfies it.
- **Requires `spec.auth.rootCredentialsSecretRef`** (CEL-enforced). A `kubernetes.io/basic-auth` Secret in the cluster's namespace; the operator reads its `password` key. The etcd user is always `root` (etcd requires a user named `root` to enable auth), so the Secret's `username` should be `root`.
- **Immutable post-create** (CEL), like `spec.tls`. Enabling/disabling auth on a live cluster mutates persisted data-store state in lockstep with the operator's own client; v1 punts that to delete-and-recreate. The Secret *reference* is frozen too, and **in-place password rotation is not supported** — the operator reads the password fresh on every dial, so changing the Secret's contents after auth is on would desync it from etcd. Recreate to change the password.
- **No etcd startup flag, no Pod change.** Auth is a runtime operation persisted in the data store. The operator enables it via the etcd API *after* the cluster has converged to a healthy quorum — there is nothing to add to the member command line, so `EtcdMember` and the Pod spec are untouched.
- **`status.authEnabled`** latches `true` once `auth enable` succeeds. It is the single signal every operator etcd dial consults: `false` ⇒ dial anonymously (the bootstrap window, before the cluster is up), `true` ⇒ read the Secret and present the root credentials. This is what lets the operator keep managing membership before *and* after the flip — `clientv3` attempts an `Authenticate` RPC on connect only when a username is set, which would fail until auth is on. Provisioning is idempotent: a crash between `auth enable` and the status write is recovered on the next reconcile via `AuthStatus`.

Consumers of the cluster (e.g. a Kamaji `DataStore`) can point their own `basicAuth` at the same Secret once auth is enabled.

Enabling auth on an existing cluster — or migrating from the legacy operator's implicit `root:root` — has ordering and password-matching gotchas; see [migration: root credentials](migration.md#authentication-root-credentials-are-byo-and-required).

## PodDisruptionBudget

Every `EtcdCluster` gets a per-cluster `PodDisruptionBudget` (`policy/v1`) named after the cluster. The PDB is what makes `kubectl drain` safe: it tells the apiserver "this many of my Pods may be voluntarily unavailable at once". Without it, a drain can evict more-than-quorum voters before the operator can react and the cluster loses consensus.

### Selector and budget

- **Selector**: `etcd-operator.cozystack.io/cluster=<name>, etcd-operator.cozystack.io/role=voter`. Only voting members are protected; learners can be evicted freely (a learner-only loss does not affect quorum, and the operator's existing scale-up flow will re-add a learner if the cluster was mid-promotion).
- **MaxUnavailable**: `(votingMembers - 1) / 2`, integer-divided so the result floors automatically. For 1 voter → 0 (any disruption is quorum loss). For 3 → 1, 4 → 1, 5 → 2, 7 → 3.

### Where the `role=voter` label comes from

The cluster controller is the source of truth for whether a member is a voter; it learns this from etcd's `MemberList` (specifically `IsLearner=false`). It writes `Status.IsVoter` onto each `EtcdMember`. The member controller reads `Status.IsVoter` and patches its Pod's `etcd-operator.cozystack.io/role=voter` label accordingly. The new label is visible to the PDB by the next cluster-controller reconcile after promotion — three reconcile cycles end-to-end (cluster writes `IsVoter` → member patches Pod label → cluster's next pass picks up the new voter Pod via `reconcilePDB`). The controller boundaries stay clean: the cluster controller never patches a Pod directly.

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

## Snapshots & restore

Two surfaces, one agent. The operator image doubles as a snapshot agent: `main.go` dispatches on `os.Args[1]` so `manager snapshot-agent` / `manager restore-agent` run the agent and exit, while a bare `manager` runs the controller. This keeps one binary and one image — no separate agent build — and means the agent always matches the operator it ships with.

### Snapshot (`EtcdSnapshot`)

An `EtcdSnapshot` is a one-shot record. The controller resolves `spec.clusterRef`, builds a Job (owned by the `EtcdSnapshot`, with `ttlSecondsAfterFinished` so it self-GCs, `automountServiceAccountToken: false`, and a restricted security context), and tracks `status.phase` through `Pending → Started → Complete | Failed`. The Job's Pod runs the snapshot agent, which:

1. dials the cluster's client Service — TLS material (server CA + operator-client cert) is mounted from the same Secrets the operator dials with, and when `status.authEnabled` is latched the agent authenticates as `root` using the password from `spec.auth.rootCredentialsSecretRef`;
2. streams `clientv3` `Maintenance.Snapshot` to a local file (PVC destination) or a temp file it then multipart-uploads to S3, hashing (sha256) and counting bytes as it goes;
3. prints a marker line — `snapshot uploaded: uri="..." size=N sha256=<hex>` — that the controller scans out of the Pod log (via an uncached `APIReader` to find the Pod and the typed Clientset to read its log) to populate `status.artifact{uri,sizeBytes,checksum}`.

Why parse a log line rather than have the agent write status? The agent has no Kubernetes API access by design (`automountServiceAccountToken: false`), so the controller — which does — is the one that records the result. The marker is the agent→controller channel. Terminal phases are sticky: a `Complete`/`Failed` snapshot is a historical record and never re-runs.

Snapshot integrity note: a `Maintenance.Snapshot` stream carries no appended hash (unlike `etcdutl snapshot save`), so the sha256 in `status.artifact.checksum` is computed by the agent over the bytes it stored, and restore runs with `SkipHashCheck`.

### Restore (`spec.bootstrap.restore`)

Restore is a first-bootstrap-only path, not a controller that mutates a running cluster. When `spec.bootstrap.restore.source` is set, the cluster controller stamps the `RestoreSpec` onto the bootstrap **seed** `EtcdMember` (only the seed — scale-up members join the live cluster normally). The member controller's `buildPod` then prepends a `restore` init container (the operator image, `manager restore-agent`) that shares the etcd data volume. Before etcd starts, the agent fetches the snapshot (S3 download / PVC read) and runs `etcdutl` `snapshot.Restore` into the data dir, using the seed's exact identity — member name, `--initial-cluster`, cluster token, peer URL — so etcd accepts the rebuilt data dir.

The init container is idempotent: it no-ops if the data dir already contains a `member/` directory, so Pod restarts after first boot leave live data untouched and never re-download. Because `spec.bootstrap` is CEL-immutable post-create, the restore intent can't be added to or changed on a live cluster — restore happens once, at birth, or not at all. A restored cluster gets a fresh etcd cluster ID: it is a new cluster seeded with old data, not a continuation.

The rebuild uses the `etcdutl` vendored into the operator image, whose on-disk storage format is minor-version-specific. So restore requires `spec.version` to match that `etcdutl`'s minor (currently etcd **3.6.x**): the agent reads `spec.version` (passed as `ETCD_VERSION`) and **fails the restore early** with an actionable message if the major.minor differs, rather than rebuilding a data dir an older etcd would fail to boot. Restoring into a different minor means using an operator build whose `etcdutl` matches. (Non-restore clusters are unaffected — this gate only fires on the restore path.)

This idempotency relies on the data dir being **persistent**. Restore is therefore rejected (by CEL) together with `spec.storage.medium: Memory`: a tmpfs data dir is wiped on every Pod restart, which would defeat the `member/`-exists guard and silently re-restore the original snapshot — reverting any writes since the restore, or breaking a multi-member cluster whose other members already moved past the restored cluster ID. Restore onto memory-backed storage is unsupported; use a PVC-backed cluster.

**Restoring an auth-enabled snapshot.** A snapshot serializes the whole data store, *including etcd's auth state* (users, roles, and the auth-enabled flag). Restoring a snapshot taken while auth was on yields a seed that boots with auth already enabled — so the new `EtcdCluster` must carry a matching `spec.auth` (`enabled: true` + `rootCredentialsSecretRef`, which transitively requires `spec.tls.client`). `reconcileAuth` is built for exactly this: when `spec.auth.enabled` is set it probes `AuthStatus` first, finds auth already on, and latches `status.authEnabled` *without* re-running `UserAdd`/`AuthEnable` — so the password is never reset and the operator simply adopts the restored credentials. The catch is that those credentials must be the *original* ones: etcd stores the root password's bcrypt hash in the snapshot, so the referenced Secret's `password` must equal the password in effect when the snapshot was taken. If `spec.auth` is omitted entirely, the decoupling that makes the bootstrap window correct works against you — `status.authEnabled` never latches, every operator dial stays anonymous (`resolveEtcdCredentials` returns no creds), and the restored etcd rejects them all. Rather than loop on an opaque error, the cluster controller detects the auth-required rejection while no credentials are configured and surfaces `Available=False` / `Degraded=True` with reason `AuthRequiredNotConfigured` and an actionable message, then stops retrying (auth is immutable post-create, so no spec edit recovers it — the fix is delete-and-recreate with `spec.auth`). The operator does **not** inspect the snapshot's auth state to pre-empt this, so the [restore runbook](operations.md#restoring-a-cluster-from-a-snapshot) calls it out as the operator's responsibility.

### Intentionally out of scope: scheduling

There is no `EtcdSnapshotSchedule`. Recurring snapshots are a `CronJob` that `kubectl apply`s date-stamped `EtcdSnapshot` objects — composable with the one-shot primitive without adding a cron surface (and its timezone/missed-run/concurrency semantics) to the operator. See the [snapshot runbook](operations.md#taking-a-snapshot) and [restore runbook](operations.md#restoring-a-cluster-from-a-snapshot).

## What is not in the design

A few things that recur in similar operators but are intentionally absent here:

- **No automatic broken-member replacement for PVC clusters.** `isBroken` is a real predicate only for memory-backed members (Pod lost → memory gone → member replaced); for PVC-backed members it stays a stub. The replacement policy (corruption? irrecoverable crashloop? quorum-loss handling?) is a richer decision and not yet wired up. Broken PVC members stay broken and require an explicit user action (see [operations.md](operations.md#broken-member)).
- **No leader-aware client routing.** Each etcd-client call balanced by clientv3 lands on whatever endpoint is first responsive. Filtering to non-learner endpoints (the issue #12 fix) handles the "rpc not supported for learner" case, but heavy `MemberList` traffic can still spread across followers. A leader-aware proxy or a sidecar that intercepts apiserver→etcd traffic is the proper fix; not in scope here.
- **No multi-user / RBAC inside etcd.** Single-user `root` authentication is available via `spec.auth.enabled` (password sourced from a referenced Secret — see [Authentication](#authentication)), but per-tenant users and role-based authorization are not yet wired up — every authenticated client is `root`.

See [`What's not supported`](../README.md#whats-not-supported-yet) in the README for the running follow-up list.
