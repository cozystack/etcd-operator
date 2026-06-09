# Meet etcd-operator in Cozystack

Aenix has donated the etcd operator to the Cozystack project!

Not just a formal adoption, but also an improved implementation under new API version:
`etcd-operator.cozystack.io/v1alpha2`. This
page is the orientation: what the operator does, what changed if you used the
older `etcd.aenix.io/v1alpha1` operator, and how the feature set lines up
against a popular alternative.

The guiding idea of `v1alpha2` is **make it better while keeping the good**.
The parts that worked — declarative clusters, in-place storage, the Cozystack
and Kamaji integration shape — are preserved, often verbatim, so existing
operational habits carry over. The parts that were fragile — a free-form
options map, StatefulSet-shaped lifecycle, webhook-dependent validation — are
rebuilt on stricter, protocol-aware foundations. Nothing good was thrown away
to get there; where a thing moved, there is a migration path, not a cliff.

---

## If you're new to etcd-operator

The operator runs [etcd](https://etcd.io/) clusters on Kubernetes through two
custom resources:

- **`EtcdCluster`** — what *you* create. Cluster-wide intent: replica count,
  etcd version, per-member storage, TLS, auth, tuning.
- **`EtcdMember`** — what the *operator* creates, one per etcd member. Each owns
  its own Pod and PVC. You don't edit these.

There is deliberately **no StatefulSet**. Each member's Pod and PVC are
reconciled independently, which lets the operator model etcd's real lifecycle —
learner-mode joins, member-ID assignment, graceful `MemberRemove`,
scale-to-zero pause/resume — without fighting StatefulSet's "every replica is
interchangeable" assumption. The full rationale is in
[concepts.md](concepts.md).

What you get out of the box today:

- **Bootstrap, scale up/down** one member at a time (learner-mode adds,
  promote, graceful removal).
- **Scale to zero** (`spec.replicas: 0`) to park a cluster without losing its
  data, and resume it with the same cluster and member IDs.
- **Storage choices**: PVC-backed by default, or opt-in `Memory` (tmpfs) for
  reconstructable state; per-cluster `storageClassName`.
- **TLS** on the client and peer surfaces independently — bring your own
  Secrets or let the operator drive **cert-manager** issuance (auto-renewing).
- **Single-user (`root`) authentication** with bring-your-own credentials.
- **Snapshots & restore**: one-shot `EtcdSnapshot` to S3 or a PVC, and
  restore-on-bootstrap from a snapshot.
- **Safety rails**: an auto-emitted `PodDisruptionBudget` that won't let a
  drain break quorum, apiserver-enforced **CEL validation** (no webhook, no
  cert dependency), and a locking pattern that keeps mid-flight spec edits from
  corrupting consensus.
- **Ops integration**: the `/scale` subresource (so `kubectl scale` and a
  `VerticalPodAutoscaler` work), an always-on plaintext metrics port at `2381`,
  pass-through `affinity` / `topologySpreadConstraints`, and merged
  `additionalMetadata`.

Start with the [Quick start in the README](../README.md#quick-start), then
[installation.md](installation.md) and [operations.md](operations.md).

---

## If you're coming from the old version

If you ran the legacy aenix operator (`etcd.aenix.io/v1alpha1`), here is what
changed — and, just as importantly, what didn't. The full procedure, including
the in-place migration tool, lives in [migration.md](migration.md).

| Area | Old (`etcd.aenix.io/v1alpha1`) | New (`etcd-operator.cozystack.io/v1alpha2`) | Why |
| --- | --- | --- | --- |
| **API group** | `etcd.aenix.io` | `etcd-operator.cozystack.io` | New home; old identity scrubbed. |
| **Workload model** | StatefulSet | Per-member `EtcdMember` CRs, no StatefulSet | Protocol-aware lifecycle: learner joins, member-ID assignment, graceful removal, pause/resume. |
| **etcd tuning** | Free-form `spec.options` map | Typed `spec.options` (closed set) | A string→string map let users inject operator-conflicting flags. The new set types exactly the keys actually used; new flags land as new fields, not an escape hatch. |
| **Backups** | `EtcdBackup` | `EtcdSnapshot` | Pre-GA naming change; semantics preserved. |
| **Validation** | webhook | apiserver CEL on the CRD | Removes the webhook + cert-manager dependency from the validation path. |
| **Services** | ClusterIP | one Service reshaped to headless | Required for stable per-member peer DNS; has consumer prerequisites — read migration.md first. |
| **Auth** | — | single-user `root`, BYO credentials Secret | New capability (see below). |

**Migration is in-place.** The `etcd-migrate` tool adopts a running legacy
cluster without moving data, restarting a pod, or touching quorum — it changes
ownership, labels, member annotations and CRs only, and the new operator takes
over the live data plane. Clients that connect by DNS name keep working. See
[migration.md](migration.md) for the `--apply` prerequisites (especially the
one Service that changes shape and the auth-credentials step).

### What `v1alpha2` fixed from the old implementation

The rebuild lands the work behind a long tail of issues tracked against the
prior implementation. A selection, grouped by theme:

- **Auth & RBAC**: enable etcd auth ([#160](https://github.com/cozystack/etcd-operator/issues/160)), RBAC switcher design ([#195](https://github.com/cozystack/etcd-operator/issues/195)).
- **TLS**: single CA for server+client certs as Kamaji requires ([#162](https://github.com/cozystack/etcd-operator/issues/162)); replace the deprecated `gcr.io/kubebuilder/kube-rbac-proxy` image ([#271](https://github.com/cozystack/etcd-operator/issues/271)); fix flaky TLS cluster creation ([#264](https://github.com/cozystack/etcd-operator/issues/264)).
- **Storage & tuning**: EmptyDir/memory storage ([#214](https://github.com/cozystack/etcd-operator/issues/214)); enable autocompaction ([#222](https://github.com/cozystack/etcd-operator/issues/222)); set `quota-backend-bytes` from requested size ([#211](https://github.com/cozystack/etcd-operator/issues/211)); optimize default CPU/memory quotas ([#199](https://github.com/cozystack/etcd-operator/issues/199)).
- **Observability & ops**: metrics Service/port ([#273](https://github.com/cozystack/etcd-operator/issues/273)); detailed logs for ConfigMap and Service ([#177](https://github.com/cozystack/etcd-operator/issues/177)); disable the development logger ([#208](https://github.com/cozystack/etcd-operator/issues/208)); fix logging-system issues ([#235](https://github.com/cozystack/etcd-operator/issues/235)); the `kubectl-etcd` plugin ([#212](https://github.com/cozystack/etcd-operator/issues/212)).
- **API & correctness**: use `corev1.PodSpec`-shaped passthrough ([#172](https://github.com/cozystack/etcd-operator/issues/172)); sorting for extra keys ([#248](https://github.com/cozystack/etcd-operator/issues/248)); breaking Service rename ([#166](https://github.com/cozystack/etcd-operator/issues/166)).

---

## How it compares: a popular alternative

[etcd-io/etcd-operator](https://github.com/etcd-io/etcd-operator) is a popular
alternative etcd operator, and its
[roadmap](https://github.com/etcd-io/etcd-operator/blob/main/docs/roadmap.md)
is a useful yardstick because it enumerates what a "complete" etcd operator
should do. Here is where `v1alpha2` stands against each of its milestones.

Legend: ✅ implemented · 🟡 partial / by-design-different · ⬜ not yet.

### v0.1.0 — Core lifecycle

| Roadmap item | Status | Notes |
| --- | --- | --- |
| Create a 3/5-member cluster | ✅ | Single seed first, learner-mode adds afterwards. |
| Understand cluster health | ✅ | `status` conditions (`Available`/`Progressing`/`Degraded`), `readyMembers`, per-Pod `/health` on `:2381`. |
| Scale in/out (1→3→5 and back) | ✅ | One member at a time, as learners; scale-down via `MemberRemove` finalizer. |
| Customize etcd options | ✅ 🟡 | Supported, but as a **typed closed set** (`quota-backend-bytes`, auto-compaction mode/retention, `snapshot-count`) rather than a free-form flag map — on purpose. |

### v0.2.0 — Security & upgrades

| Roadmap item | Status | Notes |
| --- | --- | --- |
| Enable TLS, with certificate renewal | ✅ | Client and peer planes independently; BYO Secrets or operator-driven cert-manager Certificates that auto-renew. |
| Cross-version upgrades (patch/minor) | 🟡 | `spec.version` applies to **newly-created** members (scale-up, replacement); there is no in-place rolling upgrade of existing Pods yet. |

### v0.3.0 — Failure recovery

| Roadmap item | Status | Notes |
| --- | --- | --- |
| Recover a single failed member (quorum held) | 🟡 | Pod restart / node failure: PVC is preserved and the member rejoins with the same ID. Memory-backed members **auto-replace** on Pod loss. Automatic replacement for *broken* PVC-backed members is not wired yet (`status.brokenMembers` reads 0). |
| Recover from multiple failures (quorum loss) | ⬜ | Majority-failure recovery is tracked but not implemented ([#202](https://github.com/cozystack/etcd-operator/issues/202)). |

### v0.4.0 — Backup & restore

| Roadmap item | Status | Notes |
| --- | --- | --- |
| On-demand backups | ✅ | `EtcdSnapshot` runs a Job that snapshots to S3 or a PVC; `status.artifact` records URI, size, checksum. |
| Periodic automated backups | 🟡 | Intentionally **out of scope** as a CRD — drive recurring snapshots with a `CronJob`/`kubectl apply`. Schedule-from-etcd-druid is under research ([#257](https://github.com/cozystack/etcd-operator/issues/257)). |
| Create a cluster from a backup | ✅ | `spec.bootstrap.restore.source` (S3 or PVC); the seed Pod restores before etcd starts. TLS and auth are honored. |

### v1.0.0

| Roadmap item | Status | Notes |
| --- | --- | --- |
| Helm chart distribution | 🟡 | Install today is kustomize-based (`make install` / `make deploy`); Helm packaging already implemented in Cozystack. |

### Issues there that `v1alpha2` already addresses

Beyond the roadmap milestones, the [etcd-io/etcd-operator issue
tracker](https://github.com/etcd-io/etcd-operator/issues) lists open feature
requests and gaps that this operator already solves. A selection (their issue
numbers), with how `v1alpha2` stands on each:

| etcd-io/etcd-operator issue | Status here | How |
| --- | --- | --- |
| [#302](https://github.com/etcd-io/etcd-operator/issues/302) Support enabling etcd authentication | ✅ | `spec.auth.enabled` provisions the `root` user from a BYO credentials Secret and runs `auth enable` once quorum converges. |
| [#333](https://github.com/etcd-io/etcd-operator/issues/333) Recover a single failed member (quorum held) | 🟡 | PVC is preserved and the member rejoins with the same ID after Pod/node loss; memory-backed members auto-replace. No automatic replacement for *broken* PVC-backed members yet. |
| [#135](https://github.com/etcd-io/etcd-operator/issues/135) / [#357](https://github.com/etcd-io/etcd-operator/issues/357) Status reporting on `EtcdCluster` | ✅ | `status` conditions (`Available`/`Progressing`/`Degraded`), `readyMembers`, `clusterID`, `authEnabled`, and a populated `/scale` selector. |
| [#217](https://github.com/etcd-io/etcd-operator/issues/217) Make labels consistent with Kubernetes | ✅ | Owned objects carry the `app.kubernetes.io/*` set plus cluster/role labels; `spec.additionalMetadata` merges user labels/annotations, operator keys winning on collision. |
| [#321](https://github.com/etcd-io/etcd-operator/issues/321) Flexible pod template | 🟡 | Addressed by design with typed pass-through fields (`affinity`, `topologySpreadConstraints`, `resources`, `additionalMetadata`) rather than a free-form strategic-merge `podTemplate`. |

Some of their open issues remain open here too, by design or by scope — notably
in-place upgrades ([#327](https://github.com/etcd-io/etcd-operator/issues/327))
and recovery from multiple failed members / quorum loss
([#359](https://github.com/etcd-io/etcd-operator/issues/359)). See
[What's intentionally not here](#whats-intentionally-not-here-yet).

### Beyond that roadmap

`v1alpha2` also ships capabilities that roadmap doesn't enumerate,
driven by the Cozystack/Kamaji multi-tenant use case:

- **Scale to zero** (pause/resume) preserving cluster and member identity.
- **Memory-backed (tmpfs) storage** with operator-driven member replacement.
- **Apiserver-side CEL validation** — no webhook, no cert dependency.
- **Auto-emitted PodDisruptionBudget** scoped to voting members.
- **`/scale` subresource + populated `status.selector`** so `kubectl scale` and
  a `VerticalPodAutoscaler.targetRef` work directly.
- **Pass-through scheduling** (`affinity`, `topologySpreadConstraints`) and
  **merged `additionalMetadata`** across every owned object.
- **In-place migration tool** from the legacy operator.
- **`kubectl-etcd` plugin** for day-2 operations.
