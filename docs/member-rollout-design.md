# Design: mutable cluster settings and member rollout

## Why this exists

`EtcdCluster` has one setting that actually changes a running cluster — `spec.replicas`. Everything else is either CEL-locked to delete-and-recreate, or accepted by the apiserver and then quietly applied to nothing.

That second category is the part worth naming, because it is not obviously broken from the outside. `snapshotSpecIntoObserved` (`controllers/etcdcluster_controller.go:2019`) copies `version`, `storage`, `resources`, `affinity`, `topologySpreadConstraints`, `additionalMetadata`, `options` and `imagePullSecrets` into `status.observed`. Members mirror those same fields — but only at creation (`api/v1alpha2/etcdmember_types.go:85-120`, "existing members are not re-templated when the cluster spec changes"). And `reconciliationComplete` (`:2070`) asks only three questions: is the member count equal to `observed.Replicas`, is `ClusterID` latched, is every member `Ready`. It never asks whether a single member reflects the values it just snapshotted.

So editing `spec.resources` on a live cluster does this: snapshot into `observed`, set `Progressing=True/SpecChanged`, and then — because the member count never changed and every member is still Ready — immediately satisfy `reconciliationComplete` and settle to `Progressing=False/Reconciled`. The CR reports that it reconciled a change no member ever received. A user has no signal that anything was ignored.

Fixing that is not a matter of tightening one predicate. Making the settings genuinely mutable requires a mechanism for rolling members onto a new template safely, and that mechanism has to satisfy constraints that differ sharply by storage medium and by which setting changed. This document proposes that mechanism.

## The constraints it has to satisfy

**A restart is not the same operation on every cluster.** For a PVC-backed member, deleting the Pod is a restart: the data dir survives and the member rejoins with its log intact. For a memory-backed member the data dir is a tmpfs `emptyDir` whose lifetime is the Pod's (`etcdmember_controller.go:643`), so deleting the Pod is a *replacement* — the member comes back empty and must re-sync from its peers. The member controller already treats it that way, deleting the `EtcdMember` outright when a memory-backed member's Pod disappears (`:149-160`). Any rollout has to branch on this, not paper over it.

**Disrupting two members at once is not "twice as fast".** With three members, quorum is two, so exactly one may be down. Losing two costs availability on a PVC cluster and costs *the data* on a memory cluster, because no survivor holds it. The existing `pdbMaxUnavailable` formula (`:1500`) already encodes the arithmetic, but the PodDisruptionBudget it feeds only gates the Eviction API — the operator deletes Pods directly, so the PDB does not constrain the operator at all. Self-governance is the only protection there is.

**Some changes cannot be applied one member at a time in a single pass.** The CA-rotation case is covered by the companion research note *What etcd can and cannot hot-reload* (`docs/etcd-tls-reload.md`, not yet on `main`): leaf certs re-read per handshake, but CA trust pools are built once at process start and frozen. Rotating a CA means first widening every member's trust to `concat(old, new)` — a restart each — and only then swapping leaves. Applying both to one member before moving to the next would isolate it. The engine therefore needs a notion of ordered passes, not just a per-member loop.

**Some changes are not rollable at all**, and should stay locked: a PVC's `storageClassName` is immutable in Kubernetes, and `storage.medium` would mean migrating data between backends.

## The mechanism

### Target template and outdated members

`status.observed` already is the target template — it is the snapshot the cluster is trying to reach. What is missing is (a) a per-member record of which template that member was built from, and (b) a completion check that consults it.

Compare structurally rather than by stored hash: derive the member-affecting template from `status.observed` and compare it against the member's mirrored fields, the way `membersNeedingTLSHandover` already compares TLS. Structural comparison has no migration concern (no stored value to invalidate when the template shape changes) and no risk of a hash that is stable while the meaning underneath drifts. Expose a short digest in status purely as a human-readable label, never as the source of truth.

```
outdated(member) := memberTemplate(observed) != mirroredFields(member)
```

Then `reconciliationComplete` gains a fourth question — *is every member on the current template* — which closes the silent no-op above, and which alone converts "accepted and ignored" into "accepted and visibly in progress".

### The rollout step

A rollout step sits in the cluster reconcile between scale and steady state, and does *at most one member per pass*:

1. **Refuse to start** unless the cluster is at rest: member count equals `observed.Replicas`, no member has a deletion timestamp, no pending or learner member, no in-flight scale.
2. **Refuse to proceed** unless the cluster is healthy enough to lose one member — every member `Ready`, quorum present.
3. **Pick the victim deterministically.** Prefer a non-leader (avoids a gratuitous election), and prefer a non-seed. The seed preference is not cosmetic: self-heal is gated on `!member.Spec.Bootstrap`, and picking the seed is exactly the trap that #345 exists to fix in the e2e suite.
4. **Re-template** the victim's mirrored fields from `observed`.
5. **Effect it**, branching on medium:
   - *PVC-backed*: delete the Pod. The member controller rebuilds it from the updated spec.
   - *Memory-backed*: delete the `EtcdMember`. The finalizer does a clean `MemberRemove`; the cluster controller gap-fills a replacement at the new template, added via `MemberAddAsLearner` and promoted only after it has caught up. The learner path matters — a learner does not count toward quorum, so the add itself cannot cost the cluster availability.
6. **Gate before the next member** on the cluster having genuinely recovered, not merely on the Pod being `Ready`.

### What "recovered" has to mean

Pod readiness is too weak, particularly for a memory-backed replacement that rejoins empty. Before the next member is touched:

- `MemberList` shows the expected number of **voting** members, none `IsLearner`.
- `endpoint health` passes on **every** endpoint, not only the one just replaced.
- The replaced member's applied index has converged with the leader's.
- A linearizable read succeeds against the replaced member.

With three members the whole window runs at zero fault tolerance — two healthy voters, quorum of two. Sequentially that is a normal maintenance risk. In parallel it is the unrecoverable case.

### Ordered passes

A change contributes one or more passes. Each pass is a mutation plus whether it forces a restart; the engine completes a pass across **all** members before starting the next. Most changes are a single pass. CA rotation is two:

| Pass | Mutation | Restart | Why it must complete first |
|---|---|---|---|
| widen trust | trust file = `concat(old, new)` | yes, one per member | every member still presents an old-CA leaf, and every member trusts the old CA, so every pair authenticates at every intermediate step |
| swap leaves | leaf cert/key from new CA | only if the mount changes | everyone already trusts both CAs, so old-leaf and new-leaf members authenticate in both directions |

This is the generalization that makes the TLS handover expressible without a cluster-wide outage, and it costs nothing for the single-pass cases.

### One structural chokepoint

Every path that disrupts a member — this rollout, self-heal replacement, the TLS handover, anything added later — should go through a single function that refuses when another member is already disrupted. Enforcing it in one place rather than by convention is the difference between a rule and a habit: a simultaneous roll is precisely the shape of an unrecoverable event on a memory-backed cluster, and any future code path that grows one will hit the same wall. The invariant is worth stating as: **never more than one member's Pod deleted concurrently, for any reason, on any cluster.**

## Which settings become mutable

| Setting | Rollable | Mechanism | Notes |
|---|---|---|---|
| `replicas` | already is | scale up/down | existing machinery |
| `resources` | yes | 1 pass, Pod rebuild | |
| `affinity`, `topologySpreadConstraints` | yes | 1 pass, Pod rebuild | |
| `additionalMetadata`, `imagePullSecrets` | yes | 1 pass, Pod rebuild | |
| `options` (etcd flags) | yes | 1 pass, Pod rebuild | some flags deserve their own validation |
| `version` | yes | 1 pass, Pod rebuild | needs an upgrade policy of its own — see open questions |
| `storage.size` | partly | PVC: expand in place; memory: Pod rebuild | PVC expansion is a distinct mechanism, not a roll |
| TLS leaf material | no restart at all | per-handshake reload | free today; nothing to design |
| TLS CA / trust set | yes | 2 passes | widen, then swap |
| TLS on/off | not proposed | listener scheme is captured at start, and the operator's own client must switch in lockstep | leave locked |
| `auth` | not proposed | a data-plane change, not a Pod change | leave locked |
| `storage.medium` | no | data migration between backends | leave locked |
| `storage.storageClassName` | no | a PVC's `storageClassName` is immutable | leave locked |
| `bootstrap` | no | one-shot by definition | leave locked |

The CEL rules relax to match: fields in the rollable set drop their immutability rule, the rest keep it. That is a strictly additive API change — nothing that validates today stops validating.

## Status surface

- `status.updatedMembers` — how many members are on the current template. The rollout's progress in one number, and the field a human watches.
- A `RollingUpdate` condition, `True` while a rollout is in flight, with reasons distinguishing *waiting for health*, *rolling member N*, and *blocked*. It must be its own condition type: `updateStatus` rewrites `Available`, `Degraded` and `Progressing` from quorum health on every pass, so a rollout signal parked on any of them is overwritten within seconds.
- The existing progress-deadline machinery is the right escalation path for a rollout that wedges — no new timeout concept needed.

## What this does not solve

A rollout cannot make a cluster safer than its replica count allows. On a single-member cluster every one of these changes is a full outage, and on a memory-backed single-member cluster it is data loss; the engine should refuse rather than pretend. Likewise, none of this makes the genuinely-locked settings mutable — it narrows the locked set, it does not empty it.

## Open questions

1. **`spec.updateStrategy`?** A StatefulSet-style `RollingUpdate` vs `OnDelete` choice would let an operator take the disruption in their own maintenance window rather than whenever they edit the spec. `OnDelete` is cheap to add and is the conservative default for a datastore. Worth having, or unnecessary surface?
2. **Version upgrade policy.** etcd supports one-minor-at-a-time upgrades, and downgrades are a different operation entirely. Should `version` changes be gated to a single minor step and downgrades refused outright, or is that the user's problem?
3. **PVC expansion for `storage.size`.** In scope for this design, or a separate piece of work? It is not a roll — it is a PVC patch plus, for some CSI drivers, a Pod restart — so it may not belong in the same engine at all.
4. **Concurrency on larger clusters.** The proposal is one-at-a-time universally. For a 5- or 7-member PVC cluster the quorum arithmetic would permit 2 or 3, but the added complexity buys speed on a rare operation. I would keep it at one and revisit only if rollout duration becomes a real complaint.
5. **`maxUnavailable` on the PDB vs the engine.** These are now two encodings of the same arithmetic. Worth collapsing to one, or is it fine that the PDB governs drains while the engine governs itself?

## Confirmed vs. proposed

**Confirmed from the code**: the mirrored-fields-at-creation-only behaviour and its doc comments; that `reconciliationComplete` never inspects member content; that the PDB gates evictions while the operator deletes Pods directly; that memory-backed members are replaced rather than restarted on Pod loss; that the learner add/promote path already exists and is used by scale-up.

**Taken from prior research**, not re-derived here: everything in *What etcd can and cannot hot-reload* (`docs/etcd-tls-reload.md`) about leaf reload, frozen CA pools, and multi-CA trust — including that document's own caveat that kubelet's Secret projection reaching etcd is inferred rather than verified. The two-pass CA sequence rests on it. That note is not on `main` yet and cross-references a runbook section that lives on the parked TLS-handover branch, so it is deliberately not carried onto this branch; the two should land together or in that order.

**Proposed, not validated**: the rollout step ordering, the victim-selection preferences, and the health gate composition. None of these have been exercised against a cluster. The cheap validation is a scratch three-member cluster of each medium, rolling a trivial setting (`resources`) end to end before anything harder is attempted.
