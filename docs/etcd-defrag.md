# Defragmentation (`EtcdDefrag`)

etcd never reclaims backend disk on its own: compaction frees pages logically,
but the file — and the space counted against `--quota-backend-bytes` — stays
allocated until a **defragment** returns it. `EtcdDefrag` is how you ask the
operator to do that, safely.

It is a one-shot, run-to-completion record, modeled on [`EtcdSnapshot`](concepts.md#snapshots--restore):
the operator drives it through `status.phase` and it never re-runs.

**Scheduling.** `EtcdDefrag` is the *run*; what *triggers* a run is separate.
Today, recurring defragmentation is driven by creating `EtcdDefrag` objects from
outside (a `CronJob`, a GitOps cron). A companion `EtcdDefragPolicy` kind — a
cadence (`schedule`) and/or a condition (`when`) that stamps out `EtcdDefrag`
runs — is planned so the operator absorbs that scheduling itself; it is not
implemented yet.

## Why in the operator (not a bare CronJob)

A defrag briefly blocks the member it runs on, so it must be sequenced. The
operator already holds each cluster's TLS/auth material and endpoints, has the
whole-cluster view, and runs under leader election, so it can defragment members
**one at a time, followers before the leader, and only while the cluster is
healthy** — deferring rather than forcing on a degraded cluster. A detached
`CronJob` calling `etcdctl defrag` cannot make those guarantees.

## Usage

Defragment every member of a cluster once, now (`rule.all` — the explicit,
unconditional form):

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdDefrag
metadata:
  name: etcd-now
  namespace: team-a
spec:
  clusterRef:
    name: etcd
  rule:
    all: true
```

Guarded run (skips members that aren't worth defragmenting; with
`ttlSecondsAfterFinished` the controller GCs the record an hour after it
finishes — useful for objects a scheduler stamps out):

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdDefrag
spec:
  clusterRef:
    name: etcd
  ttlSecondsAfterFinished: 3600
  rule:
    freeSpaceAbove: 200Mi   # reclaimable (DbSize - DbSizeInUse) worth reclaiming
    quotaUsageAbove: 80%    # under quota pressure, take smaller wins too …
    minReclaim: 32Mi        # … but never a no-op defrag
```

Inspect progress and history:

```sh
kubectl get etcddefrag.etcd-operator.cozystack.io -n team-a
# NAME       CLUSTER   PHASE      DEFRAGMENTED   AGE
# etcd-now   etcd      Complete   2              5m

kubectl get etcddefrag.etcd-operator.cozystack.io etcd-now -n team-a \
  -o jsonpath='{.status.members}' | jq
```

## The rule

A defrag can only reclaim `DbSize - DbSizeInUse`, so that reclaimable amount is
the always-applied floor — this is what stops a full-but-unfragmented backend
(`DbSize ≈ DbSizeInUse` near the quota) from being defragmented over and over
for nothing.

| Field | Default | Meaning |
|---|---|---|
| `all` | `false` | Defragment every member unconditionally. Mutually exclusive with the fields below. |
| `freeSpaceAbove` | `200Mi` | Defragment a member whose reclaimable space exceeds this. |
| `quotaUsageAbove` | unset | When `DbSize` exceeds this fraction of the backend quota, lower the reclaim floor to `minReclaim` so small wins are taken under pressure. |
| `minReclaim` | `32Mi` | The floor for the quota arm (only meaningful with `quotaUsageAbove`, and must not exceed `freeSpaceAbove`) — never a no-op defrag. |

An **absent `rule`** is equivalent to an empty one: the default gate
(`freeSpaceAbove: 200Mi`). Unconditional defragmentation is something you ask
for explicitly with `rule.all: true`, never something you get by leaving a key
out.

> **Compaction is a prerequisite you own.** Defrag reclaims what compaction
> freed. A cluster with no auto-compaction (`spec.options.autoCompactionMode` /
> `autoCompactionRetention`) has `DbSizeInUse ≈ DbSize` and little to reclaim.
> Set auto-compaction if you rely on defrag to hold the backend down.

## Safety model

- **One member at a time, followers before the leader**, only while the whole
  cluster is healthy. A defrag due on a not-fully-healthy cluster is **deferred**
  — the object stays `Pending` with a condition explaining why — never forced,
  so quorum is never at risk.
- **Serialized per cluster:** at most one `EtcdDefrag` runs against a given
  `EtcdCluster` at a time; others wait in `Pending`.
- Health is judged from more than "the member answered": a member replies to a
  local status read while partitioned or alarmed, so the gate checks that every
  desired member is present and reachable, that they agree on a single non-zero
  leader, and that no member reports a blocking alarm. A `CORRUPT` alarm blocks;
  a `NOSPACE` alarm does **not** — a backend at its quota is exactly what a defrag
  relieves, so the run is admitted and the alarm is disarmed once space has been
  reclaimed. Raft lag is not yet part of the gate.

## Status

`status.phase` moves `Pending → Running → Complete | Failed`; a `Pending` run
waiting on cluster health carries a condition saying so. `status.members[]`
records, per member (keyed by name), the role at processing time, the outcome
(`Skipped` / `Defragmented` / `Failed`), the before/after `DbSize`, and the
bytes reclaimed — the run's full history, not a single rolled-up condition.

## Timeouts and retries

Following [`EtcdSnapshot`](concepts.md#snapshots--restore) — where the Job's
deadlines are controller constants and terminal phases are sticky — this needs
no `spec` knobs:

- **Per-member timeout** bounds each `Defragment` RPC (a stop-the-world call on a
  large backend), so one wedged member can't consume the whole run; on expiry
  that member is `Failed`.
- **An overall active-deadline** bounds `Running` + waiting-while-`Pending`
  together; on expiry the run is `Failed`. This also protects the per-cluster
  serialization slot — a run stuck waiting on an unhealthy cluster can't block
  the next one forever.
- **Retry within a run:** a deferred `Pending` re-checks cluster health each pass
  up to the deadline. A failed per-member `Defragment` RPC marks that member
  `Failed` immediately, and any failed member fails the run — a partial sweep that
  reclaimed space still disarms `NOSPACE` on the way out. (Per-member RPC retry is
  a possible follow-up, not shipped here.)
- **Retry across runs:** terminal phases (`Complete`/`Failed`) are sticky — an
  `EtcdDefrag` never re-runs itself. A retry is a *new* `EtcdDefrag`: the external
  scheduler's next tick for periodic use, or a re-create for a one-shot. Each
  attempt is a discrete, auditable object (GC'd via `ttlSecondsAfterFinished`)
  rather than hidden retry state.

## Relationship to capacity metrics

The capacity metrics and alert rules that tell you *when* a defrag is worth
running are tracked separately (see #357); `EtcdDefrag` records sizes in its own
`status` during a run rather than as continuously-scraped gauges.
