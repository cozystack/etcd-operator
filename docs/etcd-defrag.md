# Defragmentation (`EtcdDefrag`)

etcd never reclaims backend disk on its own: compaction frees pages logically,
but the file — and the space counted against `--quota-backend-bytes` — stays
allocated until a **defragment** returns it. `EtcdDefrag` is how you ask the
operator to do that, safely.

It is a one-shot, run-to-completion record, modeled on [`EtcdSnapshot`](concepts.md#snapshots--restore):
the operator drives it through `status.phase` and it never re-runs.

**Scheduling.** `EtcdDefrag` is the *run*; what *triggers* a run is separate.
For recurring defragmentation, [`EtcdDefragPolicy`](#recurring-runs-etcddefragpolicy)
stamps out `EtcdDefrag` objects on a cron schedule so the operator drives the
cadence itself; you can also create `EtcdDefrag` objects from outside (a
`CronJob`, a GitOps cron) if you prefer to own the schedule elsewhere.

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
  local status read while partitioned, alarmed (`NOSPACE`/`CORRUPT`), or behind
  in raft, so those are checked before acting.

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
- **Retry within a run:** a deferred `Pending` re-checks cluster health with
  backoff up to the deadline; a failed per-member RPC is retried a bounded number
  of times then marked `Failed` (a failing leader fails the run).
- **Retry across runs:** terminal phases (`Complete`/`Failed`) are sticky — an
  `EtcdDefrag` never re-runs itself. A retry is a *new* `EtcdDefrag`: an
  [`EtcdDefragPolicy`](#recurring-runs-etcddefragpolicy) tick for periodic use, or
  a re-create for a one-shot. Each attempt is a discrete, auditable object (GC'd
  via `ttlSecondsAfterFinished`) rather than hidden retry state.

## Recurring runs (`EtcdDefragPolicy`)

`EtcdDefragPolicy` schedules `EtcdDefrag` runs on a cron cadence. Each tick
stamps a new `EtcdDefrag` — owned by the policy (so it cascades on delete) — and
the run then follows all the safety rules above. The policy only *triggers*
runs; it never defragments directly.

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdDefragPolicy
metadata:
  name: nightly
  namespace: team-a
spec:
  clusterRef:
    name: etcd
  schedule: "0 3 * * *"        # standard five-field cron, evaluated in UTC
  concurrencyPolicy: Forbid    # skip a tick while a previous run is still active (default)
  ttlSecondsAfterFinished: 3600
  historyLimit: 3              # keep the last 3 finished runs
  rule:
    freeSpaceAbove: 200Mi
    quotaUsageAbove: 80%
    minReclaim: 32Mi
```

- **`schedule`** is a standard five-field cron expression in UTC. Prefix it with
  `CRON_TZ=<zone>` to use another zone.
- **`concurrencyPolicy`** is `Forbid` (default — a tick is skipped while a
  stamped run is still active) or `Allow` (stamp anyway; `EtcdDefrag`'s own
  per-cluster serialization queues it behind the active run).
- **`suspend: true`** pauses stamping without deleting the policy; missed ticks
  are not backfilled on resume.
- **`startingDeadlineSeconds`** skips a tick that is already older than the
  deadline (e.g. after the operator was down) instead of starting it late.
- **`historyLimit`** caps retained finished runs; `ttlSecondsAfterFinished` (per
  run) is the other cleanup path.
- **`rule`** / **`ttlSecondsAfterFinished`** are copied verbatim into each
  stamped `EtcdDefrag`.

`status.lastScheduleTime` anchors the next tick (so a tick is never acted on
twice), `status.lastSuccessfulTime` records the last `Complete`, and
`status.active` lists runs still in flight.

## Relationship to capacity metrics

The capacity metrics and alert rules that tell you *when* a defrag is worth
running are tracked separately (see #357); `EtcdDefrag` records sizes in its own
`status` during a run rather than as continuously-scraped gauges.
