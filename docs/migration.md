# Migration

Notes for migrating onto this operator (`etcd-operator.cozystack.io/v1alpha2`)
from the legacy aenix operator (`etcd.aenix.io/v1alpha1`), and for behavioural
changes that need an explicit migration step.

This document covers the **tool-driven migration from the legacy operator**
(`etcd-migrate`), the **`EtcdBackup` → `EtcdSnapshot` rename** (a pre-GA naming
change), the **`spec.options` map → typed fields** change, and the one change
that has a hard migration requirement: **etcd authentication credentials**.

## Tool-driven in-place migration (`etcd-migrate`)

`etcd-migrate` adopts running legacy clusters **in place**: the etcd pods and
their PVCs stay exactly as they are — only ownership, labels, member
annotations and CRs change, and the new operator takes over the live data
plane. No data is moved, no pod is restarted, and quorum is never touched.
Clients that connect by DNS name keep working; one Service changes shape
(ClusterIP → headless) and has consumer prerequisites — see
[Endpoint compatibility](#endpoint-compatibility) before you `--apply`.

Get it from the GitHub release — each release attaches
`etcd-migrate-<os>-<arch>` binaries (with a `cli-SHA256SUMS.txt`):

```sh
VERSION=v0.5.0; OS=$(uname -s | tr A-Z a-z); ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
curl -sSLo etcd-migrate "https://github.com/cozystack/etcd-operator/releases/download/$VERSION/etcd-migrate-$OS-$ARCH"
chmod +x etcd-migrate && ./etcd-migrate version
```

Or build from a checkout with `make etcd-migrate` (lands in `bin/etcd-migrate`).

### How adoption works

The adopted pods are made to look native through **durable identity stamped
as two reserved annotations on each adopted `EtcdMember`** — there is no
permanent, user-facing API knob for this, and the annotations **self-wipe** as
the cluster rolls:

- **`etcd-operator.cozystack.io/headless-service-name`**. Legacy StatefulSet
  pods carry an immutable `spec.subdomain` of `<cluster>-headless`, and the
  peer URLs persisted *inside etcd* use that DNS domain. The annotation makes
  every URL the operator constructs for that member (dial endpoints,
  `--initial-cluster`, replacement-pod DNS) match the adopted pod's actual
  identity — no special cases.
- **`etcd-operator.cozystack.io/data-dir-subpath`**. The legacy operator kept
  etcd's data under the `default.etcd/` subdirectory of the PVC; the annotation
  relocates `--data-dir` so a future replacement Pod resumes from the existing
  data dir instead of crashlooping with a fresh identity. The controller
  validates the value in code (single safe path component — no `/`, no `..`)
  and fails closed to the volume root on anything malformed.

The operator **never stamps these annotations on members it creates**. So
every member the operator rolls or replaces comes up *native* (cluster-name
DNS, data dir at the volume root); once the cluster has fully rolled, no member
carries either annotation and the cluster is indistinguishable from one created
natively — no permanent knob, nothing to deprecate later. `additionalMetadata`
cannot set keys under the `etcd-operator.cozystack.io/` reserved prefix, so a
user can neither forge these annotations nor break the self-wipe.

Per cluster, the tool:

1. **Inspects** the live etcd (read-only, over a port-forward with the legacy
   operator's client certificate): member list, cluster ID, auth status.
   Runs in dry-run too, so the printed plan shows the real IDs.
2. **Disables legacy auth** if enabled (the legacy NoPassword root can never
   match a credentials Secret; the new operator re-enables auth itself). This
   runs **before** the backup on purpose: the snapshot Job dials etcd
   anonymously, and etcd rejects the Maintenance Snapshot RPC while auth is on.
3. **Backs up** the cluster (see below) — before anything is mutated.
4. **Creates the new CRs with prefilled status**: the `EtcdCluster` gets
   `status.clusterID`/`clusterToken`/`observed` (so the operator's bootstrap
   branch never fires against a cluster that already exists), and one
   `EtcdMember` per pod — named exactly like the pod, carrying the reserved
   adoption annotations above — gets its `status.memberID` and `isVoter=true`.
5. **Owner-references the legacy headless Service to the adopted members,
   then dismantles the legacy control plane** — in that order. The legacy
   headless Service (`<cluster>-headless`) has its `ownerReferences` replaced
   with one non-controller entry per adopted `EtcdMember`, so Kubernetes GC
   removes it exactly when the last adopted member rolls away (new members
   aren't owners, so they never keep it alive). Only then are the legacy
   `EtcdCluster` and its StatefulSet deleted with **Orphan** propagation (pods
   survive) and the cluster-state ConfigMap + legacy PDB removed. Doing the
   owner-ref rewrite first avoids a window where the Service is sole-owned by
   a now-deleted object and gets reaped prematurely.
6. **Re-owns the data plane**: each pod and PVC gets the operator's labels
   and a controller owner reference to its `EtcdMember` (only after the
   StatefulSet is gone, so its controller can't re-adopt the pods).
7. **Cuts over the client Service**: the legacy client Service is named after
   the cluster (`<cluster>`), which collides with the operator's *native*
   headless Service of the same name. The tool deletes the legacy client
   Service and immediately recreates it as the native headless Service (owned
   by the new `EtcdCluster`), so the DNS name keeps resolving with the minimum
   possible gap rather than waiting for the operator's first reconcile. See
   [Endpoint compatibility](#endpoint-compatibility) for what this means for
   consumers.

Every step is idempotent — re-running the tool completes a partially-applied
adoption.

### Prerequisites

1. **Scale both operators to zero.** The legacy etcd Pods keep running — only
   the controllers must be quiet. The legacy (v1alpha1) controller is
   `etcd-operator-controller-manager`; this operator's Helm release is named
   `etcd-operator`:

   ```sh
   kubectl -n etcd-operator-system scale deploy etcd-operator-controller-manager --replicas=0  # legacy
   kubectl -n etcd-operator-system scale deploy etcd-operator --replicas=0                     # new
   ```

   The tool verifies this for both Deployments before doing anything
   (`--legacy-controller` / `--new-controller` override the coordinates,
   `--skip-controller-check` bypasses the gate).
2. The new CRDs (`etcd-operator.cozystack.io/v1alpha2`) must be installed —
   they ship with the operator chart (`make deploy IMG=...`, or `helm install`;
   see [installation](installation.md)).
3. A kubeconfig that can list/delete the legacy CRs cluster-wide, create the
   new ones, and patch pods/PVCs/Services.
4. **All etcd pods Ready.** Adoption refuses clusters with missing members,
   learners, or unreachable etcd.

### Workflow: dry-run first

```sh
# Dry-run (the default): inspects each live cluster and prints the planned
# v1alpha2 manifests, the adoption steps, and warnings for legacy settings
# that do not carry over.
bin/etcd-migrate

# Execute the adoption (backup destination required — see below).
bin/etcd-migrate --apply \
  --backup-s3-endpoint=https://s3.example.com \
  --backup-s3-bucket=etcd-migration \
  --backup-s3-credentials-secret=s3-creds   # needed in EVERY migrated namespace
```

What gets migrated:

| Legacy (`etcd.aenix.io/v1alpha1`) | New (`etcd-operator.cozystack.io/v1alpha2`) |
|---|---|
| `EtcdCluster` | `EtcdCluster` + `EtcdMember`s **adopting the running pods in place** |
| `EtcdBackup` | `EtcdSnapshot` (created; legacy CR deleted) |
| `EtcdBackupSchedule` | a `CronJob` manifest creating `EtcdSnapshot`s — **printed only**, never applied; the legacy CR is left for you to delete |

Every legacy knob with no v1alpha2 equivalent (`spec.options` keys beyond the
[four typed ones](#specoptions-free-form-map--typed-fields), service/PDB
templates, podTemplate overrides beyond affinity/topology-spread/resources/
metadata) is reported as a warning — review them before `--apply`. Hard
blockers (`emptyDir` storage — nothing to adopt, an unparsable etcd image tag
without `--version`, `enableAuth` without server TLS, a non-integer
`quota-backend-bytes`/`snapshot-count`, a failed inspection) skip that
cluster and exit non-zero.

TLS caveat: the legacy API kept CAs in separate Secrets
(`serverTrustedCASecret`, `peerTrustedCASecret`); the new operator reads
`ca.crt` from the server/peer Secret itself. The tool warns per cluster —
merge the CA into the referenced Secret **before** starting the new operator
(with cert-manager-issued secrets, `ca.crt` is typically already in place).

### Peer auto-TLS (legacy `--peer-auto-tls`)

The legacy operator ran etcd with `--peer-auto-tls` **unconditionally** unless
you supplied a BYO peer Secret. Under that flag each member generates its own
self-signed peer certificate and there is **no shared CA**: peer traffic is
encrypted but **not authenticated** — any TLS-capable workload that can reach a
member's `:2380` can peer with the cluster or impersonate a member. This is a
weaker posture than the real mutual-TLS the native operator offers via
`spec.tls.peer.secretRef` / `spec.tls.peer.certManager`, and it is **not** the
same thing as the [SAN-coverage caveat](#endpoint-compatibility) above (that is
about explicit mTLS certs needing both DNS domains during rollover — a different
scenario; don't conflate them).

The tool **detects this and carries it forward**, because it has to: with no CA
in existence there is nothing to mint real mTLS certs from, so a replacement or
scaled-up member running strict mTLS (or plaintext peer) could never rejoin the
still-auto-tls members. Carry-forward keeps replacement/scale working.

It is **not** exposed as a typed spec field — an unauthenticated peer plane must
not be a discoverable, first-class option for new clusters. Instead the tool
stamps a reserved cluster annotation:

```yaml
metadata:
  annotations:
    etcd-operator.cozystack.io/peer-auto-tls: "true"
```

The operator reads it and propagates `--peer-auto-tls` to every member it builds
for that cluster. It is superseded by an explicit `spec.tls.peer.secretRef` /
`certManager` (real mTLS always wins). The dry-run plan flags the adoption with a
loud `⚠️  SECURITY:` line, and the post-`--apply` summary re-surfaces it — you
cannot complete a migration without being told you adopted an unauthenticated
peer plane.

**The only off-ramp to real mTLS is delete-and-recreate** (`spec.tls` is
immutable), or a careful manual rolling restart onto BYO/cert-manager peer
certs. Because strict-mTLS and auto-tls members **cannot peer with each other**,
either route has a brief no-quorum window at the cutover — plan it like any
peer-cert rotation.

### The safety backup

Adoption rewires ownership of live storage, so the tool snapshots every
cluster to the `--backup-s3-*`/`--backup-pvc-claim` destination **before any
ownership/data-plane mutation** — the only step that precedes it is the
auth-disable above, which the snapshot Job's anonymous dial depends on (a
one-off Job running the operator image's snapshot agent —
`--agent-image` overrides; by default the image is read from the new
controller Deployment's spec, which works at replicas=0). Nothing is restored
from the artifact — the data never moves — it exists purely for disaster
recovery. A failed backup excludes that cluster from the apply. Skipping the
backup requires an explicit `--skip-backup`.

### Auth during migration

The legacy operator provisioned the etcd `root` user with **NoPassword**
(certificate-only identity). The new operator requires BYO root credentials
(see [Authentication](#authentication-root-credentials-are-byo-and-required)
below). The tool bridges this: it generates a `kubernetes.io/basic-auth`
Secret (`<cluster>-root-credentials`, random password) per auth-enabled
cluster — or references the one you name via `--auth-secret` — runs
`auth disable` on the live etcd (authenticating with the legacy operator's
client certificate), and lets the new operator re-enable auth with the
Secret's password once it takes over. Mind the window: auth is off from that
moment until the new operator latches `status.authEnabled`. Update consumers
(e.g. a Kamaji `DataStore` `basicAuth`) to point at the Secret.

### Endpoint compatibility

The etcd cluster ID is preserved (it's an adoption, not a restore) and the pods
keep their IPs, but the **client Service changes shape** because of a naming
collision you must plan for.

The legacy operator names its **client** Service `<cluster>` and its headless
Service `<cluster>-headless`. The native operator names its **headless** Service
`<cluster>` and its client Service `<cluster>-client`. So the native headless
Service collides with the legacy client Service on the name `<cluster>`. Since
a Service's `clusterIP` is immutable, the collision cannot be reconciled in
place — the tool deletes the legacy client Service and recreates `<cluster>` as
a **headless** Service (step 7 above).

What this means for consumers connecting to `<cluster>.<ns>.svc:2379`:

- **The DNS name keeps resolving** and the server-cert SAN still covers it, so
  clients that connect **by DNS name** (a normal etcd client with retries — a
  Kamaji `DataStore`, for example) keep working across the cutover. The
  recreate happens back-to-back, so the no-resolution window is minimal.
- **The ClusterIP VIP disappears.** `<cluster>` is now headless (it returns
  pod A-records directly instead of a single virtual IP), and it publishes
  not-ready addresses. Any consumer that **depends on the ClusterIP/VIP
  semantics** — a cached service IP, a NetworkPolicy keyed on the VIP, a
  customized legacy client Service (`LoadBalancer`/`NodePort`/external-dns
  annotations) — will break, and the customizations are lost.

> **Prerequisite — repoint VIP-dependent consumers before cutover.** If any
> consumer relies on ClusterIP/VIP behaviour rather than plain DNS, point it at
> the operator's native **`<cluster>-client`** Service (a regular ClusterIP
> Service the operator creates) before you run `--apply`. DNS-name consumers
> need no change.

The legacy headless Service (`<cluster>-headless`) is **not** managed by the
operator; it is owner-referenced to the adopted members and is garbage-collected
automatically once the last adopted member is replaced (see step 5). The adopted
pods remain reachable under it for their whole lifetime (their immutable
`spec.subdomain` points at it); rolled/replacement members come up under the
native `<cluster>` headless Service instead.

> **Prerequisite — externally-issued certs must carry both DNS domains during
> the mixed window.** Server/peer certs here are external (e.g. Cozystack
> cert-manager); the operator does not synthesize them. The operator's SAN
> contract is a wildcard pinned to the Service name (`*.<svc>.<ns>.svc`).
> During rollover, adopted members resolve under `<cluster>-headless` and
> rolled members under `<cluster>`, so the cert the pods mount must carry
> **both** `*.<cluster>-headless.<ns>.svc` and `*.<cluster>.<ns>.svc` (plus the
> `.<cluster-domain>` FQDN forms) for the duration. Drop the legacy SAN once
> rollover completes. Coordinate this with whoever issues the certs before
> starting the new operator.

### Final cleanup

After `--apply` succeeds, **scale the new operator up** — it takes over the
adopted clusters without touching the pods:

```sh
kubectl -n etcd-operator-system scale deploy etcd-operator --replicas=1
```

The tool deletes the migrated legacy **CRs** but never the **CRDs**. Once no
`etcd.aenix.io` CRs remain (remember `EtcdBackupSchedule`s are left in
place), remove them:

```sh
kubectl delete crd etcdclusters.etcd.aenix.io etcdbackups.etcd.aenix.io etcdbackupschedules.etcd.aenix.io
```

## Snapshot CRD renamed: `EtcdBackup` → `EtcdSnapshot`

The one-shot snapshot CRD was renamed from `EtcdBackup` to `EtcdSnapshot` (and
its status field `status.snapshot` to `status.artifact`) to match upstream
etcd's terminology (`etcdctl snapshot save` / `restore`). Nothing has shipped
under the old name, so there is no stored-object migration — but if you applied
an `EtcdBackup` from a pre-rename build, recreate it under the new kind:

```diff
 apiVersion: etcd-operator.cozystack.io/v1alpha2
-kind: EtcdBackup
+kind: EtcdSnapshot
 metadata:
   name: my-etcd-snapshot
 spec:
   clusterRef:
     name: my-etcd
   destination:
     s3: { ... }
```

The spec is otherwise unchanged (`spec.clusterRef`, `spec.destination`). The
restore path (`spec.bootstrap.restore.source`) is unaffected — `restore` keeps
its name.

## `spec.options`: free-form map → typed fields

The legacy operator's `spec.options` was a free-form `map[string]string` passed
through as etcd flags. This operator keeps the `spec.options` path but types it
as a closed struct covering exactly the keys Cozystack's etcd package set —
arbitrary flag injection is no longer possible (see
[concepts: etcd tuning options](concepts.md#etcd-tuning-options) for why).

The key mapping, using Cozystack's actual legacy values:

```diff
 spec:
   options:
-    quota-backend-bytes: "10200547328"
-    auto-compaction-mode: "periodic"
-    auto-compaction-retention: "5m"
-    snapshot-count: "10000"
+    quotaBackendBytes: 10200547328
+    autoCompactionMode: periodic
+    autoCompactionRetention: "5m"
+    snapshotCount: 10000
```

Note the value types: `quotaBackendBytes` and `snapshotCount` are integers, not
quoted strings. `etcd-migrate` performs this mapping automatically. Any other
key the legacy map accepted has no typed equivalent — the tool drops it with a
warning; if you relied on one, file an issue: the flag gets a typed field, not
a pass-through.

## Authentication: root credentials are BYO and required

In the legacy operator, enabling auth provisioned a fixed `root:root` user
implicitly. In this operator, `spec.auth.enabled` requires you to
**bring your own** root credentials via a referenced Secret — nothing is
hardcoded. Concretely, when `spec.auth.enabled: true`:

- `spec.tls.client` **must** be set (CEL-enforced) — credentials never cross a
  plaintext wire.
- `spec.auth.rootCredentialsSecretRef` **must** be set (CEL-enforced). It
  names a `kubernetes.io/basic-auth` Secret in the cluster's namespace; the
  operator reads its `password` key. The etcd user is always `root` (etcd
  requires a user named `root` to enable auth), so the Secret's `username`
  should be `root`.

See [concepts: Authentication](concepts.md#authentication) for the full
behaviour.

### Migration steps

1. **Create the credentials Secret** in the cluster's namespace:

   ```sh
   kubectl create secret generic my-etcd-root \
     --type=kubernetes.io/basic-auth \
     --from-literal=username=root \
     --from-literal=password='<choose-a-password>'
   ```

2. **Reference it on the cluster** alongside client TLS:

   ```yaml
   spec:
     tls:
       client:
         serverSecretRef: { name: my-server-tls }
     auth:
       enabled: true
       rootCredentialsSecretRef: { name: my-etcd-root }
   ```

3. **Update consumers.** Any client that talks to the cluster must now
   authenticate as `root`. For a Kamaji `DataStore`, point its `basicAuth` at
   the same Secret (Cozystack's `packages/extra/etcd` chart needs this added —
   it currently sets no `basicAuth`). Without this, tenant control planes lose
   access the moment auth turns on.

### Gotchas — read before flipping auth on a cluster that has data

- **Ordering.** The apiserver accepts the CR as soon as
  `rootCredentialsSecretRef` is *set* (CEL checks the field, not the Secret's
  existence). The operator then enables auth only once the cluster has converged
  to a healthy quorum **and** the Secret exists with a non-empty `password` key;
  until then it requeues without enabling. So you can create the Secret before
  or just after the cluster, but `status.authEnabled` will not latch until the
  Secret is present.

- **Adopting an etcd that ALREADY has auth enabled (the critical one).** The
  operator's enable step is idempotent: if etcd reports auth already on, it does
  **not** reset the root password — it just latches `status.authEnabled=true` and
  from then on dials as `root` with the password **from your Secret**. If that
  password does not match the one already stored in etcd, every operator dial
  (member discovery, scale, removal) fails and reconciliation stalls.

  → When migrating data that already had a root password, the Secret's
  `password` **must equal the existing one**. (Migrating from the legacy
  operator's implicit `root:root`? Put `password: root` in the Secret to keep
  working, then rotate later by recreating — see below.)

- **No in-place rotation.** `spec.auth` (including the Secret *reference*) is
  immutable post-create, and the operator reads the password fresh on every
  dial. Changing the Secret's *contents* after auth is enabled desyncs the
  operator from etcd. Rotating the root password is therefore a recreate, not an
  edit — a native `UserChangePassword` reconcile is a possible future
  improvement.
