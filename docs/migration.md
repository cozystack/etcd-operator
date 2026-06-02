# Migration

Notes for migrating onto this operator (`etcd-operator.cozystack.io/v1alpha2`)
from the legacy aenix operator (`etcd.aenix.io/v1alpha1`), and for behavioural
changes that need an explicit migration step.

This document grows as more legacy features are ported. Right now it covers the
one change that has a hard migration requirement: **etcd authentication
credentials**.

> **TODO — full legacy-operator migration.** The end-to-end story for moving an
> existing `etcd.aenix.io/v1alpha1` cluster onto `etcd-operator.cozystack.io/v1alpha2`
> (CRD shape, data-dir adoption vs backup/restore, member-ID continuity) is not
> written yet — the two operators manage members differently (the new one uses
> per-member `EtcdMember` CRs + Pods, not a single StatefulSet), so this is not a
> drop-in CRD swap. Fill this in as the migration path is validated.

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
