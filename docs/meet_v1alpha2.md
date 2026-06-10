# Cozystack introduces a reworked etcd-operator with a new API

The [etcd-operator](https://github.com/cozystack/etcd-operator) project, which develops an operator for deploying and maintaining [etcd](https://etcd.io) clusters on Kubernetes, has been donated to the [Cozystack](https://cozystack.io) project. Alongside the donation, a from-scratch implementation of the operator has been published under a new API version — `etcd-operator.cozystack.io/v1alpha2`, superseding the previous `etcd.aenix.io/v1alpha1`. Instead of managing members through a StatefulSet, the new implementation directly drives etcd's native Membership API (the MemberAdd, MemberPromote and MemberRemove operations), giving the operator full control over cluster membership. The new implementation was written by [Timofei Larkin](https://github.com/lllamnyp), one of the maintainers of the previous codebase, which is preserved in the [v1alpha1](https://github.com/cozystack/etcd-operator/tree/v1alpha1) branch. The project is written in Go and distributed under the Apache 2.0 license. Cozystack is developed under the umbrella of the CNCF as a Sandbox project.

The project was started by Ænix, which assembled an initiative group from the Russian-speaking Kubernetes community to build it. After the base implementation was completed, an attempt was made to donate the project to the CNCF. Prompted by this initiative, the etcd project concluded that an official operator was needed and formed its own working group, which, after evaluating existing implementations, chose to develop a codebase from scratch — this is how [etcd-io/etcd-operator](https://github.com/etcd-io/etcd-operator) came to be. Feature-wise the official operator has not yet caught up with the aenix etcd-operator, which is already used in production by the community and by projects such as Cozystack and [Kamaji](https://github.com/clastix/kamaji), so the project has continued its own independent line of development (a comparison with the official operator is given at the end of this article).

The operator manages etcd clusters through two resources: EtcdCluster describes the desired state of a cluster (replica count, etcd version, storage parameters, TLS, authentication, etcd tuning), while EtcdMember is created by the operator itself for every cluster member and owns its Pod and PVC. Unlike typical solutions, the operator does not use a StatefulSet — each member's Pod and PVC are reconciled independently, and cluster membership changes go through etcd's Membership API: new members join as learners (MemberAdd) and are later promoted to voting members (MemberPromote), removal is performed with a graceful exit from quorum (MemberRemove), and pausing a cluster preserves member identity. The rationale behind this architecture is described in [concepts.md](concepts.md).

Key features:

- cluster bootstrap and scaling in both directions one member at a time: learner-mode joins, graceful removal with exit from quorum;
- pausing a cluster without losing data (`spec.replicas: 0`) and resuming it with the same cluster and member identifiers;
- data storage in PVCs (default) or in tmpfs — for data that can be reconstructed; memory-backed members are automatically recreated when their Pod is lost;
- independent TLS configuration for client and peer connections: bring your own Secrets or let the operator issue and automatically renew certificates via cert-manager;
- authentication with a single root user whose credentials are supplied through a Secret;
- snapshots to S3 or a PVC via the EtcdSnapshot resource and cluster restore from a snapshot at initial bootstrap;
- an automatically created PodDisruptionBudget that prevents drain operations from breaking quorum;
- spec validation by the apiserver (CEL expressions in the CRD) without webhooks or a cert-manager dependency;
- the `/scale` subresource, which makes `kubectl scale` and the VerticalPodAutoscaler work, a metrics port on 2381, pass-through `affinity` and `topologySpreadConstraints`;
- the kubectl-etcd plugin for day-2 operations performed after the cluster is deployed.

Compared with the old `etcd.aenix.io/v1alpha1` implementation, the following changes were made:

- the API group changed from `etcd.aenix.io` to `etcd-operator.cozystack.io`;
- separate per-member EtcdMember resources are used instead of a StatefulSet;
- the free-form `spec.options` map was replaced with a typed set of parameters (`quota-backend-bytes`, auto-compaction mode and retention, `snapshot-count`) — the free-form map allowed passing flags that conflicted with the operator's logic;
- the EtcdBackup resource was renamed to EtcdSnapshot with its semantics preserved;
- validation moved from a webhook to CEL rules in the CRD;
- the cluster Service was switched to headless mode, which is required for stable per-member DNS names.

Migration is performed in place with the etcd-migrate tool: a running cluster of the old operator is adopted without moving data, restarting Pods or losing quorum — only object ownership, labels and annotations are changed, after which the new operator takes over. Clients that reach the cluster by DNS name keep working unchanged. The procedure is described in [migration.md](migration.md).

The implementation covers most items of the [roadmap](https://github.com/etcd-io/etcd-operator/blob/main/docs/roadmap.md) of the official [etcd-operator](https://github.com/etcd-io/etcd-operator) developed by the etcd project. Status by roadmap item:

1. Create a new etcd cluster, e.g 3 or 5 members cluster of a specified etcd version — implemented.
2. Understand health of a cluster — implemented.
3. Enabling TLS communication, including cert renewal — implemented.
4. Upgrade across patches or one minor version — partially implemented: `spec.version` applies only to newly created members.
5. Scale in and out, e.g 1 -> 3 -> 5 members and vice versa — implemented.
6. Support customizing etcd options (via flags or env vars) — implemented, as a typed closed set of parameters.
7. Recover a single failed cluster member (still have quorum) — partially implemented: members with a broken PVC are not replaced automatically yet.
8. Recover from multiple failed cluster members (quorum loss) — not implemented, work is planned.
9. Create on-demand backup of a cluster — implemented.
10. Create periodic backup of a cluster — deliberately out of scope: recurring snapshots are expected to be driven by a standard CronJob.

Beyond that roadmap, `v1alpha2` also ships capabilities the official plan does not enumerate, driven by the Cozystack and Kamaji multi-tenant use case:

- scale to zero (pause/resume) preserving cluster and member identity;
- memory-backed (tmpfs) storage with operator-driven member replacement;
- apiserver-side CEL validation — no webhook, no certificate dependency;
- an auto-emitted PodDisruptionBudget scoped to voting members;
- the `/scale` subresource with a populated `status.selector`, so `kubectl scale` and a `VerticalPodAutoscaler.targetRef` work directly;
- pass-through scheduling (`affinity`, `topologySpreadConstraints`) and merged `additionalMetadata` across every owned object;
- an in-place migration tool from the legacy operator;
- the kubectl-etcd plugin for day-2 operations.
