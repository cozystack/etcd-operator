# Installation

How to deploy the operator into a cluster. Assumes you have admin on the target cluster (the operator installs cluster-scoped CRDs and ClusterRoles) and a registry you can push to.

For the operator's runtime behaviour see [concepts](concepts.md); for day-2 operations see [operations](operations.md).

## Prerequisites

| Requirement | Note |
|---|---|
| Kubernetes | 1.29+ recommended: CEL CRD validation went GA in 1.29 and the `quantity()` CEL extension (used by two of the operator's validation rules) was added in 1.28. 1.28 *may* work in practice because the CEL gate was beta-on-by-default from 1.25, but is not covered by CI. |
| Default `StorageClass` | Each per-member PVC uses the namespace's default `StorageClass`. Override per-cluster via `spec.storage.storageClassName` (a string naming a specific `StorageClass`, or `""` to disable dynamic provisioning entirely). Immutable post-create — Kubernetes PVCs cannot have their StorageClass swapped in place. |
| Go (build-from-source only) | 1.25+, matches `go.mod`'s `toolchain` directive. |
| Docker / buildx (build-from-source only) | For producing the operator image. The Dockerfile uses `golang:1.25.10` for the builder and `gcr.io/distroless/static:nonroot` for runtime. |

Workload-side: every etcd Pod runs as UID 65532 with `runAsNonRoot=true`, `allowPrivilegeEscalation=false`, all capabilities dropped, and `seccompProfile=RuntimeDefault`. The Pods comply with the `restricted` PodSecurity profile. If your cluster enforces a stricter policy, see `controllers/etcdmember_controller.go`'s `buildPod` for the exact security context the operator emits and adjust accordingly.

## Install from a release

Tagged releases publish a signed multi-arch operator image to GHCR and attach
ready-to-apply install manifests to the GitHub release — no checkout, no build,
no registry of your own. This is the recommended path for consuming a release.

```sh
# Pick a released version (see https://github.com/cozystack/etcd-operator/releases).
VERSION=v0.5.0

# Everything (CRDs + namespace + RBAC + controller Deployment + Service):
kubectl apply -f https://github.com/cozystack/etcd-operator/releases/download/$VERSION/etcd-operator.yaml
```

The manifest already points the manager (and its `OPERATOR_IMAGE`, used for
snapshot/restore Pods) at `ghcr.io/cozystack/etcd-operator:$VERSION` — the same
tag whose image the release published, so there is nothing to substitute.

If you split CRDs from the rest (e.g. CRDs are applied by a separate
cluster-admin step, or server-side-applied to dodge the annotation size limit):

```sh
kubectl apply --server-side -f https://github.com/cozystack/etcd-operator/releases/download/$VERSION/etcd-operator.crds.yaml
kubectl apply             -f https://github.com/cozystack/etcd-operator/releases/download/$VERSION/etcd-operator.non-crds.yaml
```

The image is cosign-signed (keyless). To verify before deploying:

```sh
cosign verify ghcr.io/cozystack/etcd-operator:$VERSION \
  --certificate-identity-regexp 'https://github.com/cozystack/etcd-operator/.github/workflows/.+' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

To pull a prebuilt image without the release manifests (e.g. to feed your own
overlay), the image ref is `ghcr.io/cozystack/etcd-operator:<tag>`.

## Install with Helm

Helm is the primary install path: the chart is the single source of truth for
the CRDs, RBAC, and the manager Deployment (the release manifests below are just
`helm template` of this same chart). Tagged releases publish it as an OCI Helm
chart to GHCR (`ghcr.io/cozystack/charts/etcd-operator`), versioned from the
same tag — the chart version is the tag without the leading `v`, and
`appVersion` keeps the `v`. The CRDs are generated straight into the chart and
templated into the release.

```sh
# Chart version == release tag without the leading 'v'
# (see https://github.com/cozystack/etcd-operator/releases).
VERSION=0.5.0

helm install etcd-operator oci://ghcr.io/cozystack/charts/etcd-operator \
  --version "$VERSION" \
  --namespace etcd-operator-system --create-namespace
```

By default the chart pulls `ghcr.io/cozystack/etcd-operator:<appVersion>` — the
image that same release published — so a stock install has nothing to
substitute. The chart wires that ref into **both** the manager's `image:` and
its `OPERATOR_IMAGE` env var (the image launched for snapshot/restore Pods); the
two must be identical, and the chart keeps them equal for you. Override the
image via `image.repository` / `image.tag` and both follow.

The CRDs are **templated** into the release (not in Helm's install-only `crds/`
directory), so `helm upgrade` keeps them current with the chart — no separate
CRD-apply step on upgrade. They carry `helm.sh/resource-policy: keep`, so `helm
uninstall` leaves the CRDs (and therefore your `EtcdCluster`s and their data) in
place; deleting the CRDs is a deliberate, manual step. Set `crds.enabled=false`
to manage CRDs out-of-band, or `crds.keep=false` to let uninstall remove them.

Common values (`--set key=value`, or a `-f my-values.yaml`):

| Value | Default | Purpose |
|---|---|---|
| `image.repository` | `ghcr.io/cozystack/etcd-operator` | Operator image repo. Override to mirror or fork. |
| `image.tag` | chart `appVersion` | Operator image tag; also becomes `OPERATOR_IMAGE` (see above). |
| `replicaCount` | `1` | Operator replicas (leader election picks the active one). |
| `kubeRbacProxy.enabled` | `true` | Front `/metrics` with the kube-rbac-proxy SubjectAccessReview sidecar. Set `false` to bind metrics on `:8080` directly with no proxy. |
| `metrics.serviceMonitor.enabled` | `false` | Create a prometheus-operator `ServiceMonitor` for the metrics endpoint (needs the `monitoring.coreos.com` CRDs and `kubeRbacProxy.enabled`). |
| `crds.enabled` / `crds.keep` | `true` / `true` | Render the CRDs with the release / annotate them so uninstall keeps them. |
| `manager.resources` | 10m/64Mi → 500m/128Mi | Manager container requests/limits. |
| `imagePullSecrets` | `[]` | Pull secrets for the **operator's own** image (private registry mirror). |
| `etcdImage.repository` | `quay.io/coreos/etcd` | Operator-wide default **etcd** image repo for member Pods (always wired into `ETCD_IMAGE_REPOSITORY`). Repoint at an air-gapped mirror once; an `EtcdCluster`'s `spec.image` overrides it per cluster. |

See `charts/etcd-operator/values.yaml` for the complete, annotated list. Verify
the install:

```sh
kubectl -n etcd-operator-system get deploy
```

With release name `etcd-operator` the Deployment is named `etcd-operator`. The
release manifests (the kubectl-apply path above) render from this same chart, so
they produce the same name. The Deployment carries the label
`control-plane=controller-manager`, a name-agnostic handle for scripts.

## Build from source

The repo's Makefile drives a complete install via Helm. From a checkout (needs
`helm` v3.16+ on PATH):

```sh
# 1. Build the operator image (or skip to a prebuilt registry tag).
make docker-build docker-push IMG=<your-registry>/etcd-operator:<tag>

# 2. Install/upgrade the operator (CRDs + RBAC + manager) with Helm.
#    `make deploy` runs `helm upgrade --install` and wires image == OPERATOR_IMAGE.
make deploy IMG=<your-registry>/etcd-operator:<tag>
```

The cluster must be able to pull from `<your-registry>`. For local clusters (`kind` / `minikube` / `k3d`), either sideload the image (`kind load docker-image ...`) or push to an ephemeral registry the cluster can reach (e.g. `ttl.sh/<random>:1h`); otherwise the operator Deployment goes `ImagePullBackOff` with no clear hint from the operator side.

`make deploy` installs the release `etcd-operator` into the `etcd-operator-system`
namespace (override with `HELM_RELEASE=` / `NAMESPACE=`). The Deployment is named
after the release. Verify:

```sh
kubectl get pod -n etcd-operator-system
kubectl -n etcd-operator-system logs deploy/etcd-operator -c manager --tail=20
```

You should see the manager start lines and an empty work-queue (no `EtcdCluster` resources yet). Tear down with `make undeploy` (see [Teardown](#teardown)).

## Rendering manifests (GitOps / no in-cluster Helm)

For GitOps flows that apply plain YAML, render the chart with `helm template`
instead of installing it — this is exactly what `make build-dist-manifests` (and
the release pipeline) does to produce the release's `etcd-operator.yaml`:

```sh
helm template etcd-operator charts/etcd-operator \
  --namespace etcd-operator-system \
  --set image.repository=<your-image-repo> --set image.tag=<tag> \
  --set namespace.create=true | kubectl apply --server-side -f -
```

`--server-side` avoids the client-side last-applied-config annotation size limit (the consolidated manifest embeds the full CRD schemas). `namespace.create=true` emits the Namespace so the output is self-contained.

> **The chart keeps `image:` and `OPERATOR_IMAGE` equal for you.** `OPERATOR_IMAGE` is the image the operator launches for snapshot Jobs and restore init containers; it must match the manager image. The chart renders both from `image.repository`/`image.tag` (the `etcd-operator.image` helper in `_helpers.tpl`), so setting the image once covers both. If you hand-craft manifests and leave `OPERATOR_IMAGE` at the placeholder `controller:latest`, the operator **refuses to start** and exits with a clear error (rather than letting snapshot/restore Pods `ImagePullBackOff` later).

## Create your first cluster

```sh
cat <<'EOF' | kubectl apply -f -
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: my-etcd
  namespace: default
spec:
  replicas: 3
  version: 3.6.11
  storage:
    size: 1Gi
EOF
```

Watch it form:

```sh
kubectl get etcdcluster.etcd-operator.cozystack.io my-etcd -w
```

The operator bootstraps a single seed first, latches `clusterID`, then adds the remaining members one at a time as learners (with promotion). A 3-member cluster typically reaches `READY=3` in well under a minute on a healthy cluster.

To open a shell to one of the members:

```sh
POD=$(kubectl get pod -l etcd-operator.cozystack.io/cluster=my-etcd \
  -o jsonpath='{.items[0].metadata.name}')
kubectl exec -it "$POD" -- etcdctl --endpoints=http://localhost:2379 \
  member list -w table
```

Don't hard-code Pod names — they carry a random suffix from `GenerateName` (e.g. `my-etcd-7xq2k`). The label selector is the stable handle.

### Memory-backed variant

For reconstructable workloads (e.g. a Kubernetes-in-Kubernetes apiserver whose state is GitOps-managed) you can opt the cluster onto a tmpfs `emptyDir` instead of a PVC:

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: my-mem-etcd
  namespace: default
spec:
  replicas: 3
  version: 3.6.11
  storage:
    size: 256Mi          # tmpfs SizeLimit per member
    medium: Memory
```

This trades durability for speed: a Pod that loses its tmpfs (eviction, node failure) loses its data and the member is automatically replaced via `MemberRemove` + scale-up. **Don't use it as a general-purpose etcd backend** — see [docs/concepts.md](concepts.md#storage) and [docs/operations.md](operations.md#memory-backed-clusters) for the full trade-off. For production, set `spec.affinity` (pod anti-affinity) and `spec.resources.limits.memory` explicitly — neither is defaulted (tracked in [#16](https://github.com/lllamnyp/etcd-operator/issues/16)); see the [production checklist](operations.md#what-you-should-configure-before-going-to-production). The apiserver rejects `replicas: 0` on memory clusters via the [CEL validation rules](concepts.md#apiserver-enforced-validation), and every cluster gets an auto-emitted [PodDisruptionBudget](concepts.md#poddisruptionbudget).

### TLS-enabled variant

You can opt the client API (2379), the peer API (2380), or both onto TLS. Two sources per subtree, mutually exclusive:

- **BYO Secrets** — you create `kubernetes.io/tls`-shaped Secrets out-of-band (e.g. via your own cert pipeline) and reference them via `spec.tls.client.serverSecretRef` / `operatorClientSecretRef` and `spec.tls.peer.secretRef`. See [operations: TLS-enabled clusters](operations.md#tls-enabled-clusters) for Secret-creation commands.
- **cert-manager** — point at an `Issuer` or `ClusterIssuer` via `spec.tls.client.certManager.{serverIssuerRef,operatorClientIssuerRef}` and `spec.tls.peer.certManager.issuerRef`. The operator emits `cert-manager.io/v1 Certificate` resources, cert-manager produces the Secrets, the rest of the wiring is identical.

See [concepts: TLS](concepts.md#tls) for the trade-offs (EKU `clientAuth` requirement on the server cert, CA-bundle topology, etc.) and [concepts: cert-manager-driven TLS](concepts.md#cert-manager-driven-tls) for the operator-emitted-Certificate path.

#### Prerequisites for cert-manager mode

- cert-manager (any v1.x release) installed on the cluster *before* the operator starts. The operator probes the discovery API for `cert-manager.io/v1` at startup; if absent, clusters with `certManager` set are parked at `Available=False / CertManagerNotInstalled` and the operator never touches the GVK. Recovery is install-cert-manager + operator restart.
- An `Issuer` (namespaced, in the EtcdCluster's namespace) or `ClusterIssuer` for each role. Most commonly a single Issuer per plane signs both the server and operator-client certs.
- The operator's cluster DNS suffix must match the cluster's actual suffix so the emitted SANs match what kube-dns returns for peer reverse-DNS. The operator auto-discovers it from `/etc/resolv.conf`'s `search` line at startup (covers `cluster.local`, `cozy.local`, and any other kubelet-injected suffix for normal cluster-pod deployments); falls back to `cluster.local` when auto-discovery yields nothing (hostNetwork pods, custom `dnsPolicy`). Override explicitly with `--cluster-domain=<suffix>` when neither path finds the right value.

Required SANs on the per-cluster server cert (BYO must cover all of these because the cert is shared by every member; cert-manager mode emits them automatically based on `--cluster-domain`):

- `*.<cluster>.<ns>.svc` (DNS SAN, wildcard — etcd ≥3.4 supports wildcard DNS SANs)
- `*.<cluster>.<ns>.svc.<cluster-domain>` (also wildcard) — required by etcd's peer-mTLS verification, which reverse-DNS-looks-up the connecting peer's IP. Kubernetes' DNS returns the fully-qualified `<pod>.<svc>.<ns>.svc.<cluster-domain>` form, and the cert SAN has to cover it. `<cluster-domain>` is `cluster.local` on most clusters; Cozystack uses `cozy.local`. Check `kubectl exec -n kube-system <coredns-pod> -- cat /etc/coredns/Corefile` or your cluster's resolved DNS suffix if unsure.
- `<cluster>.<ns>.svc` (the headless Service)
- `<cluster>-client.<ns>.svc` (the client Service)
- `localhost` (DNS SAN, for the `kubectl exec ... etcdctl --endpoints=https://localhost:2379` flow documented under operations)
- `127.0.0.1` (IP SAN — same)

Required SANs on the per-cluster peer cert: both `*.<cluster>.<ns>.svc` AND `*.<cluster>.<ns>.svc.<cluster-domain>` — the second is load-bearing for peer-mTLS, as above.

Server cert EKU **must include `serverAuth` AND `clientAuth`** (the etcd grpc-gateway loopback presents the server cert as a client cert when self-dialing; the server's `--trusted-ca-file` then verifies it with `ExtKeyUsageClientAuth`). Peer cert EKU must include both because peer is symmetric. Operator-client cert needs only `clientAuth`.

The `spec.tls` subtree is immutable post-create — flipping TLS on or off on an existing cluster is delete-and-recreate.

## Image versions

By default `spec.version` in an `EtcdCluster` becomes `quay.io/coreos/etcd:v<version>`. For air-gapped environments that mirror the image to a private registry there are two override surfaces, lowest-precedence first:

- **Operator-wide default** — set `etcdImage.repository` in the chart (env `ETCD_IMAGE_REPOSITORY` / flag `--etcd-image-repository`) to a registry/path, e.g. `registry.internal/mirror/etcd`. Every cluster that doesn't override picks it up; the tag stays `v<version>`.
- **Per-cluster** — `spec.image` on an `EtcdCluster` overrides the repository, the tag, and the pull policy for that cluster's member Pods:

  ```yaml
  spec:
    version: "3.6.11"
    image:
      repository: registry.internal/mirror/etcd  # optional; falls back to the operator default
      tag: ""                                     # optional; defaults to v3.6.11
      pullPolicy: IfNotPresent                    # optional; this is the default
    imagePullSecrets:
      - name: regcreds                            # Secret in the cluster's namespace
  ```

  `spec.imagePullSecrets` references pull-credential Secrets in the cluster's own namespace and is passed straight through to each member Pod. Note the operator still keys every version-dependent behaviour off `spec.version`, not off `spec.image.tag` — set the tag only when the mirror's tag scheme differs from the upstream `vX.Y.Z`. Like `spec.resources`, an image change applies to **newly-created** members (scale-up, replacement), not existing Pods in place.

  These two surfaces cover the **etcd member image** only. Two caveats in a fully air-gapped install where the operator image is mirrored too:

  - `spec.imagePullSecrets` is set Pod-wide, so it **does** cover the member Pod's restore initContainer (which runs the operator image at bootstrap-from-snapshot) — a `spec.bootstrap.restore` can pull the mirrored operator image via these secrets.
  - Standalone `EtcdSnapshot` backup/restore **Jobs** also run the operator image but are **not** covered by `spec.imagePullSecrets`. Repoint the operator image (chart `image.repository`) and make sure the snapshot's namespace can already pull it, or those Jobs `ImagePullBackOff`. The operator Pod itself uses the chart-level `imagePullSecrets`.

The `spec.version` examples throughout these docs use **3.6.x**, to match the `etcdutl` bundled in the operator image: `spec.bootstrap.restore` requires `spec.version` and that `etcdutl` to share a minor (see the [restore runbook](operations.md#restoring-a-cluster-from-a-snapshot)). The operator's etcd client is v3.6.x and is wire-compatible with 3.5.x servers, so a cluster you never restore into can still run 3.5.x — but to back up and restore on the same version, run 3.6.x.

Operator's own toolchain (relevant when building from source):

| Component | Version |
|---|---|
| Go | 1.25.10 |
| controller-runtime | v0.21 |
| k8s.io/api, k8s.io/client-go | v0.33 |
| controller-gen | v0.18.0 |
| Helm (install/render the chart) | v3.16+ |
| etcd client (`go.etcd.io/etcd/client/v3`) | v3.6.11 |
| Kubebuilder layout | v4 |

All pinned in `go.mod`, `Dockerfile`, and `Makefile`.

## RBAC

The operator runs as a ClusterRole — it needs to watch `EtcdCluster` and `EtcdMember` across all namespaces, plus create/delete the per-member Pods, PVCs, and Services in each user namespace. The rules are generated from the `+kubebuilder:rbac` markers (by `make manifests`) into `charts/etcd-operator/files/manager-role-rules.yaml` and pulled into the chart's templated ClusterRole — don't hand-edit; edit the markers and regenerate.

Single-namespace scoping is not currently exposed: `main.go` does not wire a namespace flag into the manager's `Cache.DefaultNamespaces`, so the manager always watches all namespaces. Limiting RBAC alone (ClusterRole → Role) is not sufficient — the manager will still attempt list/watch across the cluster and the API server will deny it. Scoped deployment is a follow-up.

## Networking

The operator creates two Services per `EtcdCluster`:

- **`<cluster>`** — headless (`clusterIP: None`), `publishNotReadyAddresses: true`, selector `etcd-operator.cozystack.io/cluster=<cluster>`, exposes **both 2379 (client) and 2380 (peer)**. Used by etcd for peer discovery and by the operator's own etcd client (which dials per-pod DNS `<member>.<cluster>.<ns>.svc:2379` resolved through this service). `publishNotReadyAddresses` is required for bootstrap: members during the initial join window aren't `Ready` yet but still need DNS entries to find each other.
- **`<cluster>-client`** — `ClusterIP`, exposes 2379 only. Intended for end-user client traffic (load-balanced across all pods backing the selector).

External access (NodePort / LoadBalancer / Ingress) isn't created automatically. If you need it, layer a separate Service or Ingress on top of `<cluster>-client`'s selector.

A specific routing pitfall: kube-apiserver pointed at the headless Service or the client `ClusterIP` will round-robin its etcd client across all reachable backends, including any current learner. Learners reject `MemberList` etc. with "rpc not supported for learner". The operator's *own* etcd client filters learners out (issue #12 fix in `memberEndpoints` / `discoverMemberID`), but you can't make kube-apiserver do the same. The pragmatic options for apiserver→etcd:

- Point at a single voter Pod's per-pod DNS name. Simple, fragile (Pod rescheduling).
- Point at a leader-aware proxy (etcd's own gRPC-proxy, or a sidecar). Robust, extra moving part.
- Accept occasional "rpc not supported for learner" errors during scale-up windows; kube-apiserver's own retry layer absorbs them.

This is outside the operator's scope but documented because operators ask.

## Teardown

```sh
# Remove individual clusters first — their finalizers will clean up etcd state.
kubectl delete etcdcluster.etcd-operator.cozystack.io --all -A

# Remove the operator (helm uninstall). The CRDs carry
# helm.sh/resource-policy: keep, so they (and any surviving EtcdClusters) are
# intentionally left in place.
make undeploy

# Remove the CRDs too (only after all EtcdClusters are gone) — deleting them
# cascade-deletes every remaining EtcdCluster:
kubectl delete crd etcdclusters.etcd-operator.cozystack.io \
  etcdmembers.etcd-operator.cozystack.io \
  etcdsnapshots.etcd-operator.cozystack.io
```

Deleting an `EtcdCluster` while it's running cascades through every owned resource: the operator's finalizer on each `EtcdMember` calls `MemberRemove` (when the cluster itself is also being deleted, the operator detects this and skips `MemberRemove` to avoid a deadlock — see `handleDeletion` in `controllers/etcdmember_controller.go`). Pods and PVCs are then GC'd via owner-refs.

If the operator is uninstalled while `EtcdCluster` resources still exist, they're stranded — the finalizers won't run because no controller is reading the queue. Recovery is to either re-install the operator, or `kubectl patch ... --type=merge -p '{"metadata":{"finalizers":null}}'` on each `EtcdMember` (manual, leaves PVCs and Pods in place — clean them up by label).

## Upgrades

For now, in-place operator upgrades work via `kubectl set image` on the operator Deployment, but in-place etcd version upgrades **do not** — changing `spec.version` on an existing `EtcdCluster` only affects newly-created members. See [What's not supported](../README.md#whats-not-supported-yet). The current recommended path for an etcd-version bump is:

1. Scale up by one to introduce a new-version member as a learner.
2. Scale down by one to evict an old-version member.
3. Repeat for each member.
4. Once all members are on the new version, edit `spec.version` so future scale-ups use it directly.

This is manual and slow. A native rolling upgrade is a tracked follow-up.

## kubectl-etcd plugin

`kubectl-etcd` is an optional client-side [kubectl plugin](https://kubernetes.io/docs/tasks/extend-kubectl/kubectl-plugins/) for day-2 operations on operator-managed clusters (member list, status, defrag, compact, alarms, snapshot, member add/remove). It runs on your workstation against your kubeconfig — it is **not** part of the operator image.

Each release attaches `kubectl-etcd-<os>-<arch>` binaries (with `cli-SHA256SUMS.txt`). Install it onto your `PATH` named `kubectl-etcd`, and kubectl picks it up as `kubectl etcd`:

```sh
VERSION=v0.5.0; OS=$(uname -s | tr A-Z a-z); ARCH=$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')
curl -sSLo kubectl-etcd "https://github.com/cozystack/etcd-operator/releases/download/$VERSION/kubectl-etcd-$OS-$ARCH"
chmod +x kubectl-etcd && sudo mv kubectl-etcd /usr/local/bin/   # any dir on $PATH works

kubectl etcd --version
kubectl etcd members --help
```

Or build from a checkout with `make kubectl-etcd` (lands in `bin/kubectl-etcd`). There is no krew package yet.

## Development

Out-of-cluster development run (against the current `$KUBECONFIG`):

```sh
make run
```

This builds and runs the operator binary on your laptop. It can reconcile `EtcdCluster` resources but **cannot dial etcd via in-cluster DNS** — `MemberList`/`MemberAdd`/`MemberRemove` will fail. Useful for testing reconcile loop logic against the apiserver, not for end-to-end testing. For e2e use the deploy flow above.
