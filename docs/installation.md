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

## Quick deploy

The repo's Makefile drives a complete install. From a checkout:

```sh
# 1. Install the CRDs cluster-wide.
make install

# 2. Build the operator image (or skip to a prebuilt registry tag).
make docker-build docker-push IMG=<your-registry>/etcd-operator:<tag>

# 3. Deploy the operator (creates the etcd-operator-system namespace,
#    ClusterRole/Binding, controller Deployment, metrics service).
make deploy IMG=<your-registry>/etcd-operator:<tag>
```

The cluster must be able to pull from `<your-registry>`. For local clusters (`kind` / `minikube` / `k3d`), either sideload the image (`kind load docker-image ...`) or push to an ephemeral registry the cluster can reach (e.g. `ttl.sh/<random>:1h`); otherwise the operator Deployment goes `ImagePullBackOff` with no clear hint from the operator side.

By default this lands in the `etcd-operator-system` namespace. The deployment name is `etcd-operator-controller-manager`. Verify:

```sh
kubectl get pod -n etcd-operator-system
kubectl logs -n etcd-operator-system deploy/etcd-operator-controller-manager \
  -c manager --tail=20
```

You should see the manager start lines and an empty work-queue (no `EtcdCluster` resources yet).

## Manual install (no Make)

If you don't want to invoke the Makefile (e.g. GitOps environments where `kustomize` is run by a controller in-cluster):

```sh
# CRDs
kubectl apply -f config/crd/bases/

# Operator + RBAC + Service, rendered by kustomize:
bin/kustomize-v5.6.0 build config/default | kubectl apply -f -
```

Override the image inline:

```sh
cd config/manager && bin/kustomize-v5.6.0 edit set image controller=<your-image>
cd ../.. && bin/kustomize-v5.6.0 build config/default | kubectl apply -f -
```

The `bin/kustomize-v*` binary is auto-downloaded by `make kustomize` (version pinned to `v5.6.0` in the Makefile); a system-installed `kustomize` works equally if you have one.

> **Set the operator image, or backups/restores won't run.** The `config/default` overlay rewrites both the manager's `image:` *and* its `OPERATOR_IMAGE` env var to the same ref via a kustomize replacement — `OPERATOR_IMAGE` is the image the operator launches for backup Jobs and restore init containers. If you bypass that overlay (e.g. `kubectl apply -f config/manager/` directly, or a base that drops the replacement), `OPERATOR_IMAGE` stays the placeholder `controller:latest`. The operator **refuses to start** on that placeholder and exits with a clear error (rather than letting backup/restore Pods `ImagePullBackOff` later). Always render through `config/default` (which the commands above do) or set `OPERATOR_IMAGE` to the real ref by hand.

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

This trades durability for speed: a Pod that loses its tmpfs (eviction, node failure) loses its data and the member is automatically replaced via `MemberRemove` + scale-up. **Don't use it as a general-purpose etcd backend** — see [docs/concepts.md](concepts.md#storage) and [docs/operations.md](operations.md#memory-backed-clusters) for the full trade-off. The remaining production hardening gaps (anti-affinity, container memory limits) are tracked in [#16](https://github.com/lllamnyp/etcd-operator/issues/16). The apiserver rejects `replicas: 0` on memory clusters via the [CEL validation rules](concepts.md#apiserver-enforced-validation), and every cluster gets an auto-emitted [PodDisruptionBudget](concepts.md#poddisruptionbudget).

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

`spec.version` in an `EtcdCluster` becomes `quay.io/coreos/etcd:v<version>`. The image repository is hard-coded in `controllers/helpers.go:EtcdImage`. Override it by patching the operator image with your own registry/repo if you mirror etcd internally.

The `spec.version` examples throughout these docs use **3.6.x**, to match the `etcdutl` bundled in the operator image: `spec.bootstrap.restore` requires `spec.version` and that `etcdutl` to share a minor (see the [restore runbook](operations.md#restoring-a-cluster-from-a-snapshot)). The operator's etcd client is v3.6.x and is wire-compatible with 3.5.x servers, so a cluster you never restore into can still run 3.5.x — but to back up and restore on the same version, run 3.6.x.

Operator's own toolchain (relevant when building from source):

| Component | Version |
|---|---|
| Go | 1.25.10 |
| controller-runtime | v0.21 |
| k8s.io/api, k8s.io/client-go | v0.33 |
| controller-gen | v0.18.0 |
| kustomize | v5.6.0 |
| etcd client (`go.etcd.io/etcd/client/v3`) | v3.6.11 |
| Kubebuilder layout | v4 |

All pinned in `go.mod`, `Dockerfile`, and `Makefile`.

## RBAC

The operator runs as a ClusterRole — it needs to watch `EtcdCluster` and `EtcdMember` across all namespaces, plus create/delete the per-member Pods, PVCs, and Services in each user namespace. The full role lives in `config/rbac/role.yaml` (regenerated from `+kubebuilder:rbac` markers — don't hand-edit).

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

# Remove the operator.
make undeploy

# Remove the CRDs (only after all EtcdClusters are gone; the CRDs have
# protected finalizers via the operator).
make uninstall
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

## Development

Out-of-cluster development run (against the current `$KUBECONFIG`):

```sh
make run
```

This builds and runs the operator binary on your laptop. It can reconcile `EtcdCluster` resources but **cannot dial etcd via in-cluster DNS** — `MemberList`/`MemberAdd`/`MemberRemove` will fail. Useful for testing reconcile loop logic against the apiserver, not for end-to-end testing. For e2e use the deploy flow above.
