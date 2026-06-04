/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	clientv3 "go.etcd.io/etcd/client/v3"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// DefaultProgressDeadlineSeconds is used when the user doesn't set one.
const DefaultProgressDeadlineSeconds = int32(600)

// EtcdClusterReconciler reconciles an EtcdCluster object.
type EtcdClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// EtcdClientFactory builds an etcd client. Tests inject a fake;
	// production wiring uses DefaultEtcdClientFactory.
	EtcdClientFactory EtcdClientFactory

	// CertManagerAvailable reports whether the cert-manager.io/v1 API
	// is registered on this cluster. Detected once at operator startup
	// via the discovery API. When false, clusters using
	// spec.tls.{client,peer}.certManager are surfaced as terminally
	// Available=False/CertManagerNotInstalled rather than being
	// reconciled into an informer hot-loop.
	CertManagerAvailable bool

	// ClusterDomain is the DNS suffix used for the cluster-domain half
	// of Kubernetes service / pod DNS (e.g. "cluster.local",
	// "cozy.local"). Threaded into the cert-manager-emitted Certificate
	// SAN lists so the FQDN form (`*.<cluster>.<ns>.svc.<cluster-domain>`)
	// matches what kube-dns returns for the peer reverse-DNS lookup
	// etcd uses to authenticate incoming peer mTLS connections.
	//
	// Defaults to "cluster.local" when empty. Override at operator
	// startup with the --cluster-domain flag.
	ClusterDomain string
}

//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdclusters,verbs=get;list;watch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdclusters/finalizers,verbs=update
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdmembers,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdmembers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch
//+kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch
// `delete` is intentionally omitted from the certificates verb list:
// each Certificate is owned by the EtcdCluster via SetControllerReference,
// and spec.tls is CEL-immutable post-create, so the only Certificate
// deletion path is the apiserver's owner-ref GC cascading from a cluster
// delete. If a future change relaxes the spec.tls immutability rule,
// add `delete` here.
//+kubebuilder:rbac:groups=cert-manager.io,resources=certificates,verbs=get;list;watch;create;update;patch

func (r *EtcdClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	cluster := &lll.EtcdCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// If the cluster is being deleted, don't keep reconciling it. Owned
	// resources are cascaded out via owner refs; recreating a Service for a
	// Terminating cluster races against the GC and pollutes logs.
	if !cluster.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Terminal-config gate. spec.tls.{client,peer}.certManager requires
	// cert-manager.io/v1 to be registered on the apiserver (detected at
	// operator startup). When it isn't, the cluster cannot form a single
	// Pod (the member controller is gated on Secret existence; cert-
	// manager would never materialise it). Surface Available=False/
	// CertManagerNotInstalled and stop here so the rest of Reconcile
	// (which would clobber this condition with ClusterUnreachable /
	// WaitingForSeed once it tries to bootstrap a member) doesn't run.
	if !r.CertManagerAvailable && hasCertManagerTLS(cluster) {
		changed := setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse,
			"CertManagerNotInstalled",
			"spec.tls.{client,peer}.certManager is set but cert-manager.io/v1 is not registered on this cluster; install cert-manager and restart the operator, or switch to BYO Secrets")
		if changed {
			if err := r.Status().Update(ctx, cluster); err != nil {
				if errors.IsConflict(err) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if err := r.ensureServices(ctx, cluster); err != nil {
		log.Error(err, "failed to ensure services")
		return ctrl.Result{}, err
	}

	if err := r.reconcileTLSCertificates(ctx, cluster); err != nil {
		log.Error(err, "failed to reconcile cert-manager Certificates")
		return ctrl.Result{}, err
	}

	memberList := &lll.EtcdMemberList{}
	if err := r.List(ctx, memberList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabels{LabelCluster: cluster.Name},
	); err != nil {
		return ctrl.Result{}, err
	}
	active := filterActiveMembers(memberList.Items)
	// running excludes dormant members. The cluster controller's replica
	// accounting (`current`), readiness checks, and most scale decisions
	// operate on this set: a dormant member has no Pod and contributes
	// no etcd capacity, so it must not count as "we have N members" or
	// "we need to wait for it to become Ready" — otherwise the resume
	// flow could never decide to wake it.
	running := filterRunningMembers(memberList.Items)

	// ── First-reconcile init ──────────────────────────────────────────
	// Combine the ClusterToken latch, initial Observed snapshot, and
	// progress deadline into a single Status write so a brand-new cluster
	// settles into a reconcile-ready shape in one pass.
	current := int32(len(running))
	now := metav1.Now()

	if cluster.Status.ClusterToken == "" || cluster.Status.Observed == nil {
		log.Info("initialising cluster status (token + observed snapshot)")
		if cluster.Status.ClusterToken == "" {
			cluster.Status.ClusterToken = deriveClusterToken(cluster)
		}
		if cluster.Status.Observed == nil {
			snapshotSpecIntoObserved(cluster)
			setProgressDeadline(cluster, now)
			setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionTrue, "InitialSnapshot",
				"snapshotted initial spec into status.observed")
		}
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// ── Subsequent spec-change handling ───────────────────────────────
	// Triggers that update Observed from the latest spec:
	//   - the previous Observed target is complete and spec has changed
	//   - the deadline has elapsed (handled separately)

	// reconciliationComplete uses the running set (non-dormant). A
	// paused cluster has observed.Replicas=0 and len(running)=0 even
	// while a dormant CR sits alongside; passing `active` here would
	// count the dormant member against the target and never reach
	// complete=true, so the spec-change-adoption path would never fire
	// when the user scales the paused cluster back up.
	complete := reconciliationComplete(cluster, running)

	if !complete && deadlineExpired(cluster, now) {
		return r.handleDeadlineExceeded(ctx, cluster, now)
	}

	if complete && !specEqualsObserved(cluster) {
		log.Info("previous target reached; adopting new spec",
			"observed", cluster.Status.Observed, "spec", cluster.Spec)
		snapshotSpecIntoObserved(cluster)
		setProgressDeadline(cluster, now)
		setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionTrue, "SpecChanged",
			"adopting new spec after previous target reached")
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	desired := cluster.Status.Observed.Replicas

	// ── Bootstrap ──────────────────────────────────────────────────────
	// Bootstrap creates a single-member etcd cluster (member -0 only) with
	// --initial-cluster-state=new. Once ClusterID is latched, scale-up adds
	// the remaining members via MemberAdd. This avoids the historical bug
	// where multiple bootstrapping members had to agree on a single
	// --initial-cluster value and any mid-flight replica change would
	// corrupt that consensus.
	if cluster.Status.ClusterID == "" {
		// Pre-bootstrap fan-out:
		//  - observed.replicas == 0: user declared the cluster as paused
		//    from the start. Do NOT bootstrap a transient seed only to
		//    immediately tear it down — that would waste a pod + image
		//    pull + PVC write, surface a fleeting ClusterID for a cluster
		//    that was never used, and produce a misleading event sequence.
		//    Fall through to updateStatus, which has a Paused branch.
		//    The first non-zero spec the user submits will snapshot into
		//    observed and the bootstrap branch will fire then.
		//  - current == 0: fresh cluster, no seed yet → bootstrap() creates it.
		//  - hasPendingBootstrap: seed CR exists with empty InitialCluster
		//    (crash between Create and Patch) → bootstrap() completes it;
		//    skipping this and going to discovery would loop forever because
		//    the member controller won't start the seed's pod until
		//    InitialCluster is set.
		//  - Else: seed CR exists, fully wired, pod is coming up — proceed
		//    to discovery to latch ClusterID once etcd answers.
		if desired == 0 {
			log.Info("cluster declared paused from the start; not bootstrapping")
			return r.updateStatus(ctx, cluster, active)
		}
		if current == 0 || hasPendingBootstrap(running) {
			log.Info("bootstrapping single-node cluster")
			return r.bootstrap(ctx, cluster, running)
		}
		log.Info("waiting for bootstrap member to form cluster")
		return r.tryDiscoverCluster(ctx, cluster, running)
	}

	// ── Scale ──────────────────────────────────────────────────────────
	// Before deciding to scale either direction, wait for any in-flight
	// EtcdMember deletion to finish. The EtcdMember finalizer calls
	// MemberRemove against the etcd cluster; running that concurrently
	// with a MemberAdd from scale-up, or with another MemberRemove from
	// scale-down, races past quorum because each goroutine works from its
	// own snapshot of the etcd member list. One mutation at a time.
	for _, m := range memberList.Items {
		if !m.DeletionTimestamp.IsZero() {
			log.Info("waiting for in-flight member deletion before further scaling", "deleting", m.Name)
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	// If a half-baked scale-up CR is present (Spec.InitialCluster empty),
	// route through scaleUp regardless of replica accounting. Two reasons:
	//
	//  1. The member controller gates pod creation on InitialCluster being
	//     set, so the pending CR has no pod and contributes a never-Ready
	//     member to `running`. The standard `current < desired` +
	//     allMembersReady gate would wait forever.
	//  2. If the prior reconcile crashed AFTER MemberAddAsLearner but
	//     BEFORE the patch, etcd already has a learner whose peer URL
	//     points at the pending CR. promotePendingLearner cannot promote
	//     that learner (no pod = no sync), so any path that promotes
	//     first and patches second deadlocks. scaleUp patches first.
	if hasPendingMember(running) {
		log.Info("completing pending scale-up member before further action")
		return r.scaleUp(ctx, cluster, active)
	}

	if current < desired {
		// Resurrection from dormant: if a dormant member is parked, wake
		// it before doing anything else. This is the inverse of the 1→0
		// pause step — flip spec.Dormant back to false on the same
		// member and let the member controller bring its pod back up.
		// The dormant member is in `active` (it has no DeletionTimestamp)
		// but not in `running` (filtered by Dormant), which is exactly
		// the state that puts us here: current<desired with the dormant
		// CR sitting alongside.
		if findDormantMember(active) != nil {
			log.Info("waking dormant member", "desired", desired)
			return r.scaleUp(ctx, cluster, active)
		}
		// Don't add another member while existing ones aren't Ready yet.
		// Even with learner-mode adds (which don't shift voting quorum until
		// promotion), driving a sequence of MemberAddAsLearner without
		// waiting for the previous pod to come up makes the cluster
		// progressively more confused — the new pod has to fetch state from
		// peers, and stacking joiners against a half-empty cluster is at
		// best slow, at worst stalls indefinitely. Wait for everyone Ready
		// before adding the next.
		if !allMembersReady(running) {
			log.Info("waiting for existing members to become Ready before next scale-up step")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		log.Info("scaling up", "current", current, "desired", desired)
		return r.scaleUp(ctx, cluster, active)
	}
	if current > desired {
		log.Info("scaling down", "current", current, "desired", desired)
		return r.scaleDown(ctx, cluster, running)
	}

	// ── current == desired ────────────────────────────────────────────
	// One subtlety: the iteration that added the *last* learner returns
	// before that learner gets promoted (current is now equal to desired
	// so scaleUp won't run again on its own). We need a promote attempt
	// here too. Cheap: list etcd once and try to promote any learner;
	// no-op if none.
	if cluster.Status.ClusterID != "" && len(running) > 0 {
		endpoints := memberEndpoints(clusterClientScheme(cluster), running, cluster.Name, cluster.Namespace)
		tlsCfg, tlsErr := buildOperatorTLSConfig(ctx, r.Client, cluster)
		if tlsErr != nil {
			// Don't bail out of the whole reconcile — updateStatus
			// below still has useful work (PDB reconcile, cached-
			// status fields). Just skip the promote attempt and
			// surface the error so a missing/typo'd TLS Secret is
			// debuggable without rg-ing through silent retries.
			log.Error(tlsErr, "cannot build operator TLS config for promote-after-converged; skipping promotion this pass")
		} else if user, pass, _, credErr := resolveEtcdCredentials(ctx, r.Client, cluster); credErr != nil {
			// Same best-effort stance as the TLS-config error above: log and
			// skip promotion, let updateStatus do its work, retry next pass.
			log.Error(credErr, "cannot resolve etcd credentials for promote-after-converged; skipping promotion this pass")
		} else {
			etcdClient, err := r.EtcdClientFactory(ctx, endpoints, tlsCfg, user, pass)
			if err != nil {
				log.Error(err, "cannot dial etcd for promote-after-converged; skipping promotion this pass", "endpoints", endpoints)
			} else {
				defer etcdClient.Close()
				res, perr := r.promotePendingLearner(ctx, cluster, running, etcdClient, endpoints)
				if perr != nil {
					return ctrl.Result{}, perr
				}
				if res != nil {
					return *res, nil
				}
			}
		}
		// Fall through to updateStatus — the next reconcile will retry
		// promotion. This matches scaleUp's "log and continue" behaviour
		// when the etcd client can't be built.
	}

	// ── Enable auth once converged ─────────────────────────────────────
	// Provision the root user/role and turn on authentication, but only on
	// a healthy, fully-converged cluster (all desired members ready, quorum
	// formed). Gating on convergence keeps the auth flip from racing in-
	// flight scale-up dials. No-op (and skipped) once status.authEnabled
	// has latched.
	if res, err := r.reconcileAuth(ctx, cluster, running); err != nil {
		return ctrl.Result{}, err
	} else if res != nil {
		return *res, nil
	}

	// ── Steady state ───────────────────────────────────────────────────
	// Pass the full active set (including any dormant member), not
	// `running`: updateStatus's Paused-message branch derives the
	// dormant member from the slice via findDormantMember to name the
	// preserved PVC. Stripping dormant here would silently fall back
	// to the fresh-zero "no data has been written" message even when a
	// dormant member with a real PVC exists. updateStatus re-derives
	// `running` internally for its accounting, so passing `active`
	// here is the correct shape.
	return r.updateStatus(ctx, cluster, active)
}

// ── Bootstrap ────────────────────────────────────────────────────────────

// bootstrap brings up the single seed member for a fresh cluster. Its
// --initial-cluster lists only itself, so the etcd protocol cannot get
// confused by partial agreement; subsequent members join via MemberAdd
// once ClusterID is latched.
//
// Members are named via GenerateName ("<cluster>-"), not ordinal-suffixed,
// so seed identity is detected by Spec.Bootstrap=true rather than by a
// predictable name. Idempotency therefore looks for an existing
// Bootstrap=true CR before creating a new one. Cross-incarnation safety
// (a leftover member with this cluster's label but a different
// controller) is enforced explicitly — see comment below.
//
// The Create-then-Update sequence is required: the seed's --initial-cluster
// flag references its own (apiserver-assigned) name, so we can only fill
// Spec.InitialCluster *after* Create returns the assigned name. The member
// controller refuses to start a pod with an empty InitialCluster (see
// EtcdMemberReconciler.Reconcile), so the half-baked state between Create
// and Update doesn't reach the data plane.
func (r *EtcdClusterReconciler) bootstrap(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
) (ctrl.Result, error) {
	var seed *lll.EtcdMember
	for i := range members {
		if members[i].Spec.Bootstrap {
			seed = &members[i]
			break
		}
	}

	if seed == nil {
		seedLabels, seedAnnotations := applyAdditionalMetadata(clusterLabels(cluster.Name), cluster.Spec.AdditionalMetadata)
		seed = &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: cluster.Name + "-",
				Namespace:    cluster.Namespace,
				Labels:       seedLabels,
				Annotations:  seedAnnotations,
			},
			Spec: lll.EtcdMemberSpec{
				ClusterName:               cluster.Name,
				Version:                   cluster.Status.Observed.Version,
				Storage:                   cluster.Status.Observed.Storage,
				Resources:                 cluster.Status.Observed.Resources,
				AdditionalMetadata:        cluster.Spec.AdditionalMetadata,
				Affinity:                  cluster.Status.Observed.Affinity,
				TopologySpreadConstraints: cluster.Status.Observed.TopologySpreadConstraints,
				Bootstrap:                 true,
				ClusterToken:              cluster.Status.ClusterToken,
				TLS:                       deriveMemberTLS(cluster),
				// Restore is carried only by the seed: when the cluster
				// requests a restore, the member controller runs a restore
				// initContainer that populates the data dir from the snapshot
				// before etcd starts. Inert once the data dir is initialized.
				Restore: restoreForSeed(cluster),
				// InitialCluster filled in below once apiserver assigns Name.
			},
		}
		if err := controllerutil.SetControllerReference(cluster, seed, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, seed); err != nil {
			return ctrl.Result{}, err
		}
		// Pre-stamp IsVoter=true. The seed is never a learner — it forms
		// the cluster on its own with --initial-cluster-state=new. Setting
		// this here means the member controller applies the role=voter
		// Pod label on the first reconcile of the seed Pod, so the PDB
		// selects it from creation rather than only after MemberList runs
		// a few reconciles later.
		//
		// Written as a merge patch, not an Update: the member controller
		// reconciles the seed the moment it is created and its own
		// Status().Update races with this write — an optimistic-locked
		// Update here loses that race with a conflict, and a cache-backed
		// re-Get cannot recover (the informer may not even have the
		// object yet). This stamp happens only in the create branch —
		// losing it would leave the seed permanently IsVoter=false, and
		// both learner memberID discovery and promotion filter their dial
		// endpoints to voters, wedging every later scale-up.
		stampBase := seed.DeepCopy()
		seed.Status.IsVoter = true
		if err := r.Status().Patch(ctx, seed, client.MergeFrom(stampBase)); err != nil {
			return ctrl.Result{}, err
		}
	} else if !metav1.IsControlledBy(seed, cluster) {
		// Defence-in-depth. The primary safety net against cross-
		// incarnation adoption is that GenerateName produces a fresh
		// suffix on every Create, so we cannot collide with a stale
		// seed left over from a deleted prior cluster. This explicit
		// IsControlledBy check catches the residual cases: a manually
		// kubectl-created EtcdMember carrying our cluster label, or an
		// orphan whose owner reference was reaped without GC catching
		// the dependent. Refuse to adopt rather than risk treating
		// someone else's etcd member as ours.
		return ctrl.Result{}, fmt.Errorf(
			"bootstrap-flagged EtcdMember %q in namespace %q is not controlled by this EtcdCluster (uid=%s); "+
				"refusing to adopt", seed.Name, seed.Namespace, cluster.UID)
	}

	if seed.Spec.InitialCluster == "" {
		original := seed.DeepCopy()
		seed.Spec.InitialCluster = buildInitialCluster(clusterPeerScheme(cluster), []string{seed.Name}, cluster.Name, cluster.Namespace)
		// Now that apiserver-assigned name is known, populate the
		// per-member component label so the seed CR's label set matches
		// the Pod/PVC the member controller will create.
		if seed.Labels == nil {
			seed.Labels = map[string]string{}
		}
		for k, v := range memberLabels(cluster.Name, seed.Name) {
			seed.Labels[k] = v
		}
		if err := r.Patch(ctx, seed, client.MergeFrom(original)); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// ── Cluster discovery ────────────────────────────────────────────────────

func (r *EtcdClusterReconciler) tryDiscoverCluster(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Find the seed by Spec.Bootstrap. Member names are apiserver-assigned
	// via GenerateName, so the seed cannot be located by a predictable
	// name; Spec.Bootstrap is set on (and only on) the single CR that
	// bootstrap() created. apiserver does not guarantee any List ordering,
	// so trusting members[0] would silently anchor discovery to the wrong
	// member when scale-up CRs land in front of the seed.
	var seed *lll.EtcdMember
	for i := range members {
		if members[i].Spec.Bootstrap {
			seed = &members[i]
			break
		}
	}
	if seed == nil {
		log.Info("waiting for seed member to appear before discovery")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	// If the seed's Pod hasn't been created yet, dialing its DNS name just
	// times out the reconcile budget for no signal. Surface the wait as
	// Progressing=True/WaitingForSeed — distinct from
	// Available=False/ClusterUnreachable (which is reserved for "we
	// expected to dial something and couldn't"). Skips the Status write if
	// the condition didn't move (e.g. we already wrote WaitingForSeed last
	// reconcile).
	if seed.Status.PodName == "" {
		if setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionTrue, "WaitingForSeed",
			fmt.Sprintf("seed member %q has no Pod yet", seed.Name)) {
			if upErr := r.Status().Update(ctx, cluster); upErr != nil {
				if errors.IsConflict(upErr) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, upErr
			}
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	endpoints := []string{clientURL(clusterClientScheme(cluster), seed.Name, cluster.Name, cluster.Namespace)}

	tlsCfg, err := buildOperatorTLSConfig(ctx, r.Client, cluster)
	if err != nil {
		return r.surfaceDiscoveryError(ctx, cluster, "operator TLS config", err)
	}
	// Bootstrap-window dial: discovery only runs while ClusterID is unset,
	// strictly before reconcileAuth can turn auth on, so we always dial
	// anonymously here (resolveEtcdCredentials would return empty creds too,
	// since status.authEnabled cannot be true yet).
	etcdClient, err := r.EtcdClientFactory(ctx, endpoints, tlsCfg, "", "")
	if err != nil {
		return r.surfaceDiscoveryError(ctx, cluster, "etcd client construction failed", err)
	}
	defer etcdClient.Close()

	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := etcdClient.MemberList(listCtx)
	if err != nil {
		return r.surfaceDiscoveryError(ctx, cluster, "MemberList failed", err)
	}

	// Only latch ClusterID when the response unambiguously describes the
	// bootstrap state. With single-member bootstrap that means: exactly
	// one member, and its name matches the seed (or its peer URL does, if
	// etcd hasn't yet propagated the name).
	if len(resp.Members) != 1 {
		log.Info("waiting for definitive single-member response", "members", len(resp.Members))
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	expectedPeer := peerURL(clusterPeerScheme(cluster), seed.Name, cluster.Name, cluster.Namespace)
	matched := resp.Members[0].Name == seed.Name
	if !matched {
		for _, p := range resp.Members[0].PeerURLs {
			if p == expectedPeer {
				matched = true
				break
			}
		}
	}
	if !matched {
		log.Info("MemberList returned an unexpected member; retrying",
			"got_name", resp.Members[0].Name, "got_urls", resp.Members[0].PeerURLs)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// Discovery success. Latch ClusterID and clear any stale failure
	// condition from earlier retries so the cluster doesn't sit
	// Available=False until the next reconcile cycle.
	cluster.Status.ClusterID = fmt.Sprintf("%016x", resp.Header.ClusterId)
	setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionTrue, "ClusterDiscovered",
		fmt.Sprintf("etcd cluster %s discovered", cluster.Status.ClusterID))
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// surfaceDiscoveryError reports that the seed pod exists but we can't talk
// to it. Gates the Status write on whether the condition actually moved so
// we don't bump resourceVersion every 10 s while waiting for the etcd
// process to come up. Errors with embedded timestamps would defeat the
// short-circuit, so we strip the variable portion of common timeouts.
func (r *EtcdClusterReconciler) surfaceDiscoveryError(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	what string,
	err error,
) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	log.Error(err, what)

	// etcd is demanding authentication but the operator has no credentials to
	// present (spec.auth unset) and auth hasn't latched. This is the
	// restore-an-auth-enabled-snapshot-without-spec.auth case (or auth enabled
	// out-of-band): anonymous dials are rejected, and because spec.auth is
	// immutable post-create no spec edit can recover it — retrying forever would
	// just hide the cause behind a generic ClusterUnreachable. Surface a
	// specific, actionable Degraded condition and stop the retry loop; recovery
	// is delete-and-recreate with spec.auth, which yields a fresh object.
	if isAuthRequiredErr(err) && !authConfigured(cluster) && !cluster.Status.AuthEnabled {
		const reason = "AuthRequiredNotConfigured"
		msg := "etcd requires authentication but spec.auth is not configured, so the operator cannot manage this cluster. " +
			"This usually means a snapshot from an auth-enabled cluster was restored without spec.auth. " +
			"Auth is immutable post-create: delete and recreate the cluster with spec.auth.enabled and a rootCredentialsSecretRef " +
			"holding the snapshot's original root password (see the restore runbook)."
		changed := setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse, reason, msg)
		if setClusterCondition(cluster, lll.ClusterDegraded, metav1.ConditionTrue, reason, msg) {
			changed = true
		}
		if changed {
			if upErr := r.Status().Update(ctx, cluster); upErr != nil {
				if errors.IsConflict(upErr) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, upErr
			}
		}
		return ctrl.Result{}, nil
	}

	// etcd rejected the credentials we presented (wrong root password) — the
	// most likely restore mistake: spec.auth.rootCredentialsSecretRef must hold
	// the snapshot's *original* root password (etcd stores its bcrypt hash). This
	// is recoverable by fixing the Secret's contents (the operator re-reads the
	// password every dial), so keep requeueing — but surface a specific reason
	// rather than an opaque ClusterUnreachable so the operator knows the fix.
	if isAuthFailedErr(err) {
		const reason = "AuthCredentialsRejected"
		msg := "etcd rejected the operator's credentials (authentication failed). " +
			"spec.auth.rootCredentialsSecretRef must hold the root password etcd actually has — " +
			"for a cluster restored from an auth-enabled snapshot that is the snapshot's original password " +
			"(etcd stores its bcrypt hash, so a fresh password will not authenticate). Correct the Secret's password (see the restore runbook)."
		changed := setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse, reason, msg)
		if setClusterCondition(cluster, lll.ClusterDegraded, metav1.ConditionTrue, reason, msg) {
			changed = true
		}
		if changed {
			if upErr := r.Status().Update(ctx, cluster); upErr != nil {
				if errors.IsConflict(upErr) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, upErr
			}
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	msg := fmt.Sprintf("%s: %s", what, stableErrorMessage(err))
	if setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse, "ClusterUnreachable", msg) {
		if upErr := r.Status().Update(ctx, cluster); upErr != nil {
			if errors.IsConflict(upErr) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, upErr
		}
	}
	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// authConfigured reports whether the operator has root credentials to present
// on its etcd dials (spec.auth enabled). Distinguishes "etcd demands auth but
// we have nothing to offer" from a transient dial failure.
func authConfigured(cluster *lll.EtcdCluster) bool {
	return cluster.Spec.Auth != nil && cluster.Spec.Auth.Enabled
}

// stableErrorMessage strips per-call variable portions (timestamps, addresses
// embedded in DNS-lookup errors, etc.) from common etcd-client error strings.
// Keeps the kind-of-error stable across retries so the condition message
// doesn't move on every reconcile.
func stableErrorMessage(err error) string {
	s := err.Error()
	// "lookup foo on 127.0.0.53:53: server misbehaving" -> "lookup foo: server misbehaving"
	if i, j := strings.Index(s, " on "), strings.Index(s, ": "); i > 0 && j > i {
		s = s[:i] + s[j:]
	}
	return s
}

// ── Scale up ─────────────────────────────────────────────────────────────

// scaleUp adds one member to the etcd cluster — or, if a previous
// reconcile crashed mid-flight, completes that earlier attempt. The
// crash-recovery story shapes the function's ordering: there are two
// dangerous windows, and the steps that close them must run before any
// step that could short-circuit on an unrelated condition.
//
// Flow:
//
//  1. Fetch etcd's MemberList. We use the same snapshot for every
//     decision below to avoid races against a moving cluster view.
//  2. Adopt any pending EtcdMember (Spec.InitialCluster=="") from a prior
//     reconcile. There are two pending sub-states:
//     a. peer URL ALREADY in etcd (post-AddAsLearner crash): patch
//     Spec.InitialCluster immediately. This must happen BEFORE any
//     promotion attempt — the orphan learner cannot sync until its
//     pod is up, the pod cannot start until InitialCluster is set,
//     and promotion would block forever on the un-synced learner.
//     b. peer URL NOT yet in etcd (pre-AddAsLearner crash): retry
//     MemberAddAsLearner, then patch.
//  3. With no pending CR, try to promote a learner (the iteration after
//     a successful AddAsLearner + pod-up + sync).
//  4. If no learner was promotable, Create a fresh CR with GenerateName,
//     MemberAddAsLearner, then patch.
//
// Until the patch in step 2a, 2b, or 4 completes, the member controller
// refuses to start a pod (see EtcdMemberReconciler.Reconcile) — keeping
// half-baked state out of the data plane. The finalizer is added by
// the member controller before that gate, so a mid-flight delete still
// triggers MemberRemove cleanup of the registered learner.
func (r *EtcdClusterReconciler) scaleUp(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Wake from dormant: if a dormant member exists, flip its
	// Spec.Dormant flag back to false. The member controller's
	// reconcile loop then ensures the Pod is back up against the same
	// PVC (which never lost its EtcdMember owner-ref). Same ClusterID,
	// same member ID, same data — etcd resumes from the existing data
	// dir. No name lookup, no Create-by-fixed-name, no foreign-CR
	// adoption window to defend against.
	if dormant := findDormantMember(members); dormant != nil {
		log.Info("waking dormant member", "name", dormant.Name)
		original := dormant.DeepCopy()
		dormant.Spec.Dormant = false
		if err := r.Patch(ctx, dormant, client.MergeFrom(original)); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	// `members` passed in here is the active set (including any dormant
	// CR, which we just handled). From this point on we only care about
	// running (non-dormant) members for the etcd-side flow.
	running := filterRunningMembers(members)
	endpoints := memberEndpoints(clusterClientScheme(cluster), running, cluster.Name, cluster.Namespace)
	tlsCfg, err := buildOperatorTLSConfig(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "cannot build operator TLS config for scale-up")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	user, pass, _, err := resolveEtcdCredentials(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "cannot resolve etcd credentials for scale-up")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	etcdClient, err := r.EtcdClientFactory(ctx, endpoints, tlsCfg, user, pass)
	if err != nil {
		log.Error(err, "cannot connect to etcd for scale-up", "endpoints", endpoints)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	defer etcdClient.Close()

	// Step 1: snapshot etcd's member list once.
	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	listResp, err := etcdClient.MemberList(listCtx)
	if err != nil {
		log.Error(err, "MemberList failed", "endpoints", endpoints)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Step 2: handle a pending CR if one exists. Defence-in-depth: see
	// the comment in bootstrap() — GenerateName precludes name-collision
	// adoption, but a manually-created CR or a label-matched orphan
	// could in principle slip through; refuse rather than adopt.
	pending := findPendingMember(running)
	if pending != nil {
		if !metav1.IsControlledBy(pending, cluster) {
			return ctrl.Result{}, fmt.Errorf(
				"pending EtcdMember %q in namespace %q is not controlled by this EtcdCluster (uid=%s); "+
					"refusing to adopt", pending.Name, pending.Namespace, cluster.UID)
		}
		return r.completePendingMember(ctx, cluster, etcdClient, listResp, pending)
	}

	// Step 3: no pending CR. Try to promote any learner that's caught up.
	if res, err := tryPromoteLearner(ctx, etcdClient, listResp); err != nil || res != nil {
		return resultOrZero(res), err
	}

	// Step 4: no learner waiting. Create a fresh CR, AddAsLearner, patch.
	mLabels, mAnnotations := applyAdditionalMetadata(clusterLabels(cluster.Name), cluster.Spec.AdditionalMetadata)
	newMember := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: cluster.Name + "-",
			Namespace:    cluster.Namespace,
			Labels:       mLabels,
			Annotations:  mAnnotations,
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName:               cluster.Name,
			Version:                   cluster.Status.Observed.Version,
			Storage:                   cluster.Status.Observed.Storage,
			Resources:                 cluster.Status.Observed.Resources,
			AdditionalMetadata:        cluster.Spec.AdditionalMetadata,
			Affinity:                  cluster.Status.Observed.Affinity,
			TopologySpreadConstraints: cluster.Status.Observed.TopologySpreadConstraints,
			Bootstrap:                 false,
			ClusterToken:              cluster.Status.ClusterToken,
			TLS:                       deriveMemberTLS(cluster),
			// InitialCluster filled in by completePendingMember below.
		},
	}
	if err := controllerutil.SetControllerReference(cluster, newMember, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, newMember); err != nil {
		return ctrl.Result{}, err
	}
	return r.completePendingMember(ctx, cluster, etcdClient, listResp, newMember)
}

// completePendingMember finishes the AddAsLearner + Spec.InitialCluster
// patch for `pending`. It is idempotent in both directions of the crash
// window:
//
//   - If `pending`'s peer URL is already in etcd's MemberList (passed in
//     as `listResp`), MemberAddAsLearner is skipped and the patch uses
//     the existing list.
//   - Otherwise MemberAddAsLearner is called and the patch uses the
//     resulting member list from the add response.
//
// Either way, after this call the CR has Spec.InitialCluster set and the
// member controller can start the pod.
func (r *EtcdClusterReconciler) completePendingMember(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	etcdClient EtcdClusterClient,
	listResp *clientv3.MemberListResponse,
	pending *lll.EtcdMember,
) (ctrl.Result, error) {
	log := log.FromContext(ctx)
	pendingPeerURL := peerURL(clusterPeerScheme(cluster), pending.Name, cluster.Name, cluster.Namespace)

	alreadyAdded := false
	for _, m := range listResp.Members {
		for _, p := range m.PeerURLs {
			if p == pendingPeerURL {
				alreadyAdded = true
				break
			}
		}
		if alreadyAdded {
			break
		}
	}

	etcdMembers := listResp.Members
	if !alreadyAdded {
		addCtx, addCancel := context.WithTimeout(ctx, 10*time.Second)
		defer addCancel()
		addResp, err := etcdClient.MemberAddAsLearner(addCtx, []string{pendingPeerURL})
		if err != nil {
			log.Error(err, "MemberAddAsLearner failed", "name", pending.Name)
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		log.Info("added member as learner", "name", pending.Name)
		etcdMembers = addResp.Members
	}

	// Build --initial-cluster from etcd's view. The new pod's flags MUST
	// match what etcd will report when it tries to join, so use the list
	// from etcd directly rather than the CR set.
	allNames := make([]string, 0, len(etcdMembers))
	for _, m := range etcdMembers {
		name := m.Name
		if name == "" && len(m.PeerURLs) > 0 {
			name = memberNameFromPeerURL(m.PeerURLs[0])
		}
		if name != "" {
			allNames = append(allNames, name)
		}
	}
	sort.Strings(allNames)
	initialCluster := buildInitialCluster(clusterPeerScheme(cluster), allNames, cluster.Name, cluster.Namespace)

	original := pending.DeepCopy()
	pending.Spec.InitialCluster = initialCluster
	// Restore the per-member component label now that the apiserver-
	// assigned name is known, so the CR's label set matches the Pod/PVC
	// that the member controller will create.
	if pending.Labels == nil {
		pending.Labels = map[string]string{}
	}
	for k, v := range memberLabels(cluster.Name, pending.Name) {
		pending.Labels[k] = v
	}
	if err := r.Patch(ctx, pending, client.MergeFrom(original)); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// tryPromoteLearner promotes the first learner found in `listResp` (if
// any). Returns (&Result, nil) when a promotion happened or a learner
// is not yet caught up; (nil, nil) when there is no learner at all.
// Errors out only on apiserver-style MemberPromote failures.
func tryPromoteLearner(
	ctx context.Context,
	etcdClient EtcdClusterClient,
	listResp *clientv3.MemberListResponse,
) (*ctrl.Result, error) {
	log := log.FromContext(ctx)

	for _, m := range listResp.Members {
		if !m.IsLearner {
			continue
		}
		promoteCtx, pCancel := context.WithTimeout(ctx, 10*time.Second)
		_, perr := etcdClient.MemberPromote(promoteCtx, m.ID)
		pCancel()
		if perr != nil {
			log.Info("learner not yet promotable; will retry",
				"learner_id", fmt.Sprintf("%016x", m.ID), "err", perr.Error())
			return &ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		log.Info("promoted learner", "learner_id", fmt.Sprintf("%016x", m.ID))
		return &ctrl.Result{Requeue: true}, nil
	}
	return nil, nil
}

// findPendingMember returns the first non-bootstrap CR with an empty
// Spec.InitialCluster (the half-baked state between Create and the
// follow-up Patch). Returns nil if none.
func findPendingMember(members []lll.EtcdMember) *lll.EtcdMember {
	for i := range members {
		if !members[i].Spec.Bootstrap && members[i].Spec.InitialCluster == "" {
			return &members[i]
		}
	}
	return nil
}

// hasPendingMember is the boolean form of findPendingMember; used in
// Reconcile to gate the pending-completion fast path.
func hasPendingMember(members []lll.EtcdMember) bool {
	return findPendingMember(members) != nil
}

// promotePendingLearner is the steady-state wrapper around tryPromoteLearner:
// fetches etcd's MemberList, syncs IsVoter status onto each EtcdMember
// CR from that snapshot, and delegates the promotion decision. Only
// the Reconcile loop's current==desired branch uses this — scaleUp shares
// its MemberList snapshot with completePendingMember and calls
// tryPromoteLearner directly.
func (r *EtcdClusterReconciler) promotePendingLearner(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
	etcdClient EtcdClusterClient,
	endpoints []string,
) (*ctrl.Result, error) {
	log := log.FromContext(ctx)
	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	listResp, err := etcdClient.MemberList(listCtx)
	if err != nil {
		log.Error(err, "MemberList failed", "endpoints", endpoints)
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if err := r.syncIsVoter(ctx, cluster, members, listResp); err != nil {
		// Non-fatal: continue toward promotion. Next reconcile retries
		// the sync. The IsVoter writes drive PDB selector membership;
		// stale-by-one-reconcile is bounded and safe.
		log.Error(err, "failed to sync IsVoter status; will retry")
	}
	return tryPromoteLearner(ctx, etcdClient, listResp)
}

// syncIsVoter patches each EtcdMember's Status.IsVoter to match etcd's
// MemberList view (IsLearner=false → IsVoter=true). Idempotent — only
// writes when the value differs. Members not yet visible in etcd's
// snapshot (e.g. a brand-new CR awaiting MemberAddAsLearner) are left
// unchanged. Members in the etcd-side list but with a deletion
// timestamp on the CR side are also skipped — their IsVoter no longer
// matters once the finalizer runs.
//
// Matching is by peer URL rather than etcd Member.Name, because Name is
// only populated by the etcd process after its Pod starts; during the
// post-MemberAdd-pre-Pod-running window, Name is empty but PeerURLs is
// not.
func (r *EtcdClusterReconciler) syncIsVoter(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
	listResp *clientv3.MemberListResponse,
) error {
	if listResp == nil {
		return nil
	}
	voterByURL := map[string]bool{}
	learnerByURL := map[string]bool{}
	for _, em := range listResp.Members {
		for _, u := range em.PeerURLs {
			if em.IsLearner {
				learnerByURL[u] = true
			} else {
				voterByURL[u] = true
			}
		}
	}

	for i := range members {
		m := &members[i]
		if !m.DeletionTimestamp.IsZero() {
			continue
		}
		url := peerURL(clusterPeerScheme(cluster), m.Name, cluster.Name, cluster.Namespace)
		var wantVoter bool
		switch {
		case voterByURL[url]:
			wantVoter = true
		case learnerByURL[url]:
			wantVoter = false
		default:
			continue
		}
		if m.Status.IsVoter == wantVoter {
			continue
		}
		orig := m.DeepCopy()
		m.Status.IsVoter = wantVoter
		if err := r.Status().Patch(ctx, m, client.MergeFrom(orig)); err != nil {
			return err
		}
	}
	return nil
}

func resultOrZero(r *ctrl.Result) ctrl.Result {
	if r == nil {
		return ctrl.Result{}
	}
	return *r
}

// allMembersReady reports whether every member's MemberReady condition is
// True. An empty slice trivially passes — there's nothing to wait on.
func allMembersReady(members []lll.EtcdMember) bool {
	for _, m := range members {
		ready := false
		for _, c := range m.Status.Conditions {
			if c.Type == lll.MemberReady && c.Status == metav1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready {
			return false
		}
	}
	return true
}

// ── Auth ───────────────────────────────────────────────────────────────────

// reconcileAuth provisions the single root user/role and turns on etcd
// authentication once the cluster has converged to a healthy quorum. It is
// idempotent and latches status.authEnabled once auth is on; subsequent calls
// short-circuit. Returns a non-nil *ctrl.Result when the caller should return
// it (transient retry, or a requeue so the next pass dials with credentials).
//
// The etcd user is "root"; its password is read from the user-referenced
// credentials Secret (readRootPassword), never hardcoded. The "root" role is
// built into etcd, so UserAdd + UserGrantRole + AuthEnable is the whole
// provisioning sequence — no RoleAdd.
func (r *EtcdClusterReconciler) reconcileAuth(ctx context.Context, cluster *lll.EtcdCluster, running []lll.EtcdMember) (*ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Gates. Anything that isn't a converged, healthy, auth-wanting cluster
	// that hasn't already had auth enabled is a clean no-op.
	if cluster.Spec.Auth == nil || !cluster.Spec.Auth.Enabled {
		return nil, nil
	}
	if cluster.Status.AuthEnabled {
		return nil, nil
	}
	if cluster.Status.Observed == nil || cluster.Status.ClusterID == "" {
		return nil, nil
	}
	desired := cluster.Status.Observed.Replicas
	current := int32(len(running))
	if desired == 0 || current != desired || !allMembersReady(running) {
		// Not converged yet — wait for a later reconcile.
		return nil, nil
	}

	endpoints := memberEndpoints(clusterClientScheme(cluster), running, cluster.Name, cluster.Namespace)
	tlsCfg, err := buildOperatorTLSConfig(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "cannot build operator TLS config for auth-enable; retrying")
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Dial with whatever resolveEtcdCredentials says — pre-latch that is
	// empty (anonymous), which is correct because auth is not on yet.
	user, pass, _, err := resolveEtcdCredentials(ctx, r.Client, cluster)
	if err != nil {
		log.Error(err, "cannot resolve etcd credentials for auth-enable; retrying")
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	c, err := r.EtcdClientFactory(ctx, endpoints, tlsCfg, user, pass)
	if err != nil {
		log.Error(err, "cannot dial etcd for auth-enable; retrying", "endpoints", endpoints)
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	defer c.Close()

	st, err := c.AuthStatus(ctx)
	if err != nil {
		// On etcd builds that guard AuthStatus behind auth, an anonymous
		// probe against an already-auth-enabled cluster fails demanding
		// credentials. Treat that as "auth is already on" and latch — this
		// also recovers the crash-after-AuthEnable-before-status-write case.
		if isAuthRequiredErr(err) {
			return r.latchAuthEnabled(ctx, cluster)
		}
		log.Error(err, "cannot read etcd auth status; retrying", "endpoints", endpoints)
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if !st.Enabled {
		// The root password comes from the user-referenced Secret. Read it
		// here (not via resolveEtcdCredentials, which is gated on the not-yet-
		// latched status.authEnabled) so UserAdd provisions the same password
		// the operator will later authenticate with.
		rootPassword, err := readRootPassword(ctx, r.Client, cluster)
		if err != nil {
			log.Error(err, "cannot read root credentials secret for auth-enable; retrying")
			return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		if _, err := c.UserAdd(ctx, "root", rootPassword); err != nil && !isAuthAlreadyExists(err) {
			log.Error(err, "cannot create etcd root user; retrying")
			return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		// Granting an already-held role is idempotent in etcd (it returns
		// success, not an error), so any error here is a genuine failure —
		// surface it and retry rather than tolerating a sentinel that etcd
		// never emits.
		if _, err := c.UserGrantRole(ctx, "root", "root"); err != nil {
			log.Error(err, "cannot grant root role to etcd root user; retrying")
			return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		if _, err := c.AuthEnable(ctx); err != nil {
			log.Error(err, "cannot enable etcd auth; retrying")
			return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		log.Info("etcd authentication enabled", "cluster", cluster.Name)
	}

	return r.latchAuthEnabled(ctx, cluster)
}

// latchAuthEnabled records status.authEnabled=true and requeues so the next
// reconcile dials with credentials. A conflicting status write is retried.
func (r *EtcdClusterReconciler) latchAuthEnabled(ctx context.Context, cluster *lll.EtcdCluster) (*ctrl.Result, error) {
	if cluster.Status.AuthEnabled {
		return &ctrl.Result{Requeue: true}, nil
	}
	orig := cluster.DeepCopy()
	cluster.Status.AuthEnabled = true
	if err := r.Status().Patch(ctx, cluster, client.MergeFrom(orig)); err != nil {
		if errors.IsConflict(err) {
			return &ctrl.Result{Requeue: true}, nil
		}
		return nil, err
	}
	return &ctrl.Result{Requeue: true}, nil
}

// etcd returns these as gRPC status errors; clientv3 surfaces the server-side
// message verbatim. We match on the message substring rather than importing
// rpctypes so the operator stays decoupled from the gRPC error wrapping.
func isAuthAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "user name already exists")
}

// isAuthRequiredErr reports whether err is the server demanding credentials —
// i.e. auth is already enabled and an anonymous (no-credential) dial was
// rejected. The matched substrings correspond to etcd's rpctypes errors
// ErrUserEmpty / ErrPermissionDenied (pinned by TestAuthErrorClassification so a
// client-library bump can't silently break detection).
func isAuthRequiredErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "user name is empty") ||
		strings.Contains(msg, "permission denied") ||
		strings.Contains(msg, "insufficient credentials")
}

// isAuthFailedErr reports whether err is the server REJECTING the credentials
// the operator presented (as opposed to demanding credentials it didn't get) —
// the wrong-root-password case, most likely after restoring an auth-enabled
// snapshot with a Secret that doesn't match the snapshot's original password.
// Matches etcd's rpctypes ErrAuthFailed / ErrInvalidAuthToken (pinned by
// TestAuthErrorClassification).
func isAuthFailedErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "authentication failed") ||
		strings.Contains(msg, "invalid auth token")
}

// ── Scale down ───────────────────────────────────────────────────────────

// scaleDown removes the most-recently-created member. With apiserver-
// assigned names there is no ordinal to sort by; we pick by
// CreationTimestamp (newest first) instead, which naturally retires the
// most recently added scale-up step before touching older members.
// Tiebreak by name so two members created in the same second produce a
// deterministic choice.
//
// The 1→0 transition is special: it is a "pause" rather than a removal.
// Instead of Deleting the last EtcdMember CR, we Patch
// Spec.Dormant=true on it. The CR stays alive, the PVC remains owned
// by the same EtcdMember, and the member controller's reconcile loop
// observes the flag and deletes the Pod (only). Resume is handled in
// scaleUp by Patching Spec.Dormant back to false. The CR is never
// deleted across the pause cycle, so no name lookup / Create-by-fixed-
// name / PVC reparenting is needed.
func (r *EtcdClusterReconciler) scaleDown(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
) (ctrl.Result, error) {
	sort.Slice(members, func(i, j int) bool {
		ai, aj := members[i].CreationTimestamp, members[j].CreationTimestamp
		if !ai.Equal(&aj) {
			return ai.After(aj.Time)
		}
		return members[i].Name > members[j].Name
	})
	victim := members[0]

	// Scale-to-zero pause: the 1→0 step does NOT delete the surviving
	// member. Instead, Patch Spec.Dormant=true; the member controller
	// will delete the Pod and leave the PVC owned by the EtcdMember.
	// Resurrection (the 0→1 step in scaleUp) flips Dormant back to
	// false. Keeping the CR across the pause means no Create-by-fixed-
	// name on resume, no PVC reparenting, no cross-resource annotation
	// dance, and no stale status field to sweep.
	if cluster.Status.Observed != nil && cluster.Status.Observed.Replicas == 0 && len(members) == 1 {
		if !victim.Spec.Dormant {
			original := victim.DeepCopy()
			victim.Spec.Dormant = true
			if err := r.Patch(ctx, &victim, client.MergeFrom(original)); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if err := r.Delete(ctx, &victim); err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

// hasPendingBootstrap reports whether the bootstrap seed exists but is
// missing its InitialCluster spec — the state we land in after a crash
// between Create and the InitialCluster patch.
func hasPendingBootstrap(members []lll.EtcdMember) bool {
	for _, m := range members {
		if m.Spec.Bootstrap && m.Spec.InitialCluster == "" {
			return true
		}
	}
	return false
}

// ── Status ───────────────────────────────────────────────────────────────

// updateStatus is called with the full active member list (non-deleted),
// including any dormant member. It extracts the running subset for the
// per-condition accounting and uses the dormant member separately for
// the Paused message's PVC name.
func (r *EtcdClusterReconciler) updateStatus(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
) (ctrl.Result, error) {
	desired := cluster.Status.Observed.Replicas
	running := filterRunningMembers(members)
	dormant := findDormantMember(members)

	ready := int32(0)
	for _, m := range running {
		for _, c := range m.Status.Conditions {
			if c.Type == lll.MemberReady && c.Status == metav1.ConditionTrue {
				ready++
				break
			}
		}
	}

	changed := false
	if cluster.Status.ReadyMembers != ready {
		cluster.Status.ReadyMembers = ready
		changed = true
	}

	// /scale subresource: the VPA admission controller fetches this via
	// Scales().Get() to know which Pods to inject resource recommendations
	// into. clusterLabels stamps LabelCluster on every Pod the operator
	// emits, so the minimal selector keyed on that label matches the
	// whole cluster's Pod set.
	wantSelector := fmt.Sprintf("%s=%s", LabelCluster, cluster.Name)
	if cluster.Status.Selector != wantSelector {
		cluster.Status.Selector = wantSelector
		changed = true
	}

	// "Paused" is a distinct steady state from "Reconciled with N healthy
	// members". An empty cluster cannot serve a single request, so
	// reporting Available=True/QuorumHealthy would mislead anything
	// gating on the Available condition (alerting, GitOps health checks,
	// downstream operators that wait for Available=True). The Paused
	// branch takes precedence over the health switch below and also
	// overrides the Reconciled-Progressing override further down.
	paused := desired == 0
	switch {
	case paused:
		// Three flavours of paused:
		//   - dormant + PVC-backed: a member existed, was paused, its PVC
		//     is preserved; scale-up resumes the same etcd cluster.
		//   - dormant + memory-backed: the CRD's CEL admission rule now
		//     rejects replicas=0 + storage.medium=Memory on Create/Update,
		//     but a cluster paused before the rule was installed (operator
		//     upgrade on a pre-existing dormant memory cluster) still
		//     reaches this branch. The tmpfs went with the Pod and there
		//     is no PVC; the cluster cannot be resumed. The condition is
		//     still Paused (the cluster is paused) but the message must
		//     not claim durability the cluster doesn't have.
		//   - fresh-zero: the user created the cluster with replicas=0
		//     from the start; no member was ever created and there is no
		//     PVC. Don't claim "data is preserved" — there is no data to
		//     preserve, and runbooks/dashboards keyed on the condition
		//     message would mislead.
		var msg string
		switch {
		case dormant != nil && dormant.Spec.Storage.Medium == lll.StorageMediumMemory:
			msg = "cluster is paused (spec.replicas=0); data was on tmpfs and has been lost — recreate the cluster to resume"
		case dormant != nil:
			msg = fmt.Sprintf("cluster is paused (spec.replicas=0); data is preserved on PVC data-%s", dormant.Name)
		default:
			msg = "cluster is paused (spec.replicas=0); no data has been written (cluster never bootstrapped)"
		}
		if setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse, "Paused", msg) {
			changed = true
		}
		if setClusterCondition(cluster, lll.ClusterDegraded, metav1.ConditionFalse, "Paused", "") {
			changed = true
		}
		if setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionFalse, "Paused", "") {
			changed = true
		}
	case ready == desired:
		if setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionTrue, "QuorumHealthy", "All members are ready") {
			changed = true
		}
		if setClusterCondition(cluster, lll.ClusterDegraded, metav1.ConditionFalse, "QuorumHealthy", "") {
			changed = true
		}
	case ready > desired/2:
		msg := fmt.Sprintf("%d/%d members ready", ready, desired)
		if setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionTrue, "QuorumAvailable", msg) {
			changed = true
		}
		if setClusterCondition(cluster, lll.ClusterDegraded, metav1.ConditionTrue, "MembersUnhealthy", msg) {
			changed = true
		}
	default:
		if setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse, "QuorumLost",
			fmt.Sprintf("%d/%d members ready, quorum lost", ready, desired)) {
			changed = true
		}
		if setClusterCondition(cluster, lll.ClusterDegraded, metav1.ConditionTrue, "QuorumLost", "") {
			changed = true
		}
	}

	// Skip the Reconciled-Progressing override when paused — the Paused
	// branch above already set Progressing=False/Paused, which is the
	// truthful signal. Still clear ProgressDeadline so a stale value
	// doesn't trigger a deadline-exceeded escalation on a paused cluster.
	if paused {
		if cluster.Status.ProgressDeadline != nil {
			cluster.Status.ProgressDeadline = nil
			changed = true
		}
	} else if reconciliationComplete(cluster, running) {
		if setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionFalse, "Reconciled",
			"actual state matches status.observed") {
			changed = true
		}
		if cluster.Status.ProgressDeadline != nil {
			cluster.Status.ProgressDeadline = nil
			changed = true
		}
	}

	brokenCount := int32(0)
	for _, m := range running {
		if r.isBroken(m) {
			brokenCount++
		}
	}
	if cluster.Status.BrokenMembers != brokenCount {
		cluster.Status.BrokenMembers = brokenCount
		changed = true
	}

	// Reconcile PDB. Voter count comes from members' Status.IsVoter
	// (written by this controller from etcd's MemberList in
	// promotePendingLearner). On a brand-new cluster pre-bootstrap, no
	// member has IsVoter=true yet — voterCount=0 and no PDB is emitted
	// until the seed reaches its Status.IsVoter=true pre-stamp.
	voterCount := int32(0)
	for _, m := range running {
		if m.Status.IsVoter {
			voterCount++
		}
	}
	if err := r.reconcilePDB(ctx, cluster, voterCount); err != nil {
		log.FromContext(ctx).Error(err, "failed to reconcile PodDisruptionBudget")
		// Non-fatal: status update still runs; next reconcile retries.
	}

	if changed {
		if err := r.Status().Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// pdbMaxUnavailable returns the disruption budget for a cluster with
// the given voting-member count. The formula is (voters-1)/2, Go
// integer-divided so the result auto-floors. Equivalently: at most
// fewer-than-quorum voters may be unavailable. For 1 voter the budget
// is 0 (any disruption is quorum loss); for 3 → 1, 4 → 1, 5 → 2.
func pdbMaxUnavailable(voterCount int32) int32 {
	if voterCount < 1 {
		return 0
	}
	return (voterCount - 1) / 2
}

// reconcilePDB ensures a per-cluster PodDisruptionBudget exists that
// protects voting members. Selector keys on LabelCluster + LabelRole=
// RoleVoter; the member controller stamps that role label onto Pods
// whose owning EtcdMember has Status.IsVoter=true.
//
// When voterCount is 0 (pre-bootstrap, paused, or wedged), the PDB is
// deleted: there are no Pods to protect and a stale PDB selector
// against missing labels is at best confusing, at worst (with a
// non-zero MaxUnavailable from a prior state) misleading.
func (r *EtcdClusterReconciler) reconcilePDB(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	voterCount int32,
) error {
	nsName := types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}
	pdb := &policyv1.PodDisruptionBudget{}
	getErr := r.Get(ctx, nsName, pdb)

	if voterCount == 0 {
		if errors.IsNotFound(getErr) {
			return nil
		}
		if getErr != nil {
			return getErr
		}
		return r.Delete(ctx, pdb)
	}

	max := intstr.FromInt32(pdbMaxUnavailable(voterCount))
	wantSelector := &metav1.LabelSelector{
		MatchLabels: map[string]string{
			LabelCluster: cluster.Name,
			LabelRole:    RoleVoter,
		},
	}

	if errors.IsNotFound(getErr) {
		pdbLabels, pdbAnnotations := applyAdditionalMetadata(clusterLabels(cluster.Name), cluster.Spec.AdditionalMetadata)
		fresh := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:        cluster.Name,
				Namespace:   cluster.Namespace,
				Labels:      pdbLabels,
				Annotations: pdbAnnotations,
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MaxUnavailable: &max,
				Selector:       wantSelector,
			},
		}
		if err := controllerutil.SetControllerReference(cluster, fresh, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, fresh)
	}
	if getErr != nil {
		return getErr
	}

	// Patch only if maxUnavailable diverged. Selector is invariant by
	// construction; if a future change wanted a different selector,
	// PDB Selector is immutable on the apiserver side anyway, so the
	// correct path would be Delete+Create rather than Patch.
	currentMax := int32(-1)
	if pdb.Spec.MaxUnavailable != nil {
		currentMax = int32(pdb.Spec.MaxUnavailable.IntValue())
	}
	if currentMax == pdbMaxUnavailable(voterCount) {
		return nil
	}
	orig := pdb.DeepCopy()
	pdb.Spec.MaxUnavailable = &max
	return r.Patch(ctx, pdb, client.MergeFrom(orig))
}

// ── cert-manager Certificates ────────────────────────────────────────────

// reconcileTLSCertificates emits cert-manager.io/v1 Certificate resources
// for each role configured under spec.tls.{client,peer}.certManager.
// Resulting Secrets carry the kubernetes.io/tls shape (tls.crt + tls.key
// + ca.crt) and are mounted into the etcd Pods by the existing BYO code
// path — buildPod and buildOperatorTLSConfig consume the Secret by name
// regardless of source.
//
// No-op when the cluster has no cert-manager-driven TLS configured. We
// use unstructured.Unstructured to avoid a dependency on the cert-manager
// Go module; the Certificate shape is stable and well-documented at
// https://cert-manager.io/docs/usage/certificate/.
// hasCertManagerTLS reports whether either TLS subtree on the cluster
// requests operator-driven cert-manager issuance. Used by the Reconcile
// gate to decide whether the missing-cert-manager terminal path applies.
func hasCertManagerTLS(cluster *lll.EtcdCluster) bool {
	if cluster == nil || cluster.Spec.TLS == nil {
		return false
	}
	if c := cluster.Spec.TLS.Client; c != nil && c.CertManager != nil {
		return true
	}
	if p := cluster.Spec.TLS.Peer; p != nil && p.CertManager != nil {
		return true
	}
	return false
}

func (r *EtcdClusterReconciler) reconcileTLSCertificates(ctx context.Context, cluster *lll.EtcdCluster) error {
	// Callers (Reconcile) have already filtered out the
	// "needs cert-manager but it's missing" case via hasCertManagerTLS +
	// r.CertManagerAvailable. Anything that reaches this function either
	// doesn't use cert-manager (no-op) or has cert-manager available.
	if cluster.Spec.TLS == nil {
		return nil
	}

	domain := r.ClusterDomain
	if domain == "" {
		domain = "cluster.local"
	}

	if c := cluster.Spec.TLS.Client; c != nil && c.CertManager != nil {
		// Server cert (and its issuer's CA, exposed via ca.crt).
		if err := r.ensureCertificate(ctx, cluster, certificateSpec{
			name:        cluster.Name + "-server",
			secretName:  cluster.Name + "-server-tls",
			commonName:  cluster.Name + "-server",
			dnsNames:    serverCertDNSNames(cluster, domain),
			ipAddresses: []string{"127.0.0.1"},
			usages:      []string{"server auth", "client auth", "digital signature", "key encipherment"},
			issuerRef:   c.CertManager.ServerIssuerRef,
		}); err != nil {
			return err
		}
		// Operator-client cert — only when mTLS is selected.
		if c.CertManager.OperatorClientIssuerRef != nil {
			if err := r.ensureCertificate(ctx, cluster, certificateSpec{
				name:       cluster.Name + "-operator-client",
				secretName: cluster.Name + "-operator-client-tls",
				commonName: cluster.Name + "-operator-client",
				usages:     []string{"client auth", "digital signature", "key encipherment"},
				issuerRef:  *c.CertManager.OperatorClientIssuerRef,
			}); err != nil {
				return err
			}
		}
	}

	if p := cluster.Spec.TLS.Peer; p != nil && p.CertManager != nil {
		if err := r.ensureCertificate(ctx, cluster, certificateSpec{
			name:       cluster.Name + "-peer",
			secretName: cluster.Name + "-peer-tls",
			commonName: cluster.Name + "-peer",
			dnsNames:   peerCertDNSNames(cluster, domain),
			usages:     []string{"server auth", "client auth", "digital signature", "key encipherment"},
			issuerRef:  p.CertManager.IssuerRef,
		}); err != nil {
			return err
		}
	}

	return nil
}

// certificateSpec collects the fields of a cert-manager Certificate we
// actually populate. Keeps reconcileTLSCertificates readable and lets the
// construction helper stay one place rather than three.
type certificateSpec struct {
	name        string
	secretName  string
	commonName  string
	dnsNames    []string
	ipAddresses []string
	usages      []string
	issuerRef   lll.IssuerReference
}

// ensureCertificate Creates a cert-manager.io/v1 Certificate at the
// operator's desired shape if one doesn't already exist; takes no
// action if it does. Owner-ref points at the EtcdCluster so the
// Certificate (and through it the Secret cert-manager produces) GCs
// on cluster delete. Unstructured-based to keep us free of the
// cert-manager Go module dep — the underlying CRD shape has been
// stable since cert-manager v1.0.
//
// Create-once rather than Create-or-Patch: the operator never has a
// legitimate reason to mutate an existing Certificate. spec.tls is
// CEL-immutable post-create; the SAN list is derived from immutable
// identifiers (cluster name, namespace, cluster-domain); the issuer
// is locked the same way. Reconciling drift on every loop would fight
// cert-manager's webhook over its defaulted optional fields
// (revisionHistoryLimit, privateKey.rotationPolicy, …) — MergeFrom-
// based patches null them out, cert-manager re-defaults them, the
// audit log fills up. The simpler invariant is: we own the shape at
// creation, after that the resource is cert-manager's territory.
//
// A future operator version that needs to evolve emitted Certificate
// shape (e.g. new SAN policy) should ship a one-off migration step
// distinct from steady-state reconcile.
func (r *EtcdClusterReconciler) ensureCertificate(ctx context.Context, cluster *lll.EtcdCluster, spec certificateSpec) error {
	gvk := schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
	}

	existing := &unstructured.Unstructured{}
	existing.SetGroupVersionKind(gvk)
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: spec.name}, existing)
	switch {
	case err == nil:
		// Already created in a previous reconcile (or by the same
		// reconcile that's now retrying). Leave it alone.
		return nil
	case !errors.IsNotFound(err):
		return err
	}

	cert := &unstructured.Unstructured{}
	cert.SetGroupVersionKind(gvk)
	cert.SetName(spec.name)
	cert.SetNamespace(cluster.Namespace)
	if err := controllerutil.SetControllerReference(cluster, cert, r.Scheme); err != nil {
		return err
	}

	issuerRefMap := map[string]any{
		"name":  spec.issuerRef.Name,
		"group": "cert-manager.io",
	}
	if spec.issuerRef.Kind != "" {
		issuerRefMap["kind"] = spec.issuerRef.Kind
	} else {
		issuerRefMap["kind"] = "Issuer"
	}

	// privateKey.{algorithm,size} pinned to RSA-2048: matches cert-
	// manager's own default today, but pinning shields us against a
	// future cert-manager default flip (e.g. to ECDSA) that some etcd
	// peers might not negotiate.
	desiredSpec := map[string]any{
		"secretName": spec.secretName,
		"commonName": spec.commonName,
		"issuerRef":  issuerRefMap,
		"privateKey": map[string]any{
			"algorithm": "RSA",
			"size":      int64(2048),
		},
		"usages": toAnySlice(spec.usages),
	}
	if len(spec.dnsNames) > 0 {
		desiredSpec["dnsNames"] = toAnySlice(spec.dnsNames)
	}
	if len(spec.ipAddresses) > 0 {
		desiredSpec["ipAddresses"] = toAnySlice(spec.ipAddresses)
	}
	cert.Object["spec"] = desiredSpec

	return r.Create(ctx, cert)
}

func toAnySlice(in []string) []any {
	out := make([]any, len(in))
	for i, s := range in {
		out[i] = s
	}
	return out
}

// serverCertDNSNames builds the SAN list for the server cert. The
// wildcard short and FQDN forms cover both the operator's per-pod-DNS
// dials and etcd's own grpc-gateway loopback. The cluster-domain is
// passed in from the operator's --cluster-domain flag (default
// "cluster.local"; Cozystack uses "cozy.local").
func serverCertDNSNames(cluster *lll.EtcdCluster, clusterDomain string) []string {
	wild := fmt.Sprintf("*.%s.%s.svc", cluster.Name, cluster.Namespace)
	svc := fmt.Sprintf("%s.%s.svc", cluster.Name, cluster.Namespace)
	clientSvc := fmt.Sprintf("%s-client.%s.svc", cluster.Name, cluster.Namespace)
	return []string{
		wild,
		wild + "." + clusterDomain,
		svc,
		svc + "." + clusterDomain,
		clientSvc,
		clientSvc + "." + clusterDomain,
		"localhost",
	}
}

// peerCertDNSNames builds the peer-cert SAN list. The FQDN-wildcard
// form is load-bearing: etcd's peer-mTLS verifier reverse-DNS-looks-up
// the connecting peer's source IP and the resulting PTR is the
// fully-qualified pod hostname (`<pod>.<service>.<ns>.svc.<cluster-domain>`).
func peerCertDNSNames(cluster *lll.EtcdCluster, clusterDomain string) []string {
	wild := fmt.Sprintf("*.%s.%s.svc", cluster.Name, cluster.Namespace)
	return []string{wild, wild + "." + clusterDomain}
}

// ── Services ─────────────────────────────────────────────────────────────

func (r *EtcdClusterReconciler) ensureServices(ctx context.Context, cluster *lll.EtcdCluster) error {
	// Headless service — provides per-pod DNS for peer discovery.
	if err := r.ensureService(ctx, cluster, cluster.Name, corev1.ServiceSpec{
		ClusterIP:                corev1.ClusterIPNone,
		PublishNotReadyAddresses: true,
		Selector:                 map[string]string{LabelCluster: cluster.Name},
		Ports: []corev1.ServicePort{
			{Name: "client", Port: 2379},
			{Name: "peer", Port: 2380},
		},
	}); err != nil {
		return err
	}

	// Client service — stable endpoint for applications.
	return r.ensureService(ctx, cluster, cluster.Name+"-client", corev1.ServiceSpec{
		Selector: map[string]string{LabelCluster: cluster.Name},
		Ports: []corev1.ServicePort{
			{Name: "client", Port: 2379},
		},
	})
}

// ensureService creates the Service if absent and reconciles drift on the
// fields the operator actually owns: Selector, PublishNotReadyAddresses,
// and the named Ports it requires. Everything else (labels, annotations,
// user-added ports, type) is preserved — admission webhooks routinely
// inject these and we don't want to fight them.
func (r *EtcdClusterReconciler) ensureService(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	name string,
	desired corev1.ServiceSpec,
) error {
	svc := &corev1.Service{}
	err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, svc)
	if errors.IsNotFound(err) {
		svcLabels, svcAnnotations := applyAdditionalMetadata(clusterLabels(cluster.Name), cluster.Spec.AdditionalMetadata)
		svc = &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:        name,
				Namespace:   cluster.Namespace,
				Labels:      svcLabels,
				Annotations: svcAnnotations,
			},
			Spec: desired,
		}
		if err := controllerutil.SetControllerReference(cluster, svc, r.Scheme); err != nil {
			return err
		}
		return r.Create(ctx, svc)
	}
	if err != nil {
		return err
	}

	// Drift reconcile on owned fields only.
	original := svc.DeepCopy()
	changed := false

	if !equalSelectors(svc.Spec.Selector, desired.Selector) {
		svc.Spec.Selector = desired.Selector
		changed = true
	}
	if svc.Spec.PublishNotReadyAddresses != desired.PublishNotReadyAddresses {
		svc.Spec.PublishNotReadyAddresses = desired.PublishNotReadyAddresses
		changed = true
	}
	// Required ports must be present with correct values. Extra ports the
	// user has added stay.
	for _, want := range desired.Ports {
		if upsertPortByName(&svc.Spec.Ports, want) {
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return r.Patch(ctx, svc, client.MergeFrom(original))
}

func equalSelectors(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

// upsertPortByName ensures `want` is present in `ports`, identified by Name.
// Existing entries with the same Name are replaced if any of Port, Protocol,
// or TargetPort differ. Returns true when the slice was modified.
func upsertPortByName(ports *[]corev1.ServicePort, want corev1.ServicePort) bool {
	for i, p := range *ports {
		if p.Name != want.Name {
			continue
		}
		if p.Port == want.Port && p.Protocol == want.Protocol && p.TargetPort == want.TargetPort {
			return false
		}
		(*ports)[i] = want
		return true
	}
	*ports = append(*ports, want)
	return true
}

// ── Manager wiring ───────────────────────────────────────────────────────

func (r *EtcdClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.EtcdClientFactory == nil {
		r.EtcdClientFactory = DefaultEtcdClientFactory
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&lll.EtcdCluster{}).
		Owns(&lll.EtcdMember{}).
		Owns(&corev1.Service{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		Complete(r)
}

// ── Deadline handling ────────────────────────────────────────────────────

// handleDeadlineExceeded is the terminal-error path. The operator stops acting
// and waits for user intervention. The shape of "intervention" depends on
// whether the cluster ever bootstrapped:
//
//  1. Before the cluster has formed (clusterID==""): the existing
//     EtcdMembers' pods have an --initial-cluster flag baked into them, and
//     etcd refuses to form unless every bootstrapping member shares an
//     identical value. There's no safe in-place recovery — the only way out
//     is to delete the EtcdCluster and recreate it. This branch surfaces a
//     BootstrapFailed condition and stops.
//
//  2. After bootstrap: the cluster is healthy, the failed reconcile only
//     left some half-done state (e.g. a scale-up that couldn't schedule).
//     The user's spec edit is treated as the intervention — when spec
//     stops matching observed, we snapshot the new spec and resume. Until
//     that happens we sit in DeadlineExceeded.
//
// We never auto-pivot during bootstrap, and never silently in steady state.
func (r *EtcdClusterReconciler) handleDeadlineExceeded(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	now metav1.Time,
) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	if cluster.Status.ClusterID == "" {
		// Bootstrap terminal state. Idempotent: only persist if conditions
		// actually changed; once parked, return ctrl.Result{} and let the
		// watch wake us up if the user deletes (or edits, which won't
		// recover but will at least re-trigger reconcile).
		changed := setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionFalse, "BootstrapFailed",
			"bootstrap deadline exceeded; delete the cluster and recreate to recover")
		changed = setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse, "BootstrapFailed",
			fmt.Sprintf("could not bootstrap %d-member cluster within deadline", cluster.Status.Observed.Replicas)) || changed
		if changed {
			log.Error(nil, "bootstrap deadline exceeded; delete and recreate to recover",
				"observed", cluster.Status.Observed)
			if err := r.Status().Update(ctx, cluster); err != nil {
				if errors.IsConflict(err) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if specEqualsObserved(cluster) {
		// Steady-state terminal state. Same idempotency: write only if
		// changed, no requeue.
		changed := setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionFalse, "DeadlineExceeded",
			"deadline exceeded; edit spec to retry, or delete the cluster")
		changed = setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionFalse, "DeadlineExceeded",
			fmt.Sprintf("could not reach target %+v within deadline", cluster.Status.Observed)) || changed
		if changed {
			log.Error(nil, "deadline exceeded; awaiting spec update",
				"observed", cluster.Status.Observed)
			if err := r.Status().Update(ctx, cluster); err != nil {
				if errors.IsConflict(err) {
					return ctrl.Result{Requeue: true}, nil
				}
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	// Spec has been edited since the deadline expired — treat that as the
	// user's intervention and resume.
	log.Info("deadline exceeded but spec was updated; retrying with new target",
		"observed", cluster.Status.Observed, "spec", cluster.Spec)
	snapshotSpecIntoObserved(cluster)
	setProgressDeadline(cluster, now)
	setClusterCondition(cluster, lll.ClusterProgressing, metav1.ConditionTrue, "RetryAfterDeadline",
		"adopting updated spec after previous deadline")
	if err := r.Status().Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{Requeue: true}, nil
}

// ── Locking-pattern helpers ──────────────────────────────────────────────

func snapshotSpecIntoObserved(cluster *lll.EtcdCluster) {
	replicas := int32(3)
	if cluster.Spec.Replicas != nil {
		replicas = *cluster.Spec.Replicas
	}
	cluster.Status.Observed = &lll.ObservedClusterSpec{
		Replicas:                  replicas,
		Version:                   cluster.Spec.Version,
		Storage:                   cluster.Spec.Storage,
		Resources:                 cluster.Spec.Resources,
		Affinity:                  cluster.Spec.Affinity,
		TopologySpreadConstraints: cluster.Spec.TopologySpreadConstraints,
	}
}

func specEqualsObserved(cluster *lll.EtcdCluster) bool {
	if cluster.Status.Observed == nil {
		return false
	}
	specReplicas := int32(3)
	if cluster.Spec.Replicas != nil {
		specReplicas = *cluster.Spec.Replicas
	}
	o := cluster.Status.Observed
	return o.Replicas == specReplicas &&
		o.Version == cluster.Spec.Version &&
		o.Storage.Size.Cmp(cluster.Spec.Storage.Size) == 0 &&
		o.Storage.Medium == cluster.Spec.Storage.Medium &&
		equality.Semantic.DeepEqual(o.Resources, cluster.Spec.Resources) &&
		equality.Semantic.DeepEqual(o.Affinity, cluster.Spec.Affinity) &&
		equality.Semantic.DeepEqual(o.TopologySpreadConstraints, cluster.Spec.TopologySpreadConstraints)
}

func reconciliationComplete(cluster *lll.EtcdCluster, members []lll.EtcdMember) bool {
	if cluster.Status.Observed == nil {
		return false
	}
	if int32(len(members)) != cluster.Status.Observed.Replicas {
		return false
	}
	// ClusterID is required only when there should be members. A paused
	// or fresh-at-zero cluster has nothing to latch a ClusterID against
	// (there is no etcd process running), and waiting for one would
	// prevent the spec-change-adoption path from ever firing when the
	// user scales replicas back up from 0.
	if cluster.Status.Observed.Replicas > 0 && cluster.Status.ClusterID == "" {
		return false
	}
	for _, m := range members {
		ready := false
		for _, c := range m.Status.Conditions {
			if c.Type == lll.MemberReady && c.Status == metav1.ConditionTrue {
				ready = true
				break
			}
		}
		if !ready {
			return false
		}
	}
	return true
}

func setProgressDeadline(cluster *lll.EtcdCluster, now metav1.Time) {
	secs := DefaultProgressDeadlineSeconds
	if cluster.Spec.ProgressDeadlineSeconds != nil {
		secs = *cluster.Spec.ProgressDeadlineSeconds
	}
	deadline := metav1.NewTime(now.Add(time.Duration(secs) * time.Second))
	cluster.Status.ProgressDeadline = &deadline
}

func deadlineExpired(cluster *lll.EtcdCluster, now metav1.Time) bool {
	if cluster.Status.ProgressDeadline == nil {
		return false
	}
	return !now.Before(cluster.Status.ProgressDeadline)
}

// setClusterCondition writes a condition stamped with the cluster's current
// Generation as ObservedGeneration. Returns true if anything actually
// changed; callers can skip the Status().Update when nothing did.
func setClusterCondition(cluster *lll.EtcdCluster, condType string, status metav1.ConditionStatus, reason, msg string) bool {
	want := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: cluster.Generation,
	}
	for _, existing := range cluster.Status.Conditions {
		if existing.Type == want.Type {
			if existing.Status == want.Status &&
				existing.Reason == want.Reason &&
				existing.Message == want.Message &&
				existing.ObservedGeneration == want.ObservedGeneration {
				return false
			}
			break
		}
	}
	setCondition(&cluster.Status.Conditions, want)
	return true
}
