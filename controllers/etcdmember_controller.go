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
	"crypto/tls"
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// EtcdMemberReconciler reconciles an EtcdMember object.
type EtcdMemberReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	EtcdClientFactory EtcdClientFactory

	// OperatorImage is the operator's own image; the restore agent runs from
	// it as an initContainer on the bootstrap seed Pod.
	OperatorImage string
}

//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdmembers,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdmembers/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdmembers/finalizers,verbs=update
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdclusters,verbs=get
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;patch;delete
//+kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

func (r *EtcdMemberReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	member := &lll.EtcdMember{}
	if err := r.Get(ctx, req.NamespacedName, member); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !member.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, member)
	}

	if !controllerutil.ContainsFinalizer(member, MemberFinalizer) {
		controllerutil.AddFinalizer(member, MemberFinalizer)
		if err := r.Update(ctx, member); err != nil {
			return ctrl.Result{}, err
		}
	}

	// The cluster controller creates this CR before it knows the peer URL
	// it'll register with etcd (apiserver fills GenerateName in the Create
	// response), so Spec.InitialCluster is populated in a follow-up Patch.
	// Until that patch lands, the pod has no valid --initial-cluster flag
	// to start with — wait. Finalizer was added above so deletion mid-flight
	// still triggers MemberRemove cleanup.
	if member.Spec.InitialCluster == "" {
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	// Dormant gate: the cluster controller flips Spec.Dormant=true on the
	// surviving member during a scale-to-zero pause. Delete the Pod (the
	// PVC stays owned by this EtcdMember). When the cluster controller
	// flips Dormant back to false on resume, the next reconcile recreates
	// the Pod against the existing PVC and etcd resumes from its data
	// dir with the same ClusterID and member ID. PVC stays put — no
	// reparenting, no adoption, no cross-resource coordination.
	if member.Spec.Dormant {
		// Capture before clear: ensurePodAbsent mutates
		// member.Status.PodName in-memory. Comparing against the
		// previous in-memory value (which started life as the stored
		// value) tells us whether we need a Status update without an
		// extra apiserver Get round-trip.
		prevPodName := member.Status.PodName
		if err := r.ensurePodAbsent(ctx, member); err != nil {
			log.Error(err, "failed to delete pod for dormant member")
			return ctrl.Result{}, err
		}
		// Persist the cleared PodName + a Paused condition. updateStatus()
		// is for the running flow (reads pod, derives Ready); for dormant
		// we know the answer directly. Idempotent — setMemberCondition
		// short-circuits when the condition already matches.
		changed := prevPodName != ""
		if setMemberCondition(member, lll.MemberReady, metav1.ConditionFalse, "Paused",
			"member is paused (spec.dormant=true); pod deleted, PVC preserved") {
			changed = true
		}
		if changed {
			if err := r.Status().Update(ctx, member); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// Memory-backed pod-loss detection: a tmpfs emptyDir dies with the Pod,
	// so a missing Pod whose UID we previously recorded means the data is
	// permanently lost. Re-creating a fresh Pod here would fail to rejoin
	// the cluster (this member's ID is in raft state but the WAL is empty).
	// Self-delete instead — the finalizer runs MemberRemove against peers
	// (quorum permitting) and the cluster controller's gap-fill creates a
	// replacement EtcdMember with a fresh member ID.
	//
	// If quorum is already lost (e.g. >floor((n-1)/2) memory members lost
	// simultaneously), MemberRemove will fail and the member stays in
	// Terminating until quorum returns. That is the correct outcome for
	// memory-backed clusters: the cluster is dead and the user has to
	// recreate.
	if member.Spec.Storage.Medium == lll.StorageMediumMemory && member.Status.PodUID != "" {
		lost, err := r.memoryMemberPodLost(ctx, member)
		if err != nil {
			return ctrl.Result{}, err
		}
		if lost {
			log.Info("memory-backed member's pod is gone; deleting member for replacement",
				"previousPodUID", member.Status.PodUID)
			if err := r.Delete(ctx, member); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	if err := r.ensurePVC(ctx, member); err != nil {
		log.Error(err, "failed to ensure PVC")
		return ctrl.Result{}, err
	}

	if err := r.ensurePod(ctx, member); err != nil {
		log.Error(err, "failed to ensure Pod")
		return ctrl.Result{}, err
	}

	return r.updateStatus(ctx, member)
}

// memoryMemberPodLost reports whether the Pod we previously recorded a
// UID for is gone (NotFound) or has been replaced by a Pod with a
// different UID. A Terminating Pod with the recorded UID still counts as
// present — wait for kubelet GC before declaring loss.
func (r *EtcdMemberReconciler) memoryMemberPodLost(ctx context.Context, member *lll.EtcdMember) (bool, error) {
	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: member.Name}, pod)
	if errors.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return string(pod.UID) != member.Status.PodUID, nil
}

// ── Deletion ─────────────────────────────────────────────────────────────

func (r *EtcdMemberReconciler) handleDeletion(ctx context.Context, member *lll.EtcdMember) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(member, MemberFinalizer) {
		return ctrl.Result{}, nil
	}

	log := log.FromContext(ctx)

	clusterDeleting := false
	cluster := &lll.EtcdCluster{}
	err := r.Get(ctx, types.NamespacedName{
		Namespace: member.Namespace,
		Name:      member.Spec.ClusterName,
	}, cluster)
	switch {
	case errors.IsNotFound(err):
		clusterDeleting = true
	case err != nil:
		// Don't silently treat a transient apiserver error as "cluster
		// alive". Propagate so controller-runtime retries with backoff.
		return ctrl.Result{}, fmt.Errorf("get owner EtcdCluster: %w", err)
	case !cluster.DeletionTimestamp.IsZero():
		clusterDeleting = true
	}

	if !clusterDeleting {
		if err := r.removeMemberFromEtcd(ctx, cluster, member); err != nil {
			log.Error(err, "failed to remove member from etcd, will retry")
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
	}

	controllerutil.RemoveFinalizer(member, MemberFinalizer)
	if err := r.Update(ctx, member); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// removeMemberFromEtcd handles three cases:
//
//  1. status.MemberID is set — call MemberRemove(id) directly.
//  2. status.MemberID is empty (e.g. the pod never finished bootstrapping
//     before the user scaled back down) — list the etcd cluster, find a
//     member matching this name and call MemberRemove on its ID. Etcd may
//     have an entry from MemberAdd even if the pod never started, so we must
//     still clean it up.
//  3. The member is not in etcd's list at all — treat as already gone and
//     return nil so the finalizer can be removed.
func (r *EtcdMemberReconciler) removeMemberFromEtcd(ctx context.Context, cluster *lll.EtcdCluster, member *lll.EtcdMember) error {
	memberList := &lll.EtcdMemberList{}
	if err := r.List(ctx, memberList,
		client.InNamespace(member.Namespace),
		client.MatchingLabels{LabelCluster: member.Spec.ClusterName},
	); err != nil {
		return err
	}

	// Only dial peers that aren't themselves being deleted. Endpoints of
	// a Terminating pod can hang the client until our 10s deadline and
	// don't help us anyway.
	scheme := clusterClientScheme(cluster)
	var endpoints []string
	otherMembers := 0
	for _, m := range filterActiveMembers(memberList.Items) {
		if m.Name == member.Name {
			continue
		}
		otherMembers++
		if m.Status.PodName != "" {
			// Build each peer's endpoint with that peer's OWN Service name:
			// an adopted peer resolves under the legacy headless Service even
			// when `member` (the one being removed) is native, or vice versa.
			endpoints = append(endpoints, clientURL(scheme, m.Name, memberServiceName(&m), member.Namespace))
		}
	}
	if len(endpoints) == 0 {
		if otherMembers > 0 {
			// Other members exist on the CR side but none have a PodName —
			// transient state (controller restart hasn't repopulated status,
			// or pods haven't been created yet). Don't silently skip
			// MemberRemove; return an error so the finalizer requeues.
			return fmt.Errorf("no reachable peers (%d other member(s) have empty PodName); will retry", otherMembers)
		}
		// Genuinely the last member — nothing to remove from.
		return nil
	}

	tlsCfg, err := buildOperatorTLSConfig(ctx, r.Client, cluster)
	if err != nil {
		return fmt.Errorf("build operator TLS config: %w", err)
	}
	user, pass, _, err := resolveEtcdCredentials(ctx, r.Client, cluster)
	if err != nil {
		return fmt.Errorf("resolve etcd credentials: %w", err)
	}
	etcdClient, err := r.EtcdClientFactory(ctx, endpoints, tlsCfg, user, pass)
	if err != nil {
		return err
	}
	defer etcdClient.Close()

	rmCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	id, err := r.resolveMemberID(rmCtx, member, etcdClient)
	if err != nil {
		return err
	}
	if id == 0 {
		// Not found in etcd. Already gone.
		return nil
	}

	_, err = etcdClient.MemberRemove(rmCtx, id)
	return err
}

// resolveMemberID returns the etcd member ID this EtcdMember refers to. If
// status.MemberID was populated we trust it; otherwise we ask etcd to look up
// the member by name. Returns 0 if not found in etcd.
func (r *EtcdMemberReconciler) resolveMemberID(
	ctx context.Context,
	member *lll.EtcdMember,
	c EtcdClusterClient,
) (uint64, error) {
	if member.Status.MemberID != "" {
		id, err := strconv.ParseUint(member.Status.MemberID, 16, 64)
		if err != nil {
			return 0, fmt.Errorf("parse memberID %q: %w", member.Status.MemberID, err)
		}
		return id, nil
	}

	resp, err := c.MemberList(ctx)
	if err != nil {
		return 0, err
	}
	for _, m := range resp.Members {
		if m.Name == member.Name {
			return m.ID, nil
		}
		// Match by peer URL too — etcd may have a member added via MemberAdd
		// whose Name is empty until it joins.
		expected := peerURL(memberPeerScheme(member), member.Name, memberServiceName(member), member.Namespace)
		for _, p := range m.PeerURLs {
			if p == expected {
				return m.ID, nil
			}
		}
	}
	return 0, nil
}

// ── PVC ──────────────────────────────────────────────────────────────────

func (r *EtcdMemberReconciler) ensurePVC(ctx context.Context, member *lll.EtcdMember) error {
	// Memory-backed members keep their data in a tmpfs emptyDir bound to
	// the Pod, not in a PVC. Skip both the create and the ownership check;
	// the Pod's volume source is the only consumer of Spec.Storage in
	// that mode.
	if member.Spec.Storage.Medium == lll.StorageMediumMemory {
		member.Status.PVCName = ""
		return nil
	}

	pvcName := "data-" + member.Name
	pvc := &corev1.PersistentVolumeClaim{}
	err := r.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: pvcName}, pvc)
	if err == nil {
		// The PVC stays owned by this EtcdMember across pause/resume
		// (the cluster controller flips Spec.Dormant rather than
		// deleting the member CR), so ownership never moves. If the
		// PVC's controller-owner doesn't match this member's UID, it
		// belongs to something else and we must refuse — silently
		// inheriting another member's data dir would crashloop the
		// pod when etcd notices a removed memberID in the WAL.
		if !pvcOwnedBy(pvc, member) {
			return fmt.Errorf("PVC %q is owned by a different EtcdMember; awaiting GC before reuse", pvcName)
		}
		member.Status.PVCName = pvcName
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	// The PVC is operator-created and operator-owned, so it carries the
	// cluster's additionalMetadata like every other child object (backup
	// tooling and cost-allocation selectors target PVCs specifically).
	pvcLabels, pvcAnnotations := applyAdditionalMetadata(
		memberLabels(member.Spec.ClusterName, member.Name), nil, member.Spec.AdditionalMetadata)
	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        pvcName,
			Namespace:   member.Namespace,
			Labels:      pvcLabels,
			Annotations: pvcAnnotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: member.Spec.Storage.StorageClassName,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: member.Spec.Storage.Size,
				},
			},
		},
	}
	if err := controllerutil.SetControllerReference(member, pvc, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, pvc); err != nil {
		return err
	}
	member.Status.PVCName = pvcName
	return nil
}

// pvcOwnedBy returns true only if the PVC carries an EtcdMember owner
// reference whose UID matches this member — i.e. we created it. PVCs with
// no owner refs, or with owner refs pointing at anything else, are refused.
func pvcOwnedBy(pvc *corev1.PersistentVolumeClaim, member *lll.EtcdMember) bool {
	for _, o := range pvc.OwnerReferences {
		if o.Kind == "EtcdMember" && o.UID == member.UID {
			return true
		}
	}
	return false
}

// podOwnedBy mirrors pvcOwnedBy: true only when the Pod's owner refs
// point at this EtcdMember by UID. Less load-bearing than the PVC
// check (Pod corruption is recoverable; replacing one is cheap), but
// adopting a leftover Pod from a prior cluster generation would leave
// the operator reconciling against a Pod whose spec was written by a
// different controller incarnation. Refuse silent adoption — wait for
// GC and re-create the Pod ourselves.
func podOwnedBy(pod *corev1.Pod, member *lll.EtcdMember) bool {
	for _, o := range pod.OwnerReferences {
		if o.Kind == "EtcdMember" && o.UID == member.UID {
			return true
		}
	}
	return false
}

// ── Pod ──────────────────────────────────────────────────────────────────

func (r *EtcdMemberReconciler) ensurePod(ctx context.Context, member *lll.EtcdMember) error {
	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: member.Name}, pod)
	if err == nil {
		if !podOwnedBy(pod, member) {
			return fmt.Errorf("Pod %q is owned by a different EtcdMember; awaiting GC before reuse", member.Name)
		}
		member.Status.PodName = pod.Name
		member.Status.PodUID = string(pod.UID)
		if err := r.reconcileRoleLabel(ctx, pod, member); err != nil {
			return err
		}
		return nil
	}
	if !errors.IsNotFound(err) {
		return err
	}

	// Gate Pod creation on referenced TLS Secrets actually existing.
	// Without this check the Pod is created but stays in
	// ContainerCreating with FailedMount until the Secret materializes
	// — a confusing failure mode for users who typo a secret name or
	// haven't created the Secret yet. Returning an error here triggers
	// the standard backoff requeue.
	if err := r.checkTLSSecretsAvailable(ctx, member); err != nil {
		return err
	}

	// A seed carrying a restore spec needs the operator image for its restore
	// init container. Refuse to create the Pod with an empty image — an
	// unschedulable/empty-image Pod would brick cluster bootstrap with an
	// opaque failure. Erroring here triggers the standard backoff requeue and
	// surfaces a clear reason.
	if member.Spec.Restore != nil && r.OperatorImage == "" {
		return fmt.Errorf("member %q requests a restore but the operator image is not configured "+
			"(set --operator-image / OPERATOR_IMAGE); refusing to create a seed Pod with an empty restore image", member.Name)
	}

	pod = r.buildPod(member)
	if err := controllerutil.SetControllerReference(member, pod, r.Scheme); err != nil {
		return err
	}
	if err := r.Create(ctx, pod); err != nil {
		return err
	}
	member.Status.PodName = pod.Name
	member.Status.PodUID = string(pod.UID)
	return nil
}

// checkTLSSecretsAvailable verifies that every TLS Secret referenced by
// this member exists. Returns nil for plaintext clusters or when all
// referenced Secrets are present; returns an error (which propagates to a
// requeue) otherwise.
func (r *EtcdMemberReconciler) checkTLSSecretsAvailable(ctx context.Context, member *lll.EtcdMember) error {
	if member.Spec.TLS == nil {
		return nil
	}
	check := func(name string) error {
		sec := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: name}, sec); err != nil {
			return fmt.Errorf("TLS secret %s/%s: %w", member.Namespace, name, err)
		}
		return nil
	}
	if member.Spec.TLS.ClientServerSecretRef != nil {
		if err := check(member.Spec.TLS.ClientServerSecretRef.Name); err != nil {
			return err
		}
	}
	if member.Spec.TLS.PeerSecretRef != nil {
		if err := check(member.Spec.TLS.PeerSecretRef.Name); err != nil {
			return err
		}
	}
	return nil
}

// reconcileRoleLabel keeps the Pod's role label in sync with the
// member's Status.IsVoter. The cluster controller is the source of
// truth for IsVoter (written from etcd's MemberList); the member
// controller propagates that single bit to the Pod label so the
// per-cluster PodDisruptionBudget's selector — which keys on
// LabelRole=RoleVoter — protects voters and not learners.
//
// Idempotent: returns nil without a Patch when the label already
// matches the desired state.
func (r *EtcdMemberReconciler) reconcileRoleLabel(ctx context.Context, pod *corev1.Pod, member *lll.EtcdMember) error {
	hasLabel := pod.Labels[LabelRole] == RoleVoter
	want := member.Status.IsVoter
	if hasLabel == want {
		return nil
	}
	orig := pod.DeepCopy()
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	if want {
		pod.Labels[LabelRole] = RoleVoter
	} else {
		delete(pod.Labels, LabelRole)
	}
	return r.Patch(ctx, pod, client.MergeFrom(orig))
}

// ensurePodAbsent is the dormant-state counterpart of ensurePod. It
// deletes the member's Pod if present (PVC stays in place — that's the
// whole point of the pause) and clears Status.PodName. Idempotent: if
// no Pod exists, this is a no-op.
func (r *EtcdMemberReconciler) ensurePodAbsent(ctx context.Context, member *lll.EtcdMember) error {
	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: member.Name}, pod)
	if errors.IsNotFound(err) {
		member.Status.PodName = ""
		member.Status.PodUID = ""
		return nil
	}
	if err != nil {
		return err
	}
	if pod.DeletionTimestamp.IsZero() {
		if err := r.Delete(ctx, pod); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}
	member.Status.PodName = ""
	member.Status.PodUID = ""
	return nil
}

// containerResources returns the resource requirements for the etcd
// container, with a conservative fall-through default when the user
// hasn't set spec.resources on the cluster. The fall-through (100m
// CPU + 128Mi memory requests, no limits) preserves the operator's
// pre-existing default behaviour for clusters that don't opt into
// per-cluster sizing.
//
// The "user set anything" check is structural deep-equality against
// the zero value rather than just `len(Requests) > 0 || len(Limits) > 0`
// — DynamicResourceAllocation's Claims field is the third axis of
// ResourceRequirements and would otherwise be silently swallowed by
// the default fall-through.
func containerResources(member *lll.EtcdMember) corev1.ResourceRequirements {
	if !equality.Semantic.DeepEqual(member.Spec.Resources, corev1.ResourceRequirements{}) {
		return *member.Spec.Resources.DeepCopy()
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("128Mi"),
		},
	}
}

// optionFlags renders the member's typed tuning options (mirrored from
// EtcdCluster.spec.options) as etcd command-line flags. Unset fields emit
// nothing, leaving etcd's built-in defaults in force.
func optionFlags(o *lll.EtcdOptions) []string {
	if o == nil {
		return nil
	}
	var flags []string
	if o.QuotaBackendBytes != nil {
		flags = append(flags, fmt.Sprintf("--quota-backend-bytes=%d", *o.QuotaBackendBytes))
	}
	if o.AutoCompactionMode != "" {
		flags = append(flags, "--auto-compaction-mode="+string(o.AutoCompactionMode))
	}
	if o.AutoCompactionRetention != "" {
		flags = append(flags, "--auto-compaction-retention="+o.AutoCompactionRetention)
	}
	if o.SnapshotCount != nil {
		flags = append(flags, fmt.Sprintf("--snapshot-count=%d", *o.SnapshotCount))
	}
	return flags
}

func (r *EtcdMemberReconciler) buildPod(member *lll.EtcdMember) *corev1.Pod {
	clusterState := "new"
	if !member.Spec.Bootstrap {
		clusterState = "existing"
	}

	clientScheme := memberClientScheme(member)
	peerScheme := memberPeerScheme(member)
	clientTLS := clientScheme == "https"
	peerTLS := peerScheme == "https"
	clientMTLS := clientTLS && member.Spec.TLS != nil && member.Spec.TLS.ClientMTLS

	pAddr := peerURL(peerScheme, member.Name, memberServiceName(member), member.Namespace)
	cAddr := clientURL(clientScheme, member.Name, memberServiceName(member), member.Namespace)

	// Data volume source: tmpfs emptyDir for memory-backed members,
	// PVC otherwise. SizeLimit on the emptyDir caps tmpfs allocation;
	// note that without a container memory limit covering it, tmpfs
	// writes still count against node-level memory rather than the
	// pod's cgroup — production memory clusters should also set
	// resources.limits.memory (tracked in
	// https://github.com/cozystack/etcd-operator/issues/16).
	var dataVolumeSource corev1.VolumeSource
	if member.Spec.Storage.Medium == lll.StorageMediumMemory {
		size := member.Spec.Storage.Size
		dataVolumeSource = corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: &size,
			},
		}
	} else {
		dataVolumeSource = corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "data-" + member.Name,
			},
		}
	}

	labels := memberLabels(member.Spec.ClusterName, member.Name)
	if member.Status.IsVoter {
		// The per-cluster PodDisruptionBudget selects on this label; the
		// cluster controller's reconcilePDB only counts members with
		// Status.IsVoter=true, so labelling at create time keeps the PDB
		// and Pod state consistent in one reconcile rather than two.
		labels[LabelRole] = RoleVoter
	}
	labels, annotations := applyAdditionalMetadata(labels, nil, member.Spec.AdditionalMetadata)

	// Data dir defaults to the volume root; adopted legacy members carry an
	// AnnDataDirSubPath annotation (e.g. "default.etcd") so a replacement
	// Pod resumes from the existing data dir instead of bootstrapping an
	// empty one. memberDataDir validates the annotation and fails closed.
	dataDir := memberDataDir(member)

	cmd := []string{
		"etcd",
		"--name=" + member.Name,
		"--data-dir=" + dataDir,
		"--listen-peer-urls=" + peerScheme + "://0.0.0.0:2380",
		"--listen-client-urls=" + clientScheme + "://0.0.0.0:2379",
		"--advertise-client-urls=" + cAddr,
		"--initial-advertise-peer-urls=" + pAddr,
		"--initial-cluster=" + member.Spec.InitialCluster,
		"--initial-cluster-token=" + member.Spec.ClusterToken,
		"--initial-cluster-state=" + clusterState,
		// Plaintext metrics + /health listener on a separate port,
		// bound to 0.0.0.0 so kubelet HTTPGet probes (which dial the
		// Pod IP, not loopback) and Prometheus-style scrapers can both
		// reach it. Always on, regardless of TLS — Prometheus
		// integrations like cozystack's VMPodScrape target the named
		// "metrics" container port unconditionally.
		"--listen-metrics-urls=http://0.0.0.0:2381",
	}
	cmd = append(cmd, optionFlags(member.Spec.Options)...)

	volumes := []corev1.Volume{{Name: "data", VolumeSource: dataVolumeSource}}
	mounts := []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/etcd"}}

	if clientTLS {
		cmd = append(cmd,
			"--cert-file=/etc/etcd/tls/client/tls.crt",
			"--key-file=/etc/etcd/tls/client/tls.key",
		)
		if clientMTLS {
			cmd = append(cmd,
				"--client-cert-auth=true",
				"--trusted-ca-file=/etc/etcd/tls/client/ca.crt",
			)
		}
		volumes = append(volumes, corev1.Volume{
			Name: "tls-client",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: member.Spec.TLS.ClientServerSecretRef.Name,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "tls-client", MountPath: "/etc/etcd/tls/client", ReadOnly: true,
		})
	}
	if peerTLS {
		cmd = append(cmd,
			"--peer-cert-file=/etc/etcd/tls/peer/tls.crt",
			"--peer-key-file=/etc/etcd/tls/peer/tls.key",
			"--peer-trusted-ca-file=/etc/etcd/tls/peer/ca.crt",
			"--peer-client-cert-auth=true",
		)
		volumes = append(volumes, corev1.Volume{
			Name: "tls-peer",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: member.Spec.TLS.PeerSecretRef.Name,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name: "tls-peer", MountPath: "/etc/etcd/tls/peer", ReadOnly: true,
		})
	}

	// Restore initContainer: when the seed carries a restore spec, populate
	// the data dir from the snapshot before etcd starts. The agent no-ops if
	// the data dir is already initialized, so it's safe across Pod restarts.
	var initContainers []corev1.Container
	if member.Spec.Restore != nil {
		ic, extraVols := restoreInitContainer(member, pAddr, r.OperatorImage)
		initContainers = append(initContainers, ic)
		volumes = append(volumes, extraVols...)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        member.Name,
			Namespace:   member.Namespace,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PodSpec{
			Hostname:                  member.Name,
			Subdomain:                 memberServiceName(member),
			Affinity:                  member.Spec.Affinity,
			TopologySpreadConstraints: member.Spec.TopologySpreadConstraints,
			InitContainers:            initContainers,
			// etcd and the restore agent never call the Kubernetes API, so
			// don't mount a ServiceAccount token into the member Pod (matches
			// the snapshot Job's stance and trims needless attack surface).
			AutomountServiceAccountToken: ptrBool(false),
			SecurityContext: &corev1.PodSecurityContext{
				RunAsNonRoot: ptrBool(true),
				RunAsUser:    ptrInt64(65532),
				RunAsGroup:   ptrInt64(65532),
				FSGroup:      ptrInt64(65532),
				SeccompProfile: &corev1.SeccompProfile{
					Type: corev1.SeccompProfileTypeRuntimeDefault,
				},
			},
			Containers: []corev1.Container{{
				Name:  "etcd",
				Image: fmt.Sprintf("%s:v%s", EtcdImage, member.Spec.Version),
				SecurityContext: &corev1.SecurityContext{
					AllowPrivilegeEscalation: ptrBool(false),
					Capabilities: &corev1.Capabilities{
						Drop: []corev1.Capability{"ALL"},
					},
				},
				Command: cmd,
				Ports: []corev1.ContainerPort{
					{Name: "client", ContainerPort: 2379},
					{Name: "peer", ContainerPort: 2380},
					{Name: "metrics", ContainerPort: 2381},
				},
				VolumeMounts: mounts,
				Resources:    containerResources(member),
				// Liveness MUST NOT require quorum. /health returns non-200
				// on a member that can't reach peers — if we put HTTPGet on
				// /health here, a transient partition cascade-kills every
				// pod and turns a 30s blip into a permanent outage that
				// needs a snapshot restore. TCP on the peer port (2380) is
				// the canonical "etcd process alive" check; quorum-aware
				// signalling stays in the readiness probe.
				LivenessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						TCPSocket: &corev1.TCPSocketAction{
							Port: intstr.FromInt(2380),
						},
					},
					InitialDelaySeconds: 15,
					PeriodSeconds:       10,
					FailureThreshold:    3,
				},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path: "/health",
							Port: intstr.FromInt(2381),
						},
					},
					InitialDelaySeconds: 5,
					PeriodSeconds:       5,
					FailureThreshold:    3,
				},
			}},
			Volumes: volumes,
		},
	}
}

const restoreSrcMountPath = "/restore/src"

// restoreInitContainer builds the initContainer that restores the data dir
// from a snapshot before etcd starts. peerAddr is this member's peer URL; the
// agent feeds it (with the member name / initial-cluster / token) to etcdutl
// so the restored data matches the identity the etcd container will run with.
// For an S3 source the object key is exact (not a prefix); for a PVC source
// the volume is mounted read-only and PVC_SUBPATH points to the snapshot file.
func restoreInitContainer(member *lll.EtcdMember, peerAddr, operatorImage string) (corev1.Container, []corev1.Volume) {
	src := member.Spec.Restore.Source
	env := []corev1.EnvVar{
		{Name: "ETCD_DATA_DIR", Value: "/var/lib/etcd"},
		{Name: "ETCD_MEMBER_NAME", Value: member.Name},
		{Name: "ETCD_INITIAL_CLUSTER", Value: member.Spec.InitialCluster},
		{Name: "ETCD_INITIAL_CLUSTER_TOKEN", Value: member.Spec.ClusterToken},
		{Name: "ETCD_PEER_URLS", Value: peerAddr},
		// The cluster's etcd version, for the agent's version-compat pre-flight
		// (the restored data dir must match the etcd that boots on it).
		{Name: "ETCD_VERSION", Value: member.Spec.Version},
	}
	mounts := []corev1.VolumeMount{{Name: "data", MountPath: "/var/lib/etcd"}}
	var vols []corev1.Volume

	switch {
	case src.S3 != nil:
		s3 := src.S3
		env = append(env,
			corev1.EnvVar{Name: "SNAPSHOT_DEST_KIND", Value: "s3"},
			corev1.EnvVar{Name: "S3_ENDPOINT", Value: s3.Endpoint},
			corev1.EnvVar{Name: "S3_BUCKET", Value: s3.Bucket},
			corev1.EnvVar{Name: "S3_KEY", Value: s3.Key}, // exact object key for restore
			corev1.EnvVar{Name: "S3_REGION", Value: s3.Region},
			corev1.EnvVar{Name: "S3_FORCE_PATH_STYLE", Value: fmt.Sprintf("%t", s3.ForcePathStyle)},
			corev1.EnvVar{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: s3.CredentialsSecretRef, Key: "AWS_ACCESS_KEY_ID",
			}}},
			corev1.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: s3.CredentialsSecretRef, Key: "AWS_SECRET_ACCESS_KEY",
			}}},
		)
	case src.PVC != nil:
		env = append(env,
			corev1.EnvVar{Name: "SNAPSHOT_DEST_KIND", Value: "pvc"},
			corev1.EnvVar{Name: "PVC_MOUNT_PATH", Value: restoreSrcMountPath},
			corev1.EnvVar{Name: "PVC_SUBPATH", Value: src.PVC.SubPath},
		)
		vols = append(vols, corev1.Volume{
			Name: "restore-src",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: src.PVC.ClaimName,
				ReadOnly:  true,
			}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "restore-src", MountPath: restoreSrcMountPath, ReadOnly: true})
	}

	return corev1.Container{
		Name:    "restore",
		Image:   operatorImage,
		Command: []string{"/manager", "restore-agent"},
		Env:     env,
		SecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptrBool(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
		VolumeMounts: mounts,
		Resources:    restoreAgentResources(),
	}, vols
}

// ── Status ───────────────────────────────────────────────────────────────

func (r *EtcdMemberReconciler) updateStatus(ctx context.Context, member *lll.EtcdMember) (ctrl.Result, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: member.Name}, pod); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}
		return ctrl.Result{}, err
	}

	changed := false
	if member.Status.PodName != pod.Name {
		member.Status.PodName = pod.Name
		changed = true
	}
	if member.Status.PodUID != string(pod.UID) {
		member.Status.PodUID = string(pod.UID)
		changed = true
	}
	// Memory-backed members have no PVC; leave Status.PVCName empty.
	if member.Spec.Storage.Medium != lll.StorageMediumMemory {
		wantPVC := "data-" + member.Name
		if member.Status.PVCName != wantPVC {
			member.Status.PVCName = wantPVC
			changed = true
		}
	}

	// /scale fields: Replicas reflects "Pod exists" (1) or not (0).
	// Selector matches this member's single Pod. Consumed by the PDB
	// controller during expectedPods derivation; not user-facing.
	if member.Status.Replicas != 1 {
		member.Status.Replicas = 1
		changed = true
	}
	wantSelector := fmt.Sprintf("%s=%s,app.kubernetes.io/component=%s", LabelCluster, member.Spec.ClusterName, member.Name)
	if member.Status.Selector != wantSelector {
		member.Status.Selector = wantSelector
		changed = true
	}

	podReady := false
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			podReady = true
			break
		}
	}

	switch {
	case !podReady:
		if setMemberCondition(member, lll.MemberReady, metav1.ConditionFalse, "PodNotReady",
			fmt.Sprintf("pod phase: %s", pod.Status.Phase)) {
			changed = true
		}
	case member.Status.MemberID == "":
		// Pod ready but we haven't matched it to an etcd member yet.
		// Try once, but don't claim Ready=True until we have the ID — the
		// finalizer needs it to perform a clean MemberRemove on scale-down.
		if id, err := r.discoverMemberID(ctx, member); err == nil {
			member.Status.MemberID = fmt.Sprintf("%016x", id)
			changed = true
			if setMemberCondition(member, lll.MemberReady, metav1.ConditionTrue, "PodReady",
				"etcd member is ready") {
				changed = true
			}
		} else {
			if setMemberCondition(member, lll.MemberReady, metav1.ConditionFalse, "DiscoveringMemberID",
				fmt.Sprintf("waiting for memberID: %v", err)) {
				changed = true
			}
		}
	default:
		if setMemberCondition(member, lll.MemberReady, metav1.ConditionTrue, "PodReady",
			"etcd member is ready") {
			changed = true
		}
	}

	if changed {
		if err := r.Status().Update(ctx, member); err != nil {
			return ctrl.Result{}, err
		}
	}

	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// clusterFor fetches the EtcdMember's parent EtcdCluster. The member controller
// needs it to build the operator-side dial config: TLS material
// (buildOperatorTLSConfig) and auth credentials (resolveEtcdCredentials, which
// reads cluster.status.authEnabled).
func (r *EtcdMemberReconciler) clusterFor(ctx context.Context, member *lll.EtcdMember) (*lll.EtcdCluster, error) {
	cluster := &lll.EtcdCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: member.Namespace, Name: member.Spec.ClusterName}, cluster); err != nil {
		return nil, fmt.Errorf("fetch parent cluster %s/%s: %w", member.Namespace, member.Spec.ClusterName, err)
	}
	return cluster, nil
}

// discoverMemberID asks etcd for this member's ID. It prefers asking peers
// rather than the member's own pod: if this member is crashlooping (PVC
// corruption, OOM, etc.) its own etcd never responds, but the rest of the
// cluster knows perfectly well what its ID is. Falling back to self last
// keeps single-node bootstrap working.
//
// Peers are filtered to ones already observed Ready (i.e. voters in etcd
// terms). Including a still-learner peer in the endpoint list lets
// clientv3 balance MemberList to it and get back "rpc not supported for
// learner"; with a 5s context budget and connect-retries, that can wedge
// the discovery even when a voter peer is also present.
func (r *EtcdMemberReconciler) discoverMemberID(ctx context.Context, member *lll.EtcdMember) (uint64, error) {
	memberList := &lll.EtcdMemberList{}
	if err := r.List(ctx, memberList,
		client.InNamespace(member.Namespace),
		client.MatchingLabels{LabelCluster: member.Spec.ClusterName},
	); err != nil {
		return 0, err
	}

	scheme := memberClientScheme(member)
	var endpoints []string
	for _, m := range filterActiveMembers(memberList.Items) {
		if m.Name == member.Name {
			continue
		}
		if m.Status.PodName == "" {
			continue
		}
		if !m.Status.IsVoter {
			// Learners reject MemberList with "rpc not supported for
			// learner"; routing through them would just consume our
			// context budget.
			continue
		}
		// Each peer's endpoint uses that peer's own Service name (adopted
		// peers resolve under the legacy headless Service); self below uses
		// this member's.
		endpoints = append(endpoints, clientURL(scheme, m.Name, memberServiceName(&m), member.Namespace))
	}
	// Self is a *fallback*, not an always-on endpoint: dial our own etcd
	// only when no voter peer is available (single-node bootstrap, or no
	// other voter Ready yet). When this member is itself a still-learner
	// being discovered, appending self alongside a voter lets clientv3's
	// balancer round-robin MemberList onto our own etcd, which rejects it
	// with "rpc not supported for learner" — wedging discovery even though
	// a voter was in the list. Mirrors memberEndpoints' voter-or-fallback.
	if len(endpoints) == 0 {
		endpoints = append(endpoints, clientURL(scheme, member.Name, memberServiceName(member), member.Namespace))
	}

	// Build the operator-side dial config. Only TLS clusters need the parent
	// EtcdCluster fetched: auth requires client TLS (CEL-enforced), which is
	// propagated to member.Spec.TLS.ClientServerSecretRef, so a member with
	// no client TLS can never have auth enabled — its dial is plaintext and
	// anonymous, exactly as before. When TLS is set we fetch the cluster once
	// and derive both the TLS config and the credentials from it (after auth
	// is enabled the voter peers we dial here reject anonymous access).
	var tlsCfg *tls.Config
	var user, pass string
	if member.Spec.TLS != nil && member.Spec.TLS.ClientServerSecretRef != nil {
		cluster, err := r.clusterFor(ctx, member)
		if err != nil {
			return 0, err
		}
		tlsCfg, err = buildOperatorTLSConfig(ctx, r.Client, cluster)
		if err != nil {
			return 0, err
		}
		user, pass, _, err = resolveEtcdCredentials(ctx, r.Client, cluster)
		if err != nil {
			return 0, err
		}
	}
	c, err := r.EtcdClientFactory(ctx, endpoints, tlsCfg, user, pass)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	listCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	resp, err := c.MemberList(listCtx)
	if err != nil {
		return 0, err
	}
	expectedPeer := peerURL(memberPeerScheme(member), member.Name, memberServiceName(member), member.Namespace)
	for _, m := range resp.Members {
		if m.Name == member.Name {
			return m.ID, nil
		}
		// Window between MemberAddAsLearner and the new etcd reporting its
		// name: peer URL is the only stable identifier we have.
		for _, p := range m.PeerURLs {
			if p == expectedPeer {
				return m.ID, nil
			}
		}
	}
	return 0, fmt.Errorf("member %q not found in etcd member list", member.Name)
}

// ── Manager wiring ───────────────────────────────────────────────────────

func (r *EtcdMemberReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.EtcdClientFactory == nil {
		r.EtcdClientFactory = DefaultEtcdClientFactory
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&lll.EtcdMember{}).
		Owns(&corev1.Pod{}).
		Owns(&corev1.PersistentVolumeClaim{}).
		Complete(r)
}
