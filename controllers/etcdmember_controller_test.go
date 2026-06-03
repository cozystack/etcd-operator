/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"context"
	"crypto/tls"
	"errors"
	"strings"
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// TestRemoveMemberFromEtcd_FallbackByName covers reviewer issue #1: when a
// member's status.MemberID is empty (e.g. the pod never became Ready before
// scale-down), the finalizer must still find the etcd-side member by name and
// MemberRemove it. Without this, scale-up + immediate scale-down orphans the
// MemberAdd in etcd's member list.
func TestRemoveMemberFromEtcd_FallbackByName(t *testing.T) {
	ctx := context.Background()

	// Cluster has two existing members and the never-Ready new member.
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	existing0 := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{PodName: "test-0"},
	}
	existing1 := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", Labels: memberLabels("test", "test-1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{PodName: "test-1"},
	}
	// test-2 was just MemberAdd'd to etcd but the pod never came up, so
	// MemberID is still empty on the CR.
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-2", Namespace: "ns",
			Labels:     memberLabels("test", "test-2"),
			Finalizers: []string{MemberFinalizer},
		},
		Spec: lll.EtcdMemberSpec{ClusterName: "test"},
		// No MemberID, no PodName.
	}

	// Etcd reflects the situation: 3 members, with test-2 added by URL but
	// no Name yet (etcd populates Name only after the joiner reports in).
	const orphanID uint64 = 0xc0ffee
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: orphanID, Name: "", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}},
	)

	c, _ := newTestClient(t, cluster, existing0, existing1, victim)
	r := &EtcdMemberReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(fe),
	}

	if err := r.removeMemberFromEtcd(ctx, cluster, victim); err != nil {
		t.Fatalf("removeMemberFromEtcd: %v", err)
	}

	if len(fe.removeCalls) != 1 || fe.removeCalls[0] != orphanID {
		t.Fatalf("expected MemberRemove(0x%x); got %v", orphanID, fe.removeCalls)
	}
}

// TestRemoveMemberFromEtcd_PeerWithEmptyPodNameRetries covers reviewer
// issue #3: if other members exist on the CR side but none have a PodName
// recorded yet (transient state, controller restart), removeMemberFromEtcd
// must NOT silently return nil — that would let the finalizer clear and
// orphan the etcd-side member. Return an error so we retry.
func TestRemoveMemberFromEtcd_PeerWithEmptyPodNameRetries(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	// test-0 has no PodName recorded — simulating mid-bootstrap or
	// controller-restart staleness.
	other := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		// Status.PodName intentionally empty.
	}
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", Labels: memberLabels("test", "test-1"), Finalizers: []string{MemberFinalizer}},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{MemberID: "abc"},
	}

	c, _ := newTestClient(t, cluster, other, victim)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	err := r.removeMemberFromEtcd(ctx, cluster, victim)
	if err == nil {
		t.Fatalf("expected error when peers exist on CR side but none have PodName set")
	}
	if len(fe.removeCalls) != 0 {
		t.Fatalf("MemberRemove should not be called when endpoints are empty; got %v", fe.removeCalls)
	}
}

// TestHandleDeletion_TransientGetErrorReturnsError covers reviewer issue
// #4: a non-NotFound error from getting the owner EtcdCluster must NOT be
// silently treated as "cluster alive" — that risks repeatedly firing
// MemberRemove against a cluster we can't actually introspect. Propagate
// the error so controller-runtime applies backoff.
func TestHandleDeletion_TransientGetErrorReturnsError(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-0", Namespace: "ns",
			Labels:            memberLabels("test", "test-0"),
			Finalizers:        []string{MemberFinalizer},
			DeletionTimestamp: &now,
		},
		Spec:   lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{MemberID: "abc"},
	}
	base, _ := newTestClient(t, cluster, victim)
	c := &erroringGetClient{
		Client:     base,
		failOnKind: "EtcdCluster",
		err:        errors.New("apiserver flaked"),
	}
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	if _, err := r.handleDeletion(ctx, victim); err == nil {
		t.Fatalf("expected error from handleDeletion on transient cluster Get error")
	}
	// Finalizer should still be in place — we didn't get a clean shutdown.
	mustGet(t, base, "test-0", "ns", victim)
	if !containsFinalizer(victim, MemberFinalizer) {
		t.Fatalf("finalizer was removed despite Get error")
	}
}

func containsFinalizer(m *lll.EtcdMember, name string) bool {
	for _, f := range m.Finalizers {
		if f == name {
			return true
		}
	}
	return false
}

// TestRemoveMemberFromEtcd_NotFoundIsClean: if the member doesn't appear in
// etcd's list at all, treat it as already gone — no error. Otherwise, the
// finalizer would block forever waiting for an etcd-side state that never
// materialises.
func TestRemoveMemberFromEtcd_NotFoundIsClean(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	existing0 := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{PodName: "test-0"},
	}
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-9", Namespace: "ns", Labels: memberLabels("test", "test-9"), Finalizers: []string{MemberFinalizer}},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
	}

	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
	)

	c, _ := newTestClient(t, cluster, existing0, victim)
	r := &EtcdMemberReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(fe),
	}

	if err := r.removeMemberFromEtcd(ctx, cluster, victim); err != nil {
		t.Fatalf("removeMemberFromEtcd should not error when member already gone: %v", err)
	}
	if len(fe.removeCalls) != 0 {
		t.Fatalf("expected no MemberRemove call, got %v", fe.removeCalls)
	}
}

// TestEnsurePVC_RefusesStaleOwner covers reviewer issue #2: a same-named PVC
// owned by a now-deleted EtcdMember (pending GC) must NOT be bound to the new
// EtcdMember of the same name. Reusing the prior data dir would crashloop the
// new pod (etcd sees a memberID the cluster has just removed).
func TestEnsurePVC_RefusesStaleOwner(t *testing.T) {
	ctx := context.Background()

	staleUID := types.UID("old-uid")
	freshUID := types.UID("fresh-uid")

	stalePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-1",
			Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2",
				Kind:       "EtcdMember",
				Name:       "test-1",
				UID:        staleUID,
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}

	freshMember := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", UID: freshUID, Labels: memberLabels("test", "test-1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}

	c, _ := newTestClient(t, freshMember, stalePVC)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	err := r.ensurePVC(ctx, freshMember)
	if err == nil {
		t.Fatalf("ensurePVC should refuse to reuse a PVC owned by a stale EtcdMember")
	}
}

// TestEnsurePVC_RefusesPVCWithNoOwnerRefs: a PVC with no owner refs is no
// longer "adopted" — the only legitimate adoption flow (operator-managed
// scale-to-zero hand-off) is tracked separately and will use explicit
// re-parenting. Until then, ensurePVC accepts only PVCs we created.
func TestEnsurePVC_RefusesPVCWithNoOwnerRefs(t *testing.T) {
	ctx := context.Background()
	prePVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-0",
			Namespace: "ns",
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: types.UID("uid"), Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}

	c, _ := newTestClient(t, member, prePVC)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePVC(ctx, member); err == nil {
		t.Fatalf("ensurePVC should refuse to adopt a PVC with no owner references")
	}
}

// TestEnsurePVC_RefusesPVCOwnedByOther: a PVC owned by some other resource
// (a leaked owner ref, a Pod, another operator's CR) must not be silently
// mounted by an etcd member. ensurePVC errors out so the user can untangle
// the conflict explicitly.
func TestEnsurePVC_RefusesPVCOwnedByOther(t *testing.T) {
	ctx := context.Background()
	otherPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-0",
			Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       "some-other-pod",
				UID:        types.UID("other-uid"),
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: types.UID("uid"), Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}

	c, _ := newTestClient(t, member, otherPVC)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePVC(ctx, member); err == nil {
		t.Fatalf("ensurePVC should refuse to mount a PVC owned by something other than this EtcdMember")
	}
}

// TestEnsurePVC_AcceptsOwnPVC: when the existing PVC's owner ref UID matches
// the current EtcdMember (a normal restart-after-pod-delete situation), reuse
// is fine.
func TestEnsurePVC_AcceptsOwnPVC(t *testing.T) {
	ctx := context.Background()

	uid := types.UID("same-uid")
	ownPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "data-test-0",
			Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2",
				Kind:       "EtcdMember",
				Name:       "test-0",
				UID:        uid,
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: uid, Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}

	c, _ := newTestClient(t, member, ownPVC)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePVC(ctx, member); err != nil {
		t.Fatalf("ensurePVC for own PVC: %v", err)
	}
	if member.Status.PVCName != "data-test-0" {
		t.Fatalf("PVCName not recorded: %q", member.Status.PVCName)
	}
}

// TestEnsurePVC_AppliesStorageClassName covers the wiring of
// spec.storage.storageClassName onto the created PVC. The propagation
// is what makes per-cluster StorageClass overrides actually take effect
// — a member spec with the field set must result in
// PersistentVolumeClaim.spec.storageClassName carrying the same value.
func TestEnsurePVC_AppliesStorageClassName(t *testing.T) {
	ctx := context.Background()
	sc := "replicated"
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: types.UID("mu"), Labels: memberLabels("test", "test-0")},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi"), StorageClassName: &sc},
			InitialCluster: "x", ClusterToken: "test",
		},
	}
	c, _ := newTestClient(t, member)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePVC(ctx, member); err != nil {
		t.Fatalf("ensurePVC: %v", err)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "data-test-0"}, pvc); err != nil {
		t.Fatalf("PVC not created: %v", err)
	}
	if pvc.Spec.StorageClassName == nil {
		t.Fatalf("PVC.spec.storageClassName is nil; want %q", sc)
	}
	if *pvc.Spec.StorageClassName != sc {
		t.Fatalf("PVC.spec.storageClassName = %q; want %q", *pvc.Spec.StorageClassName, sc)
	}
}

// TestEnsurePVC_NilStorageClassNamePassesNil covers the negative case:
// when spec.storage.storageClassName is unset on the member, the
// resulting PVC must have a nil StorageClassName (which means "use the
// namespace's default"), NOT an empty string (which means "explicitly
// no dynamic provisioning"). Conflating the two would silently disable
// dynamic provisioning on clusters that didn't ask for it.
func TestEnsurePVC_NilStorageClassNamePassesNil(t *testing.T) {
	ctx := context.Background()
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: types.UID("mu"), Labels: memberLabels("test", "test-0")},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi")}, // StorageClassName left nil.
			InitialCluster: "x", ClusterToken: "test",
		},
	}
	c, _ := newTestClient(t, member)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePVC(ctx, member); err != nil {
		t.Fatalf("ensurePVC: %v", err)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "data-test-0"}, pvc); err != nil {
		t.Fatalf("PVC not created: %v", err)
	}
	if pvc.Spec.StorageClassName != nil {
		t.Fatalf("PVC.spec.storageClassName must be nil when not set on the member; got %q", *pvc.Spec.StorageClassName)
	}
}

// TestEnsurePod_RefusesStaleOwner mirrors TestEnsurePVC_RefusesStaleOwner:
// a same-named Pod owned by a now-deleted EtcdMember (pending GC) must
// not be adopted by the fresh EtcdMember of the same name. Less severe
// than the PVC case (Pod state is replaceable), but the operator-managed
// lifecycle would otherwise reconcile a Pod whose spec was written by a
// different controller generation.
func TestEnsurePod_RefusesStaleOwner(t *testing.T) {
	ctx := context.Background()
	stalePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-1", Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2",
				Kind:       "EtcdMember",
				Name:       "test-1",
				UID:        types.UID("old-uid"),
			}},
		},
	}
	freshMember := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", UID: types.UID("fresh-uid"), Labels: memberLabels("test", "test-1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}
	c, _ := newTestClient(t, freshMember, stalePod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePod(ctx, freshMember); err == nil {
		t.Fatalf("ensurePod should refuse to adopt a Pod owned by a stale EtcdMember UID")
	}
}

// TestEnsurePod_RefusesPodWithNoOwnerRefs: a same-named Pod with no
// owner refs (manually created, leaked from a previous incarnation
// without GC catching the dependent) is refused — the operator's
// reconcile flow assumes it created and controls the Pod, and adoption
// would silently bind unowned state.
func TestEnsurePod_RefusesPodWithNoOwnerRefs(t *testing.T) {
	ctx := context.Background()
	prePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
	}
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: types.UID("uid"), Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}
	c, _ := newTestClient(t, member, prePod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePod(ctx, member); err == nil {
		t.Fatalf("ensurePod should refuse to adopt a Pod with no owner references")
	}
}

// TestEnsurePod_RefusesPodOwnedByOther: a Pod owned by some other
// resource (different Kind, different operator's CR, a deployment-
// style controller) must not be adopted. Symmetric with the PVC
// other-owner refusal.
func TestEnsurePod_RefusesPodOwnedByOther(t *testing.T) {
	ctx := context.Background()
	otherPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-0", Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1",
				Kind:       "ReplicaSet",
				Name:       "some-rs",
				UID:        types.UID("other-uid"),
			}},
		},
	}
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: types.UID("uid"), Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}
	c, _ := newTestClient(t, member, otherPod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePod(ctx, member); err == nil {
		t.Fatalf("ensurePod should refuse to adopt a Pod owned by something other than this EtcdMember")
	}
}

// TestEnsurePod_AcceptsOwnPod: when the existing Pod's owner ref UID
// matches the current EtcdMember (the normal post-create steady-state
// case), reuse is fine and Status.PodName / Status.PodUID get recorded.
func TestEnsurePod_AcceptsOwnPod(t *testing.T) {
	ctx := context.Background()

	uid := types.UID("same-uid")
	ownPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-0", Namespace: "ns", UID: types.UID("pod-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2",
				Kind:       "EtcdMember",
				Name:       "test-0",
				UID:        uid,
			}},
		},
	}
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: uid, Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}
	c, _ := newTestClient(t, member, ownPod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePod(ctx, member); err != nil {
		t.Fatalf("ensurePod for own Pod: %v", err)
	}
	if member.Status.PodName != "test-0" {
		t.Fatalf("PodName not recorded: %q", member.Status.PodName)
	}
	if member.Status.PodUID != "pod-uid" {
		t.Fatalf("PodUID not recorded: %q", member.Status.PodUID)
	}
}

// TestUpdateStatus_NoMemberIDKeepsReadyFalse covers reviewer issue #3: a pod
// that's PodReady but without a populated MemberID must not be reported as
// MemberReady=True. Otherwise the cluster controller can count it toward
// readyMembers and a deletion in this window leaves an etcd-side orphan.
func TestUpdateStatus_NoMemberIDKeepsReadyFalse(t *testing.T) {
	ctx := context.Background()

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{readyPodCondition()},
		},
	}

	c, _ := newTestClient(t, member, pod)
	// Etcd is reachable but it doesn't yet know about test-0 by name —
	// simulating the brief window between the pod becoming Ready and etcd
	// propagating the joiner's identity.
	fe := newFakeEtcd(0xdeadbeef) // empty members list
	r := &EtcdMemberReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(fe),
	}

	if _, err := r.updateStatus(ctx, member); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}

	mustGet(t, c, "test-0", "ns", member)
	var ready *metav1.Condition
	for i := range member.Status.Conditions {
		if member.Status.Conditions[i].Type == lll.MemberReady {
			ready = &member.Status.Conditions[i]
		}
	}
	if ready == nil {
		t.Fatalf("no MemberReady condition")
	}
	if ready.Status != metav1.ConditionFalse {
		t.Fatalf("Ready=%v, want False (no memberID populated yet)", ready.Status)
	}
	if ready.Reason != "DiscoveringMemberID" {
		t.Fatalf("Reason=%q, want DiscoveringMemberID", ready.Reason)
	}
	if member.Status.MemberID != "" {
		t.Fatalf("MemberID populated unexpectedly: %q", member.Status.MemberID)
	}
}

// TestUpdateStatus_PopulatesMemberIDAndFlipsReady: the happy path — etcd
// knows about this member by name, we record the hex ID and flip Ready=True.
func TestUpdateStatus_PopulatesMemberIDAndFlipsReady(t *testing.T) {
	ctx := context.Background()

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{readyPodCondition()},
		},
	}

	c, _ := newTestClient(t, member, pod)
	const wantID uint64 = 0xae36f238164a08ad
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: wantID, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
	)
	r := &EtcdMemberReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(fe),
	}

	if _, err := r.updateStatus(ctx, member); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}

	mustGet(t, c, "test-0", "ns", member)
	if member.Status.MemberID != "ae36f238164a08ad" {
		t.Fatalf("MemberID = %q, want ae36f238164a08ad", member.Status.MemberID)
	}
	var ready *metav1.Condition
	for i := range member.Status.Conditions {
		if member.Status.Conditions[i].Type == lll.MemberReady {
			ready = &member.Status.Conditions[i]
		}
	}
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("Ready condition = %+v, want True", ready)
	}
}

// TestRemoveMemberFromEtcd_LastMemberIsNoOp: if no other members exist (the
// cluster is being torn down or this is genuinely the last member), the
// finalizer can't reach a peer to call MemberRemove. Don't block — return
// nil so the finalizer can clear and the resource gets GC'd.
func TestRemoveMemberFromEtcd_LastMemberIsNoOp(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-0", Namespace: "ns",
			Labels:     memberLabels("test", "test-0"),
			Finalizers: []string{MemberFinalizer},
		},
		Spec:   lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{MemberID: "abc"},
	}

	c, _ := newTestClient(t, cluster, victim)
	r := &EtcdMemberReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead)), // never reached
	}

	if err := r.removeMemberFromEtcd(ctx, cluster, victim); err != nil {
		t.Fatalf("removeMemberFromEtcd should be a no-op when no peers reachable; got %v", err)
	}
}

// TestRemoveMemberFromEtcd_FactoryError: if we can build no etcd client at
// all, the finalizer must surface the error and retry rather than silently
// removing the finalizer (which would leave the etcd-side member orphaned).
func TestRemoveMemberFromEtcd_FactoryError(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	otherMember := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", Labels: memberLabels("test", "test-1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{PodName: "test-1"},
	}
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0"), Finalizers: []string{MemberFinalizer}},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{MemberID: "abc"},
	}

	c, _ := newTestClient(t, cluster, otherMember, victim)
	r := &EtcdMemberReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: failingFactory(errors.New("dial timeout")),
	}

	err := r.removeMemberFromEtcd(ctx, cluster, victim)
	if err == nil {
		t.Fatalf("expected error from removeMemberFromEtcd when factory fails")
	}
}

// TestUpdateStatus_PodNotReadyKeepsReadyFalse covers the symmetric case to
// #3: if the pod itself isn't Ready, MemberReady should be False with reason
// PodNotReady (and we should never even attempt MemberID discovery).
func TestUpdateStatus_PodNotReadyKeepsReadyFalse(t *testing.T) {
	ctx := context.Background()

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type: corev1.PodReady, Status: corev1.ConditionFalse,
				LastTransitionTime: metav1.Now(),
			}},
		},
	}

	c, _ := newTestClient(t, member, pod)
	// Factory should never be called when pod isn't Ready; using a failing
	// factory asserts that.
	r := &EtcdMemberReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: failingFactory(errors.New("must not be called")),
	}

	if _, err := r.updateStatus(ctx, member); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}

	mustGet(t, c, "test-0", "ns", member)
	var ready *metav1.Condition
	for i := range member.Status.Conditions {
		if member.Status.Conditions[i].Type == lll.MemberReady {
			ready = &member.Status.Conditions[i]
		}
	}
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "PodNotReady" {
		t.Fatalf("Ready condition = %+v, want False/PodNotReady", ready)
	}
}

// TestBuildPod_LivenessIsNotQuorumAware covers B1: the liveness probe must
// not require quorum. A liveness HTTPGet on /health kills every member
// during a transient partition. The check is a TCP socket on the peer
// port — process-alive only.
func TestBuildPod_LivenessIsNotQuorumAware(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17"},
	})
	lp := pod.Spec.Containers[0].LivenessProbe
	if lp == nil {
		t.Fatalf("missing liveness probe entirely")
	}
	if lp.HTTPGet != nil {
		t.Fatalf("liveness probe must not use HTTPGet (would require quorum on /health); got HTTPGet=%+v", lp.HTTPGet)
	}
	if lp.TCPSocket == nil {
		t.Fatalf("liveness probe should use TCPSocket")
	}
	if lp.TCPSocket.Port.IntValue() != 2380 {
		t.Fatalf("liveness TCP port = %d, want 2380 (peer)", lp.TCPSocket.Port.IntValue())
	}
}

// TestBuildPod_AppliesSchedulingAndMetadata covers the additionalMetadata,
// affinity, and topologySpreadConstraints passthrough: buildPod must stamp
// the Pod with the member's scheduling fields and merge the extra
// labels/annotations, without letting a user-supplied label shadow an
// operator-owned one.
func TestBuildPod_AppliesSchedulingAndMetadata(t *testing.T) {
	r := &EtcdMemberReconciler{}
	aff := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				TopologyKey: "kubernetes.io/hostname",
			}},
		},
	}
	tsc := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.DoNotSchedule,
	}}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName:               "test",
			Version:                   "3.5.17",
			Affinity:                  aff,
			TopologySpreadConstraints: tsc,
			AdditionalMetadata: &lll.AdditionalMetadata{
				Labels: map[string]string{
					"cozystack.io/tenant": "foo",
					// Attempt to shadow an operator-owned label: must be ignored.
					"app.kubernetes.io/managed-by": "evil",
				},
				Annotations: map[string]string{"example.com/note": "bar"},
			},
		},
	})

	if !equality.Semantic.DeepEqual(pod.Spec.Affinity, aff) {
		t.Errorf("pod affinity = %+v, want %+v", pod.Spec.Affinity, aff)
	}
	if !equality.Semantic.DeepEqual(pod.Spec.TopologySpreadConstraints, tsc) {
		t.Errorf("pod topologySpreadConstraints = %+v, want %+v", pod.Spec.TopologySpreadConstraints, tsc)
	}
	if got := pod.Labels["cozystack.io/tenant"]; got != "foo" {
		t.Errorf("additional label not merged: cozystack.io/tenant = %q, want foo", got)
	}
	if got := pod.Labels["app.kubernetes.io/managed-by"]; got != "etcd-operator" {
		t.Errorf("operator-owned label was overridden: app.kubernetes.io/managed-by = %q, want etcd-operator", got)
	}
	if got := pod.Annotations["example.com/note"]; got != "bar" {
		t.Errorf("additional annotation not merged: example.com/note = %q, want bar", got)
	}
}

// TestBuildPod_NoAdditionalMetadataLeavesAnnotationsNil guards that a member
// without additionalMetadata produces a Pod with no annotations (rather than
// an empty non-nil map), keeping the no-op path clean.
func TestBuildPod_NoAdditionalMetadataLeavesAnnotationsNil(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17"},
	})
	if pod.Annotations != nil {
		t.Errorf("expected nil annotations, got %+v", pod.Annotations)
	}
	if pod.Spec.Affinity != nil {
		t.Errorf("expected nil affinity, got %+v", pod.Spec.Affinity)
	}
}

// TestRemoveMemberFromEtcd_SkipsDeletingPeers covers B4: when other members
// are themselves Terminating, removeMemberFromEtcd must not dial their
// (about-to-vanish) endpoints. Filter active members first.
func TestRemoveMemberFromEtcd_SkipsDeletingPeers(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	healthy := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{PodName: "test-0"},
	}
	dying := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-2", Namespace: "ns", Labels: memberLabels("test", "test-2"),
			DeletionTimestamp: &now, Finalizers: []string{MemberFinalizer}},
		Spec:   lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{PodName: "test-2"},
	}
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", Labels: memberLabels("test", "test-1"),
			Finalizers: []string{MemberFinalizer}},
		Spec:   lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{MemberID: "0000000000000002", PodName: "test-1"},
	}

	c, _ := newTestClient(t, cluster, healthy, dying, victim)

	// Record the endpoints the factory was called with.
	var seenEndpoints []string
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0x1, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0x2, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: 0x3, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}},
	)
	factory := func(_ context.Context, eps []string, _ *tls.Config, _, _ string) (EtcdClusterClient, error) {
		seenEndpoints = append([]string(nil), eps...)
		return fe, nil
	}
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factory}

	if err := r.removeMemberFromEtcd(ctx, cluster, victim); err != nil {
		t.Fatalf("removeMemberFromEtcd: %v", err)
	}
	for _, ep := range seenEndpoints {
		if ep == clientURL("http", "test-2", "test", "ns") {
			t.Fatalf("dialed a Terminating peer (test-2); endpoints were %v", seenEndpoints)
		}
	}
	if len(seenEndpoints) != 1 || seenEndpoints[0] != clientURL("http", "test-0", "test", "ns") {
		t.Fatalf("expected dial only against test-0; got %v", seenEndpoints)
	}
}

// TestDiscoverMemberID_FallsBackToPeers covers B5: if the member's own pod
// is crashlooping, peer members still know its ID. discoverMemberID must
// dial peers, not just self.
func TestDiscoverMemberID_FallsBackToPeers(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	now := metav1.Now()
	peer := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{
			PodName: "test-0", MemberID: "0000000000000001",
			IsVoter:    true,
			Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
		},
	}
	target := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", Labels: memberLabels("test", "test-1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
	}

	c, _ := newTestClient(t, cluster, peer, target)

	// Factory inspects endpoints; if the first is self URL, error; if peer
	// URL is present, succeed with a fake that knows about target.
	const wantID uint64 = 0xfeedface
	fe := newFakeEtcd(0xdead,
		&etcdserverpb.Member{ID: 0x1, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: wantID, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
	)
	var capturedEndpoints []string
	factory := func(_ context.Context, eps []string, _ *tls.Config, _, _ string) (EtcdClusterClient, error) {
		capturedEndpoints = append([]string(nil), eps...)
		return fe, nil
	}
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factory}

	id, err := r.discoverMemberID(ctx, target)
	if err != nil {
		t.Fatalf("discoverMemberID: %v", err)
	}
	if id != wantID {
		t.Fatalf("id = %x, want %x", id, wantID)
	}
	// Assert at least one peer URL is in the endpoint list (and not just self).
	wantPeer := clientURL("http", "test-0", "test", "ns")
	hasPeer := false
	for _, ep := range capturedEndpoints {
		if ep == wantPeer {
			hasPeer = true
			break
		}
	}
	if !hasPeer {
		t.Fatalf("discoverMemberID must include peer endpoints; got %v", capturedEndpoints)
	}
}

// TestDiscoverMemberID_ExcludesNonVoterPeers pins the fix for issue #12,
// tightened in the PDB PR: when one peer is a voter (Status.IsVoter=true)
// and another is still a learner (IsVoter=false), the endpoint list
// passed to clientv3 must include ONLY the voter. Including the learner
// lets clientv3 round-robin MemberList to it and get back "rpc not
// supported for learner", which wedges discovery during scale-up. The
// original filter keyed on the Ready condition, which a learner can
// also satisfy once its Pod is up; Status.IsVoter is the precise signal.
//
// Without the filter, this test sees both peers' URLs in the endpoint
// list (and the operator wedges in production).
func TestDiscoverMemberID_ExcludesNonVoterPeers(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	voter := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-voter", Namespace: "ns", Labels: memberLabels("test", "test-voter")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{
			PodName: "test-voter", MemberID: "0000000000000001",
			IsVoter:    true,
			Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
		},
	}
	learner := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-learner", Namespace: "ns", Labels: memberLabels("test", "test-learner")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{
			PodName:    "test-learner", // No MemberID, no Ready=True.
			Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionFalse, Reason: "DiscoveringMemberID", LastTransitionTime: now}},
		},
	}
	target := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-target", Namespace: "ns", Labels: memberLabels("test", "test-target")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
	}
	c, _ := newTestClient(t, cluster, voter, learner, target)

	const wantID uint64 = 0xfeedface
	fe := newFakeEtcd(0xdead,
		&etcdserverpb.Member{ID: 0x1, Name: "test-voter", PeerURLs: []string{peerURL("http", "test-voter", "test", "ns")}},
		&etcdserverpb.Member{ID: wantID, Name: "test-target", PeerURLs: []string{peerURL("http", "test-target", "test", "ns")}},
	)
	var captured []string
	factory := func(_ context.Context, eps []string, _ *tls.Config, _, _ string) (EtcdClusterClient, error) {
		captured = append([]string(nil), eps...)
		return fe, nil
	}
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factory}

	if _, err := r.discoverMemberID(ctx, target); err != nil {
		t.Fatalf("discoverMemberID: %v", err)
	}
	learnerURL := clientURL("http", "test-learner", "test", "ns")
	for _, ep := range captured {
		if ep == learnerURL {
			t.Fatalf("discoverMemberID must not pass the non-Ready learner's URL to clientv3; got %v", captured)
		}
	}
	voterURL := clientURL("http", "test-voter", "test", "ns")
	hasVoter := false
	for _, ep := range captured {
		if ep == voterURL {
			hasVoter = true
		}
	}
	if !hasVoter {
		t.Fatalf("discoverMemberID must include the Ready voter's URL; got %v", captured)
	}
}

// TestDiscoverMemberID_ExcludesSelfWhenVoterAvailable reproduces the dev4
// wedge directly: the member whose ID we're discovering is ITSELF a freshly
// added learner (Pod up, no MemberID, IsVoter=false). discoverMemberID must
// not append our own client URL to the endpoint list while a voter peer is
// reachable. Appending self lets clientv3's balancer round-robin MemberList
// onto our own learner etcd, which returns "rpc not supported for learner",
// stalling discovery past the progress deadline — exactly what kept the
// third member from ever being added during the cert-manager TLS smoke.
//
// The earlier _ExcludesNonVoterPeers / _FallsBackToPeers tests miss this
// because their target is a distinct member from the voter/learner peers,
// so self being appended is never exercised against a learner.
func TestDiscoverMemberID_ExcludesSelfWhenVoterAvailable(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	voter := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-seed", Namespace: "ns", Labels: memberLabels("test", "test-seed")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{
			PodName: "test-seed", MemberID: "0000000000000001",
			IsVoter:    true,
			Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
		},
	}
	// Target is the just-joined learner discovering its own ID.
	target := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-learner", Namespace: "ns", Labels: memberLabels("test", "test-learner")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{
			PodName:    "test-learner", // Pod up, but no MemberID and IsVoter=false.
			Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionFalse, Reason: "DiscoveringMemberID", LastTransitionTime: now}},
		},
	}
	c, _ := newTestClient(t, cluster, voter, target)

	const wantID uint64 = 0xfeedface
	fe := newFakeEtcd(0xdead,
		&etcdserverpb.Member{ID: 0x1, Name: "test-seed", PeerURLs: []string{peerURL("http", "test-seed", "test", "ns")}},
		&etcdserverpb.Member{ID: wantID, Name: "test-learner", PeerURLs: []string{peerURL("http", "test-learner", "test", "ns")}},
	)
	var captured []string
	factory := func(_ context.Context, eps []string, _ *tls.Config, _, _ string) (EtcdClusterClient, error) {
		captured = append([]string(nil), eps...)
		return fe, nil
	}
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factory}

	id, err := r.discoverMemberID(ctx, target)
	if err != nil {
		t.Fatalf("discoverMemberID: %v", err)
	}
	if id != wantID {
		t.Fatalf("id = %x, want %x", id, wantID)
	}
	selfURL := clientURL("http", "test-learner", "test", "ns")
	for _, ep := range captured {
		if ep == selfURL {
			t.Fatalf("discoverMemberID must not dial the learner's own URL while a voter is available; got %v", captured)
		}
	}
	if len(captured) != 1 || captured[0] != clientURL("http", "test-seed", "test", "ns") {
		t.Fatalf("expected only the voter's URL; got %v", captured)
	}
}

// TestDiscoverMemberID_FallsBackToSelfWhenNoVoter pins the fallback the
// above tightening must preserve: with no voter peer available (single-node
// bootstrap — the seed discovering its own ID), self is the only endpoint
// we can dial, and etcd serves MemberList fine on a single-member voter.
func TestDiscoverMemberID_FallsBackToSelfWhenNoVoter(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	// Only the seed exists; it has no MemberID yet and no voter peers.
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-seed", Namespace: "ns", Labels: memberLabels("test", "test-seed")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status:     lll.EtcdMemberStatus{PodName: "test-seed"},
	}
	c, _ := newTestClient(t, cluster, seed)

	const wantID uint64 = 0xabcdef
	fe := newFakeEtcd(0xdead,
		&etcdserverpb.Member{ID: wantID, Name: "test-seed", PeerURLs: []string{peerURL("http", "test-seed", "test", "ns")}},
	)
	var captured []string
	factory := func(_ context.Context, eps []string, _ *tls.Config, _, _ string) (EtcdClusterClient, error) {
		captured = append([]string(nil), eps...)
		return fe, nil
	}
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factory}

	id, err := r.discoverMemberID(ctx, seed)
	if err != nil {
		t.Fatalf("discoverMemberID: %v", err)
	}
	if id != wantID {
		t.Fatalf("id = %x, want %x", id, wantID)
	}
	selfURL := clientURL("http", "test-seed", "test", "ns")
	if len(captured) != 1 || captured[0] != selfURL {
		t.Fatalf("expected self URL as sole fallback endpoint; got %v", captured)
	}
}

// TestDiscoverMemberID_FallsBackToPeerURL covers blocker #2: in the window
// between MemberAddAsLearner and etcd propagating the joiner's Name, the
// only stable identifier we have is the peer URL. discoverMemberID must
// match on PeerURLs as well as Name, otherwise scale-up stalls.
func TestDiscoverMemberID_FallsBackToPeerURL(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}
	target := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-1", Namespace: "ns", Labels: memberLabels("test", "test-1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
	}
	c, _ := newTestClient(t, cluster, target)

	const wantID uint64 = 0xfeedface
	// fakeEtcd returns the target with Name="" but matching PeerURLs.
	fe := newFakeEtcd(0xdead,
		&etcdserverpb.Member{ID: wantID, Name: "", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
	)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	id, err := r.discoverMemberID(ctx, target)
	if err != nil {
		t.Fatalf("discoverMemberID should match by peer URL; got %v", err)
	}
	if id != wantID {
		t.Fatalf("id = %x, want %x", id, wantID)
	}
}

// TestUpdateStatus_NoChurnInSteadyState covers blocker #4: when nothing has
// changed since the previous reconcile, updateStatus must NOT issue a
// Status update. Otherwise every 30s periodic reconcile bumps
// resourceVersion and fans out a watch event for no reason.
func TestUpdateStatus_NoChurnInSteadyState(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
		Status: lll.EtcdMemberStatus{
			PodName:  "test-0",
			PVCName:  "data-test-0",
			MemberID: "0000000000000001",
			Replicas: 1,
			Selector: "etcd-operator.cozystack.io/cluster=test,app.kubernetes.io/component=test-0",
			Conditions: []metav1.Condition{{
				Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady",
				Message: "etcd member is ready", LastTransitionTime: now,
			}},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns"},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{readyPodCondition()},
		},
	}
	c, _ := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	rvBefore := mustGet(t, c, "test-0", "ns", &lll.EtcdMember{}).ResourceVersion
	if _, err := r.updateStatus(ctx, member); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	rvAfter := mustGet(t, c, "test-0", "ns", &lll.EtcdMember{}).ResourceVersion
	if rvBefore != rvAfter {
		t.Fatalf("ResourceVersion changed (%q -> %q) on a no-op updateStatus", rvBefore, rvAfter)
	}
}

// TestReconcile_WaitsForInitialClusterPatch covers the GenerateName flow's
// pending state: the cluster controller Creates an EtcdMember CR before
// it can fill Spec.InitialCluster (the assigned name is needed to
// register the peer URL with etcd first). Until the cluster controller
// follows up with that patch, the member controller must not start a
// pod — its etcd container would have no --initial-cluster value. The
// finalizer is added even in the pending state so a mid-flight delete
// still triggers MemberRemove cleanup.
func TestReconcile_WaitsForInitialClusterPatch(t *testing.T) {
	ctx := context.Background()
	pending := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pndng", Namespace: "ns", Labels: clusterLabels("test"),
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			ClusterToken: "ns-test-x",
			// InitialCluster intentionally empty.
		},
	}
	c, _ := newTestClient(t, pending)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-pndng", Namespace: "ns"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter for pending member; got %+v", res)
	}
	// Finalizer must be in place even in the pending state.
	got := mustGet(t, c, "test-pndng", "ns", &lll.EtcdMember{})
	hasFinalizer := false
	for _, f := range got.Finalizers {
		if f == MemberFinalizer {
			hasFinalizer = true
			break
		}
	}
	if !hasFinalizer {
		t.Fatalf("MemberFinalizer must be added before the InitialCluster gate; got %v", got.Finalizers)
	}
	// No PVC or Pod must have been created.
	pvcs := &corev1.PersistentVolumeClaimList{}
	_ = c.List(ctx, pvcs)
	if len(pvcs.Items) != 0 {
		t.Fatalf("PVC should not be created while InitialCluster is empty; got %d", len(pvcs.Items))
	}
	pods := &corev1.PodList{}
	_ = c.List(ctx, pods)
	if len(pods.Items) != 0 {
		t.Fatalf("Pod should not be created while InitialCluster is empty; got %d", len(pods.Items))
	}
}

// TestHandleDeletion_StillCallsMemberRemove pins that the deletion
// finalizer is no longer a pause path. Under the spec.Dormant design
// the cluster controller Patches Spec.Dormant=true on the surviving
// member during a 1→0 scale-down; it never issues a Delete that the
// finalizer would catch. Any Delete observed by the finalizer is
// therefore a genuine removal (intermediate scale-down step like
// 3→2 / 2→1, or user-driven `kubectl delete etcdmember`), and the
// finalizer must run MemberRemove against remaining peers as it
// always did.
//
// This test reproduces the intermediate-scale-down case: cluster
// running at observed.Replicas=0 (the 1→0 target the user just set),
// two members alive, one of them getting deleted. MemberRemove must
// fire against the surviving peer.
func TestHandleDeletion_StillCallsMemberRemove(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Namespace: "ns", UID: types.UID("cluster-uid"),
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 0, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	now := metav1.Now()
	survivor := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-keep1", Namespace: "ns", UID: types.UID("keep-uid"),
			Labels: memberLabels("test", "test-keep1"),
		},
		Spec:   lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x", ClusterToken: "ns-test-x"},
		Status: lll.EtcdMemberStatus{PodName: "test-keep1", MemberID: "00000000000000a1"},
	}
	victim := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-gone1", Namespace: "ns", UID: types.UID("gone-uid"),
			DeletionTimestamp: &now,
			Finalizers:        []string{MemberFinalizer},
			Labels:            memberLabels("test", "test-gone1"),
		},
		Spec:   lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x", ClusterToken: "ns-test-x"},
		Status: lll.EtcdMemberStatus{PodName: "test-gone1", MemberID: "00000000000000b2"},
	}
	c, _ := newTestClient(t, cluster, survivor, victim)
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa1, Name: "test-keep1", PeerURLs: []string{peerURL("http", "test-keep1", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xb2, Name: "test-gone1", PeerURLs: []string{peerURL("http", "test-gone1", "test", "ns")}},
	)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.handleDeletion(ctx, victim); err != nil {
		t.Fatalf("handleDeletion: %v", err)
	}
	if len(fe.removeCalls) != 1 || fe.removeCalls[0] != 0xb2 {
		t.Fatalf("MemberRemove(0xb2) expected; got %v", fe.removeCalls)
	}
}

// TestReconcile_DormantMemberDeletesPod covers the dormant gate. When
// the cluster controller flips Spec.Dormant=true on a member, the
// member controller's next reconcile must delete the Pod and leave the
// PVC untouched — that's the "park" state.
func TestReconcile_DormantMemberDeletesPod(t *testing.T) {
	ctx := context.Background()
	tru := true

	dormant := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-saved1", Namespace: "ns", UID: types.UID("member-uid"),
			Labels:     memberLabels("test", "test-saved1"),
			Finalizers: []string{MemberFinalizer},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: "x", ClusterToken: "ns-test-x", Bootstrap: true,
			Dormant: true,
		},
		Status: lll.EtcdMemberStatus{PodName: "test-saved1", PVCName: "data-test-saved1"},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-saved1", Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "test-saved1", UID: types.UID("member-uid"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-test-saved1", Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "test-saved1", UID: types.UID("member-uid"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	c, _ := newTestClient(t, dormant, pod, pvc)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-saved1", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Pod must be gone (or marked for deletion).
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test-saved1"}, &corev1.Pod{}); err == nil {
		t.Fatalf("dormant member's Pod must be deleted")
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error fetching Pod: %v", err)
	}
	// PVC must still exist with the EtcdMember as its owner-controller —
	// nothing reparented anything.
	gotPVC := mustGet(t, c, "data-test-saved1", "ns", &corev1.PersistentVolumeClaim{})
	if !pvcOwnedBy(gotPVC, dormant) {
		t.Fatalf("PVC owner-controller must still be the EtcdMember; got %+v", gotPVC.OwnerReferences)
	}
	// Status.PodName cleared so /status reflects reality.
	gotMember := mustGet(t, c, "test-saved1", "ns", &lll.EtcdMember{})
	if gotMember.Status.PodName != "" {
		t.Fatalf("Status.PodName should be cleared while dormant; got %q", gotMember.Status.PodName)
	}
}

// TestReconcile_WakeFromDormantCreatesPod covers the inverse: when the
// cluster controller flips Spec.Dormant back to false, the member
// controller's next reconcile must recreate the Pod against the
// (unchanged) PVC. etcd resumes from its existing data dir.
func TestReconcile_WakeFromDormantCreatesPod(t *testing.T) {
	ctx := context.Background()
	tru := true

	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x", ClusterID: "deadbeef",
			Observed: &lll.ObservedClusterSpec{Replicas: 1, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}},
		},
	}
	woken := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-saved1", Namespace: "ns", UID: types.UID("member-uid"),
			Labels:     memberLabels("test", "test-saved1"),
			Finalizers: []string{MemberFinalizer},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: buildInitialCluster("http", []string{"test-saved1"}, "test", "ns"),
			ClusterToken:   "ns-test-x", Bootstrap: true,
			// Dormant=false — the cluster controller just flipped it back.
		},
	}
	// Pre-existing PVC owned by the same EtcdMember (UID matches) — kept
	// in place across the pause.
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data-test-saved1", Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "test-saved1", UID: types.UID("member-uid"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	c, _ := newTestClient(t, cluster, woken, pvc)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-saved1", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Pod must exist now.
	gotPod := mustGet(t, c, "test-saved1", "ns", &corev1.Pod{})
	if gotPod.Name != "test-saved1" {
		t.Fatalf("expected Pod test-saved1 to exist after wake")
	}
	// PVC must still exist with the same owner.
	gotPVC := mustGet(t, c, "data-test-saved1", "ns", &corev1.PersistentVolumeClaim{})
	if !pvcOwnedBy(gotPVC, woken) {
		t.Fatalf("PVC owner-controller must still be the woken EtcdMember; got %+v", gotPVC.OwnerReferences)
	}
}

// silence unused imports
var _ = ctrl.Result{}

// TestBuildPod_MemoryMediumUsesEmptyDir verifies that storage.medium=Memory
// flips the Pod's data volume from a PVC to a tmpfs emptyDir with
// SizeLimit set from spec.storage.size. Without this, etcd writes to
// the node's filesystem and the whole "memory-backed cluster" feature
// is a no-op.
func TestBuildPod_MemoryMediumUsesEmptyDir(t *testing.T) {
	r := &EtcdMemberReconciler{}
	storage := quickQty(t, "256Mi")
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test",
			Version:     "3.5.17",
			Storage:     lll.StorageSpec{Size: storage, Medium: lll.StorageMediumMemory},
		},
	})

	if len(pod.Spec.Volumes) != 1 {
		t.Fatalf("expected one Volume; got %d", len(pod.Spec.Volumes))
	}
	v := pod.Spec.Volumes[0]
	if v.PersistentVolumeClaim != nil {
		t.Fatalf("memory member must not have a PVC volume source; got %+v", v.PersistentVolumeClaim)
	}
	if v.EmptyDir == nil {
		t.Fatalf("memory member must have an EmptyDir volume source; got %+v", v)
	}
	if v.EmptyDir.Medium != corev1.StorageMediumMemory {
		t.Fatalf("EmptyDir.Medium = %q, want %q", v.EmptyDir.Medium, corev1.StorageMediumMemory)
	}
	if v.EmptyDir.SizeLimit == nil || v.EmptyDir.SizeLimit.Cmp(storage) != 0 {
		t.Fatalf("EmptyDir.SizeLimit = %v, want %v", v.EmptyDir.SizeLimit, storage)
	}
}

// TestBuildPod_DefaultMediumUsesPVC is the negative guard: an empty
// storage.medium must still produce a PVC-backed volume so existing
// clusters' Pods don't silently start writing to tmpfs after a controller
// upgrade.
func TestBuildPod_DefaultMediumUsesPVC(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test",
			Version:     "3.5.17",
			Storage:     lll.StorageSpec{Size: quickQty(t, "1Gi")},
			// storage.medium left empty.
		},
	})
	v := pod.Spec.Volumes[0]
	if v.EmptyDir != nil {
		t.Fatalf("default member must not have an EmptyDir volume source; got %+v", v.EmptyDir)
	}
	if v.PersistentVolumeClaim == nil {
		t.Fatalf("default member must have a PVC volume source; got %+v", v)
	}
	if v.PersistentVolumeClaim.ClaimName != "data-m-1" {
		t.Fatalf("PVC claim name = %q, want data-m-1", v.PersistentVolumeClaim.ClaimName)
	}
}

// TestEnsurePVC_SkippedForMemoryMember verifies ensurePVC does not create
// a PVC and leaves Status.PVCName empty for memory members. A PVC sneaking
// into the namespace would be a silent attached cost (allocated capacity
// no one reads from) and would also wrongly suggest "data is preserved"
// via Status.PVCName.
func TestEnsurePVC_SkippedForMemoryMember(t *testing.T) {
	ctx := context.Background()
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "ns", UID: types.UID("mu")},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test",
			Version:     "3.5.17",
			Storage:     lll.StorageSpec{Size: quickQty(t, "1Gi"), Medium: lll.StorageMediumMemory},
		},
	}
	c, _ := newTestClient(t, member)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePVC(ctx, member); err != nil {
		t.Fatalf("ensurePVC: %v", err)
	}

	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "data-m-1"}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Fatalf("memory member must not create a PVC; got err=%v", err)
	}
	if member.Status.PVCName != "" {
		t.Fatalf("memory member Status.PVCName must stay empty; got %q", member.Status.PVCName)
	}
}

// TestEnsurePod_CapturesUIDOfExistingPod verifies that on a reconcile
// pass that finds an already-running Pod, ensurePod copies the Pod's
// UID into Status.PodUID. This is the steady-state path that runs on
// every reconcile, and it's the source of truth the next reconcile uses
// to detect Pod loss (Pod replaced → new UID → mismatch → loss).
//
// Pre-creating the Pod with an explicit UID rather than relying on
// ensurePod's Create branch: the controller-runtime fake client doesn't
// auto-assign UIDs on Create, so testing the Create-then-read path would
// be testing the fake's behaviour, not ours. The next-reconcile path is
// the one that matters anyway — Create races with reconcile cadence and
// the Get-then-read path will run within milliseconds in production.
func TestEnsurePod_CapturesUIDOfExistingPod(t *testing.T) {
	ctx := context.Background()
	tru := true

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "ns", UID: types.UID("mu")},
		Spec: lll.EtcdMemberSpec{
			ClusterName:    "test",
			Version:        "3.5.17",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: "m-1=" + peerURL("http", "m-1", "test", "ns"),
			ClusterToken:   "ns-test-x",
			Bootstrap:      true,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("known-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "m-1", UID: types.UID("mu"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
	}
	c, _ := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePod(ctx, member); err != nil {
		t.Fatalf("ensurePod: %v", err)
	}
	if member.Status.PodUID != "known-uid" {
		t.Fatalf("Status.PodUID = %q, want %q", member.Status.PodUID, "known-uid")
	}
	if member.Status.PodName != "m-1" {
		t.Fatalf("Status.PodName = %q, want m-1", member.Status.PodName)
	}
}

// TestReconcile_MemoryMemberDeletesSelfOnPodLoss covers the central
// guarantee of the feature: a memory-backed member whose Pod is gone
// (tmpfs lost with it) must trigger its own deletion. The finalizer
// then runs MemberRemove against peers and the cluster controller's
// scale-up gap-fill replaces it.
//
// Without this path, the next reconcile would re-create the Pod with
// an empty tmpfs and etcd would refuse to start (member ID is in raft
// state but WAL is empty), wedging the member.
func TestReconcile_MemoryMemberDeletesSelfOnPodLoss(t *testing.T) {
	ctx := context.Background()

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("mu"),
			Labels:     memberLabels("test", "m-1"),
			Finalizers: []string{MemberFinalizer},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName:    "test",
			Version:        "3.5.17",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi"), Medium: lll.StorageMediumMemory},
			InitialCluster: "m-1=" + peerURL("http", "m-1", "test", "ns"),
			ClusterToken:   "ns-test-x",
			Bootstrap:      true,
		},
		Status: lll.EtcdMemberStatus{
			PodName: "m-1",
			PodUID:  "previously-recorded-uid",
			// MemberID empty: simulates the case where the Pod went away
			// before discovery could attach a member ID. The finalizer's
			// fallback-by-name path covers that elsewhere.
		},
	}
	// No Pod object — that's the loss condition.
	c, _ := newTestClient(t, member)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "m-1", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// The EtcdMember must now carry a DeletionTimestamp (or be gone
	// outright — the fake client may not run finalizers, but it does
	// stamp the timestamp on Delete).
	got := &lll.EtcdMember{}
	err := c.Get(ctx, types.NamespacedName{Name: "m-1", Namespace: "ns"}, got)
	switch {
	case apierrors.IsNotFound(err):
		// finalizer already ran; that's fine.
	case err != nil:
		t.Fatalf("Get(member): %v", err)
	case got.DeletionTimestamp.IsZero():
		t.Fatalf("memory member with lost Pod must be marked for deletion; got DeletionTimestamp empty")
	}

	// And critically: no fresh Pod must have been created. ensurePod
	// would otherwise have run after the (false-negative) loss check and
	// created a new tmpfs-backed Pod.
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m-1"}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("memory member with lost Pod must not have a fresh Pod created; got err=%v", err)
	}
}

// TestReconcile_MemoryMemberStablePodIsNotLost is the negative guard for
// the above: a memory member whose Pod is present with the recorded UID
// must NOT be self-deleted on reconcile. Without this guard the loss
// check would fire on every reconcile and the cluster would churn itself
// to death.
func TestReconcile_MemoryMemberStablePodIsNotLost(t *testing.T) {
	ctx := context.Background()
	tru := true

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("mu"),
			Labels:     memberLabels("test", "m-1"),
			Finalizers: []string{MemberFinalizer},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName:    "test",
			Version:        "3.5.17",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi"), Medium: lll.StorageMediumMemory},
			InitialCluster: "m-1=" + peerURL("http", "m-1", "test", "ns"),
			ClusterToken:   "ns-test-x",
			Bootstrap:      true,
		},
		Status: lll.EtcdMemberStatus{
			PodName: "m-1",
			PodUID:  "stable-uid",
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("stable-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "m-1", UID: types.UID("mu"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{readyPodCondition()}},
	}
	c, _ := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "m-1", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := mustGet(t, c, "m-1", "ns", &lll.EtcdMember{})
	if !got.DeletionTimestamp.IsZero() {
		t.Fatalf("memory member with stable Pod must not be deleted; DeletionTimestamp = %v", got.DeletionTimestamp)
	}
}

// TestUpdateStatus_MemoryMemberLeavesPVCNameEmpty: even after a full
// reconcile pass, a memory member's Status.PVCName must stay empty so
// downstream consumers (the EtcdCluster's Paused message in particular,
// which refers to "PVC data-X" when describing preserved data) don't
// claim there's a PVC to preserve.
func TestUpdateStatus_MemoryMemberLeavesPVCNameEmpty(t *testing.T) {
	ctx := context.Background()
	tru := true

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("mu"),
			Labels:     memberLabels("test", "m-1"),
			Finalizers: []string{MemberFinalizer},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName:    "test",
			Version:        "3.5.17",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi"), Medium: lll.StorageMediumMemory},
			InitialCluster: "m-1=" + peerURL("http", "m-1", "test", "ns"),
			ClusterToken:   "ns-test-x",
			Bootstrap:      true,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("pod-uid"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "m-1", UID: types.UID("mu"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{readyPodCondition()}},
	}
	c, _ := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "m-1", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := mustGet(t, c, "m-1", "ns", &lll.EtcdMember{})
	if got.Status.PVCName != "" {
		t.Fatalf("memory member Status.PVCName must stay empty after reconcile; got %q", got.Status.PVCName)
	}
	if got.Status.PodUID != "pod-uid" {
		t.Fatalf("Status.PodUID = %q, want pod-uid (must reflect the live Pod)", got.Status.PodUID)
	}
}

// TestIsBroken_MemoryMemberWithLostPodIsBroken pins the predicate that
// drives EtcdCluster.status.brokenMembers. A memory member whose Pod
// UID was recorded but whose Pod is currently absent (Status.PodName
// cleared by updateStatus's NotFound branch — or never set) is broken.
func TestIsBroken_MemoryMemberWithLostPodIsBroken(t *testing.T) {
	r := &EtcdClusterReconciler{}
	cases := []struct {
		name string
		m    lll.EtcdMember
		want bool
	}{
		{
			name: "memory, UID recorded, Pod missing → broken",
			m: lll.EtcdMember{
				Spec:   lll.EtcdMemberSpec{Storage: lll.StorageSpec{Medium: lll.StorageMediumMemory}},
				Status: lll.EtcdMemberStatus{PodUID: "u", PodName: ""},
			},
			want: true,
		},
		{
			name: "memory, UID recorded, Pod present → healthy",
			m: lll.EtcdMember{
				Spec:   lll.EtcdMemberSpec{Storage: lll.StorageSpec{Medium: lll.StorageMediumMemory}},
				Status: lll.EtcdMemberStatus{PodUID: "u", PodName: "p"},
			},
			want: false,
		},
		{
			name: "memory, no UID yet (first reconcile) → not broken",
			m: lll.EtcdMember{
				Spec:   lll.EtcdMemberSpec{Storage: lll.StorageSpec{Medium: lll.StorageMediumMemory}},
				Status: lll.EtcdMemberStatus{},
			},
			want: false,
		},
		{
			name: "PVC-backed, Pod missing → stub stays false",
			m: lll.EtcdMember{
				Spec:   lll.EtcdMemberSpec{Storage: lll.StorageSpec{Medium: lll.StorageMediumDefault}},
				Status: lll.EtcdMemberStatus{PodUID: "u", PodName: ""},
			},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := r.isBroken(tc.m); got != tc.want {
				t.Fatalf("isBroken(%s) = %v; want %v", tc.name, got, tc.want)
			}
		})
	}
}

// keep errors import live in case more tests are added below.
var _ = errors.New

// TestEnsurePod_AppliesRoleLabelWhenIsVoter verifies the member
// controller propagates Status.IsVoter onto the Pod's LabelRole label
// during ensurePod's existing-Pod path. The PDB's selector keys on
// this label; without propagation the PDB would never match the Pod.
func TestEnsurePod_AppliesRoleLabelWhenIsVoter(t *testing.T) {
	ctx := context.Background()
	tru := true

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "ns", UID: types.UID("mu")},
		Spec: lll.EtcdMemberSpec{
			ClusterName:    "test",
			Version:        "3.5.17",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: "m-1=" + peerURL("http", "m-1", "test", "ns"),
			ClusterToken:   "ns-test-x",
			Bootstrap:      true,
		},
		Status: lll.EtcdMemberStatus{IsVoter: true},
	}
	// Pod exists without the role label — the steady-state case where
	// Status.IsVoter was flipped after the Pod was created.
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("pod-uid"),
			Labels: memberLabels("test", "m-1"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "m-1", UID: types.UID("mu"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
	}
	c, _ := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePod(ctx, member); err != nil {
		t.Fatalf("ensurePod: %v", err)
	}
	got := mustGet(t, c, "m-1", "ns", &corev1.Pod{})
	if got.Labels[LabelRole] != RoleVoter {
		t.Fatalf("Pod label %s = %q, want %q", LabelRole, got.Labels[LabelRole], RoleVoter)
	}
}

// TestEnsurePod_StripsRoleLabelWhenNotVoter is the inverse: when the
// cluster controller flips Status.IsVoter from true to false (member
// demoted, or stale CR state being corrected), the member controller
// must remove the label. Otherwise the PDB would over-protect a
// non-voter Pod and a learner-only eviction would consume budget.
func TestEnsurePod_StripsRoleLabelWhenNotVoter(t *testing.T) {
	ctx := context.Background()
	tru := true

	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "ns", UID: types.UID("mu")},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: "x", ClusterToken: "ns-test-x", Bootstrap: true,
		},
		Status: lll.EtcdMemberStatus{IsVoter: false},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "m-1", Namespace: "ns", UID: types.UID("pod-uid"),
			Labels: map[string]string{
				LabelCluster: "test",
				LabelRole:    RoleVoter, // stale label from a prior voter state
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdMember",
				Name: "m-1", UID: types.UID("mu"), Controller: &tru, BlockOwnerDeletion: &tru,
			}},
		},
	}
	c, _ := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensurePod(ctx, member); err != nil {
		t.Fatalf("ensurePod: %v", err)
	}
	got := mustGet(t, c, "m-1", "ns", &corev1.Pod{})
	if _, present := got.Labels[LabelRole]; present {
		t.Fatalf("Pod label %s must be stripped when Status.IsVoter=false; got %q", LabelRole, got.Labels[LabelRole])
	}
}

// TestBuildPod_RoleLabelAtCreateForVoter verifies the create-time
// optimization for the seed: when the cluster controller pre-stamps
// Status.IsVoter=true before the first ensurePod, buildPod emits the
// Pod with LabelRole=RoleVoter already set, saving one reconcile
// cycle of unprotected-Pod window during bootstrap.
func TestBuildPod_RoleLabelAtCreateForVoter(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-1", Namespace: "ns"},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17"},
		Status:     lll.EtcdMemberStatus{IsVoter: true},
	})
	if pod.Labels[LabelRole] != RoleVoter {
		t.Fatalf("buildPod with IsVoter=true must emit %s=%q; got %q", LabelRole, RoleVoter, pod.Labels[LabelRole])
	}

	// Negative side: IsVoter=false omits the label entirely (not "" — we
	// don't want the empty string to match a permissive selector).
	pod2 := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m-2", Namespace: "ns"},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17"},
		Status:     lll.EtcdMemberStatus{IsVoter: false},
	})
	if _, present := pod2.Labels[LabelRole]; present {
		t.Fatalf("buildPod with IsVoter=false must not set %s; got %q", LabelRole, pod2.Labels[LabelRole])
	}
}

// ── TLS ──────────────────────────────────────────────────────────────────

func cmdContains(cmd []string, want string) bool {
	for _, a := range cmd {
		if a == want {
			return true
		}
	}
	return false
}

func mountFor(pod *corev1.Pod, name string) *corev1.VolumeMount {
	for i, m := range pod.Spec.Containers[0].VolumeMounts {
		if m.Name == name {
			return &pod.Spec.Containers[0].VolumeMounts[i]
		}
	}
	return nil
}

func volumeFor(pod *corev1.Pod, name string) *corev1.Volume {
	for i, v := range pod.Spec.Volumes {
		if v.Name == name {
			return &pod.Spec.Volumes[i]
		}
	}
	return nil
}

// TestBuildPod_PlaintextHasNoTLSFlags is the negative regression: existing
// non-TLS clusters must keep their http:// listen URLs, no --cert-file, no
// extra volumes, and probe :2379. Catches accidental TLS-defaults creep.
func TestBuildPod_PlaintextHasNoTLSFlags(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}},
	})
	cmd := pod.Spec.Containers[0].Command
	if !cmdContains(cmd, "--listen-peer-urls=http://0.0.0.0:2380") {
		t.Fatalf("plaintext peer listen URL missing: %v", cmd)
	}
	if !cmdContains(cmd, "--listen-client-urls=http://0.0.0.0:2379") {
		t.Fatalf("plaintext client listen URL missing: %v", cmd)
	}
	for _, a := range cmd {
		if strings.HasPrefix(a, "--cert-file") || strings.HasPrefix(a, "--peer-cert-file") {
			t.Fatalf("plaintext pod must not have cert flags; got %q", a)
		}
	}
	if mountFor(pod, "tls-client") != nil || mountFor(pod, "tls-peer") != nil {
		t.Fatalf("plaintext pod must not mount TLS volumes")
	}
	if pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Port.IntValue() != 2381 {
		t.Fatalf("readiness probe should target the plaintext metrics port 2381; got %v", pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Port)
	}
	if !cmdContains(cmd, "--listen-metrics-urls=http://0.0.0.0:2381") {
		t.Fatalf("plaintext pod must always expose the metrics URL: %v", cmd)
	}
}

// TestBuildPod_ClientTLSOnlyAddsServerCertButNoClientAuth covers the
// server-TLS-only mode: --cert-file/--key-file but no --client-cert-auth
// and no --trusted-ca-file (etcd would otherwise require client certs).
func TestBuildPod_ClientTLSOnlyAddsServerCertButNoClientAuth(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			TLS: &lll.EtcdMemberTLS{
				ClientServerSecretRef: &corev1.LocalObjectReference{Name: "srv"},
				ClientMTLS:            false,
			},
		},
	})
	cmd := pod.Spec.Containers[0].Command
	if !cmdContains(cmd, "--listen-client-urls=https://0.0.0.0:2379") {
		t.Fatalf("client listen URL not https: %v", cmd)
	}
	if !cmdContains(cmd, "--cert-file=/etc/etcd/tls/client/tls.crt") {
		t.Fatalf("missing --cert-file flag: %v", cmd)
	}
	for _, a := range cmd {
		if strings.HasPrefix(a, "--client-cert-auth") {
			t.Fatalf("server-TLS-only mode must not enable client-cert-auth; got %q", a)
		}
		if strings.HasPrefix(a, "--trusted-ca-file") {
			t.Fatalf("server-TLS-only mode must not mount trusted CA; got %q", a)
		}
	}
	if v := volumeFor(pod, "tls-client"); v == nil || v.Secret == nil || v.Secret.SecretName != "srv" {
		t.Fatalf("expected tls-client volume backed by Secret %q; got %+v", "srv", v)
	}
	if pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Port.IntValue() != 2381 {
		t.Fatalf("client-TLS readiness probe should target the localhost metrics port 2381; got %v", pod.Spec.Containers[0].ReadinessProbe.HTTPGet.Port)
	}
	if !cmdContains(cmd, "--listen-metrics-urls=http://0.0.0.0:2381") {
		t.Fatalf("client-TLS pod must expose plaintext metrics URL for the probe: %v", cmd)
	}
}

// TestBuildPod_ClientMTLSAddsTrustedCAAndClientCertAuth verifies that
// ClientMTLS=true on the propagated member spec emits the apiserver-required
// flags. The mTLS bit is a separate signal from "client TLS is on" so the
// operator can recover the spec from the EtcdMember in isolation.
func TestBuildPod_ClientMTLSAddsTrustedCAAndClientCertAuth(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			TLS: &lll.EtcdMemberTLS{
				ClientServerSecretRef: &corev1.LocalObjectReference{Name: "srv"},
				ClientMTLS:            true,
			},
		},
	})
	cmd := pod.Spec.Containers[0].Command
	if !cmdContains(cmd, "--client-cert-auth=true") {
		t.Fatalf("mTLS pod must set --client-cert-auth=true: %v", cmd)
	}
	if !cmdContains(cmd, "--trusted-ca-file=/etc/etcd/tls/client/ca.crt") {
		t.Fatalf("mTLS pod must set --trusted-ca-file: %v", cmd)
	}
}

// TestBuildPod_PeerTLSAlwaysMTLS covers the peer plane's fixed-mTLS
// semantics. Peer is symmetric (same cert serves and dials), there is no
// useful encrypt-only mode, and --peer-client-cert-auth=true must always
// be set whenever peer TLS is on.
func TestBuildPod_PeerTLSAlwaysMTLS(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			TLS: &lll.EtcdMemberTLS{
				PeerSecretRef: &corev1.LocalObjectReference{Name: "peer"},
			},
		},
	})
	cmd := pod.Spec.Containers[0].Command
	if !cmdContains(cmd, "--listen-peer-urls=https://0.0.0.0:2380") {
		t.Fatalf("peer listen URL not https: %v", cmd)
	}
	for _, want := range []string{
		"--peer-cert-file=/etc/etcd/tls/peer/tls.crt",
		"--peer-key-file=/etc/etcd/tls/peer/tls.key",
		"--peer-trusted-ca-file=/etc/etcd/tls/peer/ca.crt",
		"--peer-client-cert-auth=true",
	} {
		if !cmdContains(cmd, want) {
			t.Fatalf("missing required peer-TLS flag %q in: %v", want, cmd)
		}
	}
	if v := volumeFor(pod, "tls-peer"); v == nil || v.Secret == nil || v.Secret.SecretName != "peer" {
		t.Fatalf("expected tls-peer volume backed by Secret %q; got %+v", "peer", v)
	}
}

// TestBuildPod_AlwaysExposesMetricsPort guards the cozystack-shaped
// monitoring contract: VMPodScrape (and equivalent Prometheus scrapers)
// target the named "metrics" container port unconditionally, and the
// --listen-metrics-urls flag must be set regardless of TLS state so the
// /health and /metrics endpoints are reachable on a plaintext port.
func TestBuildPod_AlwaysExposesMetricsPort(t *testing.T) {
	r := &EtcdMemberReconciler{}
	cases := []struct {
		name   string
		member *lll.EtcdMember
	}{
		{
			name: "plaintext",
			member: &lll.EtcdMember{
				ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
				Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}},
			},
		},
		{
			name: "client TLS only",
			member: &lll.EtcdMember{
				ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
				Spec: lll.EtcdMemberSpec{
					ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
					TLS: &lll.EtcdMemberTLS{ClientServerSecretRef: &corev1.LocalObjectReference{Name: "srv"}},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := r.buildPod(tc.member)
			var foundPort *corev1.ContainerPort
			for i, p := range pod.Spec.Containers[0].Ports {
				if p.Name == "metrics" {
					foundPort = &pod.Spec.Containers[0].Ports[i]
					break
				}
			}
			if foundPort == nil {
				t.Fatalf("named 'metrics' container port missing; got %+v", pod.Spec.Containers[0].Ports)
			}
			if foundPort.ContainerPort != 2381 {
				t.Fatalf("metrics port = %d; want 2381", foundPort.ContainerPort)
			}
			if !cmdContains(pod.Spec.Containers[0].Command, "--listen-metrics-urls=http://0.0.0.0:2381") {
				t.Fatalf("--listen-metrics-urls flag missing: %v", pod.Spec.Containers[0].Command)
			}
		})
	}
}

// TestBuildPod_UsesSpecResources verifies that spec.resources flows
// through to the etcd container's resources field unchanged. Without
// this wiring, custom CPU/memory sizing — including VPA recommendations
// applied to the cluster — never reaches the Pod.
func TestBuildPod_UsesSpecResources(t *testing.T) {
	r := &EtcdMemberReconciler{}
	want := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("1Gi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17",
			Storage:   lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Resources: want,
		},
	})
	got := pod.Spec.Containers[0].Resources
	if got.Requests.Cpu().Cmp(*want.Requests.Cpu()) != 0 ||
		got.Requests.Memory().Cmp(*want.Requests.Memory()) != 0 ||
		got.Limits.Cpu().Cmp(*want.Limits.Cpu()) != 0 ||
		got.Limits.Memory().Cmp(*want.Limits.Memory()) != 0 {
		t.Fatalf("Resources mismatch:\n got = %+v\nwant = %+v", got, want)
	}
}

// TestBuildPod_ClaimsOnlyResourcesNotDroppedToDefault covers the
// edge case where a user sets ResourceRequirements.Claims (the
// DynamicResourceAllocation axis) but leaves Requests and Limits
// empty. A naive `len(Requests) > 0 || len(Limits) > 0` predicate
// would mistake that for "user set nothing" and silently drop the
// claims onto the floor; containerResources uses semantic deep-equality
// against the zero value to avoid the trap.
func TestBuildPod_ClaimsOnlyResourcesNotDroppedToDefault(t *testing.T) {
	r := &EtcdMemberReconciler{}
	in := corev1.ResourceRequirements{
		Claims: []corev1.ResourceClaim{{Name: "gpu"}},
	}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17",
			Storage:   lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Resources: in,
		},
	})
	got := pod.Spec.Containers[0].Resources
	if len(got.Claims) != 1 || got.Claims[0].Name != "gpu" {
		t.Fatalf("Claims dropped on the floor; got %+v", got.Claims)
	}
	if len(got.Requests) != 0 || len(got.Limits) != 0 {
		t.Fatalf("default-fallthrough should not fire when only Claims is set; got %+v", got)
	}
}

// TestBuildPod_DefaultsResourcesWhenUnset preserves the pre-existing
// 100m/128Mi request defaults for clusters that don't opt into custom
// sizing. The fall-through behaviour matters: regressing to "no
// requests at all" would silently demote etcd Pods to BestEffort QoS
// (first-evicted under memory pressure).
func TestBuildPod_DefaultsResourcesWhenUnset(t *testing.T) {
	r := &EtcdMemberReconciler{}
	pod := r.buildPod(&lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17",
			Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			// Resources intentionally zero.
		},
	})
	got := pod.Spec.Containers[0].Resources
	if got.Requests.Cpu().Cmp(resource.MustParse("100m")) != 0 {
		t.Fatalf("default CPU request = %v; want 100m", got.Requests.Cpu())
	}
	if got.Requests.Memory().Cmp(resource.MustParse("128Mi")) != 0 {
		t.Fatalf("default memory request = %v; want 128Mi", got.Requests.Memory())
	}
	if len(got.Limits) != 0 {
		t.Fatalf("default Limits must be empty; got %+v", got.Limits)
	}
}

// TestDeriveMemberTLS covers the cluster→member projection. ClientMTLS
// must be true iff OperatorClientSecretRef is set; secret refs are deep-
// copied so a later edit to the parent's pointer can't mutate the
// already-created member.
func TestDeriveMemberTLS(t *testing.T) {
	type want struct {
		nilOut       bool
		hasClient    bool
		hasPeer      bool
		clientMTLS   bool
		serverSecret string
		opSecret     string
		peerSecret   string
	}
	withName := func(c *lll.EtcdCluster) *lll.EtcdCluster {
		c.ObjectMeta.Name = "etcd"
		return c
	}
	cases := []struct {
		name string
		in   *lll.EtcdCluster
		want want
	}{
		{
			name: "nil tls",
			in:   &lll.EtcdCluster{},
			want: want{nilOut: true},
		},
		{
			name: "byo client only, no mtls",
			in: withName(&lll.EtcdCluster{Spec: lll.EtcdClusterSpec{TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{ServerSecretRef: &corev1.LocalObjectReference{Name: "s"}},
			}}}),
			want: want{hasClient: true, serverSecret: "s"},
		},
		{
			name: "byo client with mtls",
			in: withName(&lll.EtcdCluster{Spec: lll.EtcdClusterSpec{TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{
					ServerSecretRef:         &corev1.LocalObjectReference{Name: "s"},
					OperatorClientSecretRef: &corev1.LocalObjectReference{Name: "op"},
				},
			}}}),
			want: want{hasClient: true, clientMTLS: true, serverSecret: "s", opSecret: "op"},
		},
		{
			name: "byo peer only",
			in: withName(&lll.EtcdCluster{Spec: lll.EtcdClusterSpec{TLS: &lll.EtcdClusterTLS{
				Peer: &lll.PeerTLS{SecretRef: &corev1.LocalObjectReference{Name: "p"}},
			}}}),
			want: want{hasPeer: true, peerSecret: "p"},
		},
		{
			name: "byo both",
			in: withName(&lll.EtcdCluster{Spec: lll.EtcdClusterSpec{TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{ServerSecretRef: &corev1.LocalObjectReference{Name: "s"}},
				Peer:   &lll.PeerTLS{SecretRef: &corev1.LocalObjectReference{Name: "p"}},
			}}}),
			want: want{hasClient: true, hasPeer: true, serverSecret: "s", peerSecret: "p"},
		},
		{
			name: "certManager client, no mtls",
			in: withName(&lll.EtcdCluster{Spec: lll.EtcdClusterSpec{TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
					ServerIssuerRef: lll.IssuerReference{Name: "my-ca"},
				}},
			}}}),
			want: want{hasClient: true, serverSecret: "etcd-server-tls"},
		},
		{
			name: "certManager client with mtls",
			in: withName(&lll.EtcdCluster{Spec: lll.EtcdClusterSpec{TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
					ServerIssuerRef:         lll.IssuerReference{Name: "my-ca"},
					OperatorClientIssuerRef: &lll.IssuerReference{Name: "my-ca"},
				}},
			}}}),
			want: want{hasClient: true, clientMTLS: true, serverSecret: "etcd-server-tls", opSecret: "etcd-operator-client-tls"},
		},
		{
			name: "certManager peer",
			in: withName(&lll.EtcdCluster{Spec: lll.EtcdClusterSpec{TLS: &lll.EtcdClusterTLS{
				Peer: &lll.PeerTLS{CertManager: &lll.PeerCertManagerTLS{IssuerRef: lll.IssuerReference{Name: "peer-ca"}}},
			}}}),
			want: want{hasPeer: true, peerSecret: "etcd-peer-tls"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := deriveMemberTLS(tc.in)
			if tc.want.nilOut {
				if got != nil {
					t.Fatalf("expected nil; got %+v", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("expected non-nil")
			}
			if (got.ClientServerSecretRef != nil) != tc.want.hasClient {
				t.Fatalf("hasClient = %v; want %v", got.ClientServerSecretRef != nil, tc.want.hasClient)
			}
			if (got.PeerSecretRef != nil) != tc.want.hasPeer {
				t.Fatalf("hasPeer = %v; want %v", got.PeerSecretRef != nil, tc.want.hasPeer)
			}
			if got.ClientMTLS != tc.want.clientMTLS {
				t.Fatalf("ClientMTLS = %v; want %v", got.ClientMTLS, tc.want.clientMTLS)
			}
			if tc.want.serverSecret != "" && got.ClientServerSecretRef.Name != tc.want.serverSecret {
				t.Fatalf("ClientServerSecretRef.Name = %q; want %q", got.ClientServerSecretRef.Name, tc.want.serverSecret)
			}
			if tc.want.peerSecret != "" && got.PeerSecretRef.Name != tc.want.peerSecret {
				t.Fatalf("PeerSecretRef.Name = %q; want %q", got.PeerSecretRef.Name, tc.want.peerSecret)
			}
			if tc.want.opSecret != "" && operatorClientSecretName(tc.in) != tc.want.opSecret {
				t.Fatalf("operatorClientSecretName = %q; want %q", operatorClientSecretName(tc.in), tc.want.opSecret)
			}
		})
	}
}

// TestEnsurePod_BlocksOnMissingTLSSecret covers the precheck: without it,
// the Pod would be created and stay in ContainerCreating with FailedMount.
// Returning an error keeps the reconcile in the standard backoff loop and
// surfaces a clear cause in the operator logs.
func TestEnsurePod_BlocksOnMissingTLSSecret(t *testing.T) {
	ctx := context.Background()
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns", UID: types.UID("mu")},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			TLS: &lll.EtcdMemberTLS{ClientServerSecretRef: &corev1.LocalObjectReference{Name: "missing"}},
		},
	}
	c, _ := newTestClient(t, member)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	err := r.ensurePod(ctx, member)
	if err == nil {
		t.Fatalf("expected error when referenced TLS secret is absent")
	}
	if !apierrors.IsNotFound(err) && !strings.Contains(err.Error(), "TLS secret") {
		t.Fatalf("expected the error to identify the missing TLS secret; got %v", err)
	}
	// And the Pod must not have been created.
	pod := &corev1.Pod{}
	getErr := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "m"}, pod)
	if getErr == nil {
		t.Fatalf("Pod should not exist when referenced TLS secret is missing")
	}
}
