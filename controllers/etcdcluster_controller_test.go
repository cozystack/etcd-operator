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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// reconcileUntilStable drives a reconciler in a loop until it stops requesting
// requeues, or until maxIters runs out (test failure). Each call refreshes the
// passed object from the fake client so callers can inspect the latest state.
func reconcileUntilStable(t *testing.T, r *EtcdClusterReconciler, c client.Client, name, ns string, maxIters int) ctrl.Result {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: ns}}
	var last ctrl.Result
	for i := 0; i < maxIters; i++ {
		res, err := r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("Reconcile iter %d: %v", i, err)
		}
		last = res
		if !res.Requeue && res.RequeueAfter == 0 {
			return last
		}
	}
	return last
}

// TestBootstrap_CreatesSingleSeedMember covers the simplified bootstrap
// model: regardless of spec.replicas, bootstrap creates exactly one seed
// member whose --initial-cluster lists only itself. Subsequent members
// join via MemberAdd once ClusterID is latched. Seed identity is
// established by Spec.Bootstrap=true, not by a predictable name —
// members are created via GenerateName="<cluster>-".
func TestBootstrap_CreatesSingleSeedMember(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(5),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
	}
	c, _ := newTestClient(t, cluster)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	// Run enough reconciles to land past token + observed snapshots, then
	// trigger bootstrap. Bootstrap requeues 5s; we stop iterating then.
	reconcileUntilStable(t, r, c, "test", "ns", 8)

	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("bootstrap must create exactly one member; got %d (%v)", len(members.Items),
			func() []string {
				ns := make([]string, 0, len(members.Items))
				for _, m := range members.Items {
					ns = append(ns, m.Name)
				}
				return ns
			}())
	}
	seed := members.Items[0]
	if !strings.HasPrefix(seed.Name, "test-") {
		t.Fatalf("seed name should start with %q; got %q", "test-", seed.Name)
	}
	if seed.Name == "test-" {
		t.Fatalf("seed name should carry a GenerateName-assigned suffix; got bare %q", seed.Name)
	}
	wantIC := buildInitialCluster("http", []string{seed.Name}, "test", "ns")
	if seed.Spec.InitialCluster != wantIC {
		t.Fatalf("seed initial-cluster mismatch: got %q want %q", seed.Spec.InitialCluster, wantIC)
	}
	if !seed.Spec.Bootstrap {
		t.Fatalf("seed member must be marked Bootstrap=true")
	}
	// The seed is pre-stamped IsVoter=true at create time (it forms the
	// cluster on its own — never a learner). Losing the stamp wedges every
	// later scale-up: learner memberID discovery and promotion dial voters
	// only.
	if !seed.Status.IsVoter {
		t.Fatalf("seed Status.IsVoter must be pre-stamped true at create; got false")
	}
}

// TestBootstrap_SeedIsVoterStampSurvivesConcurrentStatusWriter pins the
// merge-patch choice in the seed IsVoter pre-stamp
// (etcdcluster_controller.go bootstrap). The member controller starts
// reconciling the seed the moment Create returns, and its own
// Status().Update bumps the resourceVersion concurrently with this stamp.
// An optimistic-locked Status().Update here loses that race with a
// Conflict, and — because the stamp lives only in the create branch — the
// seed would stay IsVoter=false forever. The interceptor models the
// concurrent writer by failing every optimistic-locked EtcdMember status
// Update with a Conflict; the merge patch carries no resourceVersion and
// must land regardless. This test fails against the previous
// Status().Update implementation.
func TestBootstrap_SeedIsVoterStampSurvivesConcurrentStatusWriter(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
	}
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(cluster).
		WithStatusSubresource(&lll.EtcdCluster{}, &lll.EtcdMember{}, &lll.EtcdSnapshot{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, isMember := obj.(*lll.EtcdMember); isMember && sub == "status" {
					return apierrors.NewConflict(
						schema.GroupResource{Group: lll.GroupVersion.Group, Resource: "etcdmembers"},
						obj.GetName(), errors.New("simulated concurrent status writer"))
				}
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()
	r := &EtcdClusterReconciler{Client: c, Scheme: s, EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	reconcileUntilStable(t, r, c, "test", "ns", 8)

	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("expected one seed member; got %d", len(members.Items))
	}
	if !members.Items[0].Status.IsVoter {
		t.Fatal("seed IsVoter stamp lost under a concurrent status writer; the stamp must be a merge patch, not an optimistic-locked Update")
	}
}

// TestObservedSpec_LocksMidFlight pins the locking pattern's correctness
// outside the bootstrap flow specifically. With single-member bootstrap the
// initial-cluster-coordination race is gone, but the locking pattern still
// matters: observed.Replicas must not change while a reconcile is in
// progress (e.g. mid scale-up).
func TestObservedSpec_LocksMidFlight(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	// One member exists, two more pending — clearly mid scale-up.
	c, _ := newTestClient(t, cluster, &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", InitialCluster: "x", Bootstrap: true},
		Status:     lll.EtcdMemberStatus{PodName: "test-0", MemberID: "abc", Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: metav1.Now()}}},
	})

	// User flips spec.replicas to 7 mid-flight.
	mustGet(t, c, "test", "ns", cluster)
	cluster.Spec.Replicas = ptrInt32(7)
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatalf("Update: %v", err)
	}

	fe := newFakeEtcd(0xdeadbeef, &etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}})
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed.Replicas != 3 {
		t.Fatalf("observed.Replicas changed mid-flight (locking broken): %d", cluster.Status.Observed.Replicas)
	}
}

// TestDeadlineExceeded_BootstrapTerminal: an expired deadline before the
// cluster has formed (ClusterID is empty) is a hard terminal error. The
// operator must not adopt a new target — pods of the partially-bootstrapped
// members carry an --initial-cluster value that can't be changed in place,
// so creating new members with a different --initial-cluster would prevent
// the cluster from ever forming. Recovery is delete-and-recreate.
func TestDeadlineExceeded_BootstrapTerminal(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(5), // user already tried to "fix"
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			// ClusterID intentionally empty — bootstrap hasn't completed.
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(-1)}, // expired
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef)),
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed == nil || cluster.Status.Observed.Replicas != 3 {
		t.Fatalf("observed must NOT change during bootstrap deadline-exceeded; got %+v", cluster.Status.Observed)
	}
	var avail *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterAvailable {
			avail = &cluster.Status.Conditions[i]
		}
	}
	if avail == nil || avail.Status != metav1.ConditionFalse || avail.Reason != "BootstrapFailed" {
		t.Fatalf("Available condition = %+v; want False/BootstrapFailed", avail)
	}
}

// TestDeadlineExceeded_SteadyStateWaitsForSpecUpdate: after bootstrap, an
// expired deadline with spec == observed parks the operator in a terminal
// state until the user updates spec. observed must not auto-pivot.
func TestDeadlineExceeded_SteadyStateWaitsForSpecUpdate(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(7),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			ClusterID:    "deadbeef", // bootstrapped
			Observed: &lll.ObservedClusterSpec{
				Replicas: 7, // spec == observed, deadline still expired
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(-1)},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef)),
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed.Replicas != 7 {
		t.Fatalf("observed must not change while spec == observed; got %+v", cluster.Status.Observed)
	}
	var avail *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterAvailable {
			avail = &cluster.Status.Conditions[i]
		}
	}
	if avail == nil || avail.Status != metav1.ConditionFalse || avail.Reason != "DeadlineExceeded" {
		t.Fatalf("Available condition = %+v; want False/DeadlineExceeded", avail)
	}
}

// TestDeadlineExceeded_SteadyStateRetriesOnSpecUpdate: after bootstrap, an
// expired deadline with spec != observed is read as user intervention. The
// operator snapshots the new spec into observed and resumes.
func TestDeadlineExceeded_SteadyStateRetriesOnSpecUpdate(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(5), // user just edited spec down
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 100, // failed scale-up target
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(-1)},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef)),
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed.Replicas != 5 {
		t.Fatalf("observed should adopt new spec (5) after spec edit; got %+v", cluster.Status.Observed)
	}
	if cluster.Status.ProgressDeadline == nil {
		t.Fatalf("new deadline should have been set on retry")
	}
	var prog *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterProgressing {
			prog = &cluster.Status.Conditions[i]
		}
	}
	if prog == nil || prog.Status != metav1.ConditionTrue || prog.Reason != "RetryAfterDeadline" {
		t.Fatalf("Progressing condition = %+v; want True/RetryAfterDeadline", prog)
	}
}

// TestTryDiscoverCluster_SurfacesUnreachableEtcd covers reviewer issue #4:
// when the etcd client cannot be built or MemberList errors, the controller
// must log it and surface a Available=False condition with a meaningful
// reason so users see what's wrong.
func TestTryDiscoverCluster_SurfacesUnreachableEtcd(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(60 * 60 * 1e9)},
		},
	}
	// Pre-create three EtcdMembers so tryDiscoverCluster has endpoints.
	for i := 0; i < 3; i++ {
		m := &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%d", i),
				Namespace: "ns",
				Labels:    memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			Spec: lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
		}
		_ = m
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%d", i),
				Namespace: "ns",
				Labels:    memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			// test-0 is the seed (Bootstrap=true); the others are scale-up
			// peers. With GenerateName the names are arbitrary — the
			// Bootstrap flag is what makes test-0 discoverable as the seed.
			Spec: lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test", Bootstrap: i == 0},
			// PodName set: we want to exercise the "etcd unreachable" branch,
			// not the new "waiting for seed pod" gate.
			Status: lll.EtcdMemberStatus{PodName: fmt.Sprintf("test-%d", i)},
		})
	}
	c, _ := newTestClient(t, objs...)
	r := &EtcdClusterReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: failingFactory(errors.New("dial timeout")),
	}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mustGet(t, c, "test", "ns", cluster)
	var available *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterAvailable {
			available = &cluster.Status.Conditions[i]
		}
	}
	if available == nil {
		t.Fatalf("no Available condition set on cluster; want Available=False with ClusterUnreachable reason")
	}
	if available.Status != metav1.ConditionFalse {
		t.Fatalf("Available status = %v, want False", available.Status)
	}
	if available.Reason != "ClusterUnreachable" {
		t.Fatalf("Available reason = %q, want ClusterUnreachable", available.Reason)
	}
}

// A cluster whose etcd demands auth (e.g. an auth-enabled snapshot was
// restored) but with spec.auth unset can never be managed: the operator dials
// anonymously and is rejected, and auth is immutable post-create. Instead of
// looping on an opaque ClusterUnreachable, the controller must surface a
// specific, actionable Degraded condition and stop retrying. (Reviewer blocker:
// auth-omitted restore wedge.)
func TestTryDiscoverCluster_AuthRequiredButNotConfigured(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			// spec.auth deliberately unset — the operator has no creds to present.
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			// ClusterID NOT latched: discovery is still dialing.
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(60 * 60 * 1e9)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%d", i),
				Namespace: "ns",
				Labels:    memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			Spec:   lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test", Bootstrap: i == 0},
			Status: lll.EtcdMemberStatus{PodName: fmt.Sprintf("test-%d", i)},
		})
	}
	c, _ := newTestClient(t, objs...)
	// MemberList rejects the anonymous dial as an auth-enabled etcd would.
	fe := newFakeEtcd(0xdeadbeef)
	fe.listErr = errors.New("etcdserver: user name is empty")
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Must NOT keep retrying — nothing the operator does recovers it.
	if res.RequeueAfter > 0 {
		t.Errorf("expected no requeue (immutable auth misconfig), got %+v", res)
	}

	mustGet(t, c, "test", "ns", cluster)
	byType := map[string]metav1.Condition{}
	for _, cond := range cluster.Status.Conditions {
		byType[cond.Type] = cond
	}
	avail, ok := byType[lll.ClusterAvailable]
	if !ok || avail.Status != metav1.ConditionFalse || avail.Reason != "AuthRequiredNotConfigured" {
		t.Errorf("Available = %+v, want False/AuthRequiredNotConfigured", avail)
	}
	deg, ok := byType[lll.ClusterDegraded]
	if !ok || deg.Status != metav1.ConditionTrue || deg.Reason != "AuthRequiredNotConfigured" {
		t.Errorf("Degraded = %+v, want True/AuthRequiredNotConfigured", deg)
	}
}

// When spec.auth IS configured but etcd rejects the credentials (the wrong
// root password — the most likely restore mistake), the controller must surface
// a specific AuthCredentialsRejected condition, NOT loop on an opaque
// ClusterUnreachable. Unlike the unconfigured case this is recoverable by
// fixing the Secret's password, so it keeps requeueing.
func TestTryDiscoverCluster_AuthCredentialsRejected(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Auth: &lll.AuthSpec{
				Enabled:                  true,
				RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "root-creds"},
			},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(60 * 60 * 1e9)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test", Bootstrap: i == 0},
			Status:     lll.EtcdMemberStatus{PodName: fmt.Sprintf("test-%d", i)},
		})
	}
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	fe.listErr = rpctypes.ErrAuthFailed // etcd rejected the presented credentials
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	// Recoverable by fixing the Secret — keep requeueing.
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue for the recoverable wrong-password case, got %+v", res)
	}
	mustGet(t, c, "test", "ns", cluster)
	byType := map[string]metav1.Condition{}
	for _, cond := range cluster.Status.Conditions {
		byType[cond.Type] = cond
	}
	if avail, ok := byType[lll.ClusterAvailable]; !ok || avail.Reason != "AuthCredentialsRejected" {
		t.Errorf("Available = %+v, want reason AuthCredentialsRejected", avail)
	}
	if deg, ok := byType[lll.ClusterDegraded]; !ok || deg.Status != metav1.ConditionTrue || deg.Reason != "AuthCredentialsRejected" {
		t.Errorf("Degraded = %+v, want True/AuthCredentialsRejected", deg)
	}
}

// TestUpdateStatus_SurfacesBrokenCount covers reviewer issue #6: the isBroken
// stub must have a tested call site so the predicate is actually exercised.
// Today it always returns false, so the count must always be 0 — this test
// pins that contract until the policy lands.
func TestUpdateStatus_SurfacesBrokenCount(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(60 * 60 * 1e9)},
		},
	}
	objs := []client.Object{cluster}
	// Three members all Ready=True
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%d", i),
				Namespace: "ns",
				Labels:    memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			Spec: lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
			Status: lll.EtcdMemberStatus{
				MemberID: "abc",
				Conditions: []metav1.Condition{{
					Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady",
					LastTransitionTime: metav1.Now(),
				}},
			},
		})
	}
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{
		Client:            c,
		Scheme:            testScheme(t),
		EtcdClientFactory: factoryReturning(fe),
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.BrokenMembers != 0 {
		t.Fatalf("BrokenMembers = %d; stub predicate always returns false, so should be 0", cluster.Status.BrokenMembers)
	}
	if cluster.Status.ReadyMembers != 3 {
		t.Fatalf("ReadyMembers = %d, want 3", cluster.Status.ReadyMembers)
	}
}

// TestScaleUp_AdoptsPendingCRAndRetriesAdd covers crash recovery between
// the EtcdMember Create and MemberAddAsLearner: the next reconcile finds
// a pending CR (empty Spec.InitialCluster) and must (a) adopt it instead
// of Creating another, and (b) call MemberAddAsLearner with the pending
// CR's peer URL (because etcd has no record of it yet).
//
// This is the new shape of "idempotent after crash" for the
// Create-then-Add flow. The symmetric post-Add recovery is covered by
// TestScaleUp_InitialClusterMatchesEtcdMembership.
func TestScaleUp_AdoptsPendingCRAndRetriesAdd(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(4),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 4,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%d", i),
				Namespace: "ns",
				Labels:    memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			Spec:   lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
			Status: lll.EtcdMemberStatus{PodName: fmt.Sprintf("test-%d", i)},
		})
	}
	// Pending CR from a prior reconcile that crashed before MemberAddAsLearner.
	tru := true
	pending := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pndng", Namespace: "ns", Labels: clusterLabels("test"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdCluster",
				Name: "test", UID: types.UID("cluster-uid"), Controller: &tru,
			}},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			ClusterToken: "test",
			// InitialCluster intentionally empty.
		},
	}
	objs = append(objs, pending)
	c, _ := newTestClient(t, objs...)

	// Etcd has only the three pre-existing members; the pending CR's peer
	// URL is NOT yet registered.
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa03, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}},
	)

	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	memberList := &lll.EtcdMemberList{}
	if err := c.List(ctx, memberList); err != nil {
		t.Fatalf("List: %v", err)
	}
	if _, err := r.scaleUp(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}

	if len(fe.addCalls) != 0 {
		t.Fatalf("scale-up must use AddAsLearner, not voting MemberAdd; got %v", fe.addCalls)
	}
	if len(fe.addLearnerCalls) != 1 || fe.addLearnerCalls[0] != peerURL("http", "test-pndng", "test", "ns") {
		t.Fatalf("expected one MemberAddAsLearner against the pending CR's peer URL; got %v", fe.addLearnerCalls)
	}
	// Pending CR must still exist (adopted, not duplicated) and must now
	// have Spec.InitialCluster populated.
	all := &lll.EtcdMemberList{}
	_ = c.List(ctx, all, client.InNamespace("ns"))
	if len(all.Items) != 4 {
		t.Fatalf("expected 4 EtcdMembers (no duplicate created); got %d", len(all.Items))
	}
	got := &lll.EtcdMember{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test-pndng"}, got); err != nil {
		t.Fatalf("pending CR disappeared: %v", err)
	}
	if got.Spec.InitialCluster == "" {
		t.Fatalf("pending CR's Spec.InitialCluster was not patched")
	}
}

// TestBootstrap_Idempotent: if bootstrap re-runs (controller restart between
// Create and the next reconcile), it must not duplicate the seed member.
// With GenerateName, idempotency is anchored on Spec.Bootstrap=true rather
// than the historical name-collision path — bootstrap detects the existing
// seed via that flag and returns cleanly when InitialCluster is already set.
func TestBootstrap_Idempotent(t *testing.T) {
	ctx := context.Background()
	const clusterUID = "this-cluster-uid"
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Namespace: "ns", UID: types.UID(clusterUID),
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	seedName := "test-abc12"
	tru := true
	pre := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: seedName, Namespace: "ns", Labels: clusterLabels("test"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2",
				Kind:       "EtcdCluster",
				Name:       "test",
				UID:        types.UID(clusterUID),
				Controller: &tru,
			}},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Bootstrap: true, InitialCluster: buildInitialCluster("http", []string{seedName}, "test", "ns"),
			ClusterToken: "ns-test-x",
		},
	}
	c, _ := newTestClient(t, cluster, pre)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	if _, err := r.bootstrap(ctx, cluster, []lll.EtcdMember{*pre}); err != nil {
		t.Fatalf("bootstrap should be idempotent across restart: %v", err)
	}
	members := &lll.EtcdMemberList{}
	_ = c.List(ctx, members)
	if len(members.Items) != 1 {
		t.Fatalf("expected exactly one member, got %d", len(members.Items))
	}
}

// TestBootstrap_CompletesPendingSeed: if bootstrap crashes between Create
// and the InitialCluster Patch, the next reconcile sees a Bootstrap=true
// CR with empty Spec.InitialCluster and must finish wiring it (so the
// member controller can start the pod). No new seed should be created.
func TestBootstrap_CompletesPendingSeed(t *testing.T) {
	ctx := context.Background()
	const clusterUID = "fresh-uid"
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Namespace: "ns", UID: types.UID(clusterUID),
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	tru := true
	seedName := "test-pend1"
	pending := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: seedName, Namespace: "ns", Labels: clusterLabels("test"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2",
				Kind:       "EtcdCluster",
				Name:       "test",
				UID:        types.UID(clusterUID),
				Controller: &tru,
			}},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Bootstrap:    true, // crash-recovery: InitialCluster intentionally empty
			ClusterToken: "ns-test-x",
		},
	}
	c, _ := newTestClient(t, cluster, pending)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	if _, err := r.bootstrap(ctx, cluster, []lll.EtcdMember{*pending}); err != nil {
		t.Fatalf("bootstrap completion: %v", err)
	}
	members := &lll.EtcdMemberList{}
	_ = c.List(ctx, members)
	if len(members.Items) != 1 {
		t.Fatalf("expected exactly one member after completion; got %d", len(members.Items))
	}
	got := members.Items[0]
	wantIC := buildInitialCluster("http", []string{seedName}, "test", "ns")
	if got.Spec.InitialCluster != wantIC {
		t.Fatalf("pending seed InitialCluster not filled: got %q want %q", got.Spec.InitialCluster, wantIC)
	}
}

// TestBootstrap_RejectsStaleSeed: a Bootstrap=true EtcdMember whose owner
// reference points at a different UID must NOT be silently adopted. With
// GenerateName the historical "same-name across cluster incarnations"
// collision is gone, but an unattached or cross-owned Bootstrap CR
// (orphan from a botched migration, kubectl-created garbage) could still
// label-match the new cluster's selector. Bootstrap must error so the
// stale resource is investigated rather than co-opted.
func TestBootstrap_RejectsStaleSeed(t *testing.T) {
	ctx := context.Background()
	freshCluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Namespace: "ns", UID: types.UID("fresh-uid"),
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	tru := true
	stale := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-stalez", Namespace: "ns", Labels: clusterLabels("test"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2",
				Kind:       "EtcdCluster",
				Name:       "test",
				UID:        types.UID("stale-uid"), // different from fresh
				Controller: &tru,
			}},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Bootstrap: true, InitialCluster: buildInitialCluster("http", []string{"test-stalez"}, "test", "ns"),
			ClusterToken: "ns-test-x",
		},
	}
	c, _ := newTestClient(t, freshCluster, stale)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	_, err := r.bootstrap(ctx, freshCluster, []lll.EtcdMember{*stale})
	if err == nil {
		t.Fatalf("bootstrap should refuse to adopt a stale seed; got nil error")
	}
}

// TestScaleUp_WaitsForInFlightDeletion is the symmetric case of #3: while
// one EtcdMember is being deleted (e.g. a stale member from external
// `kubectl delete`), scale-up must not interleave a MemberAdd against the
// same etcd cluster the finalizer is running MemberRemove against.
func TestScaleUp_WaitsForInFlightDeletion(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(5),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 5, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	now := metav1.Now()
	// 4 healthy, 1 deleting.
	for i := 0; i < 5; i++ {
		m := &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("test-%d", i), Namespace: "ns",
				Labels: memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			Spec:   lll.EtcdMemberSpec{ClusterName: "test"},
			Status: lll.EtcdMemberStatus{PodName: fmt.Sprintf("test-%d", i), MemberID: "abc", Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}}},
		}
		if i == 4 {
			m.DeletionTimestamp = &now
			m.Finalizers = []string{MemberFinalizer}
		}
		objs = append(objs, m)
	}
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter while waiting for in-flight deletion; got %+v", res)
	}
	if len(fe.addCalls) > 0 {
		t.Fatalf("MemberAdd should not be called while a deletion is in flight; got %v", fe.addCalls)
	}
}

// TestScaleDown_PicksMostRecentlyCreatedVictim: with apiserver-assigned
// names there is no ordinal to scale down by; victim selection sorts by
// CreationTimestamp (newest first). This naturally retires the most
// recent scale-up step first — the just-added learner, not the seed —
// when the user shrinks the cluster.
//
// Tiebreak is by name DESC; the assertion below picks two members at
// the same timestamp so the deterministic-tiebreak behaviour gets
// exercised too.
func TestScaleDown_PicksMostRecentlyCreatedVictim(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	oldT := metav1.NewTime(now.Add(-2 * time.Hour))
	midT := metav1.NewTime(now.Add(-1 * time.Hour))
	// Three members, oldest -> newest: "test-seed" (oldest), "test-mid",
	// then two with the same CreationTimestamp where "test-zzz" > "test-aaa"
	// by name, so "test-zzz" should win the tiebreak.
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	members := []lll.EtcdMember{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "test-seed", Namespace: "ns", CreationTimestamp: oldT, Labels: memberLabels("test", "test-seed")},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x", Bootstrap: true},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "test-mid", Namespace: "ns", CreationTimestamp: midT, Labels: memberLabels("test", "test-mid")},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "test-aaa", Namespace: "ns", CreationTimestamp: now, Labels: memberLabels("test", "test-aaa")},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "test-zzz", Namespace: "ns", CreationTimestamp: now, Labels: memberLabels("test", "test-zzz")},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
		},
	}
	objs := []client.Object{cluster}
	for i := range members {
		m := members[i]
		objs = append(objs, &m)
	}
	c, _ := newTestClient(t, objs...)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.scaleDown(ctx, cluster, members); err != nil {
		t.Fatalf("scaleDown: %v", err)
	}
	// With no finalizer set the fake client removes the object entirely; in
	// production the member's MemberFinalizer would leave a DeletionTimestamp
	// instead, but either way the assertion is "scaleDown picked this one".
	// test-zzz wins the same-timestamp tiebreak (name DESC).
	gone := func(name string) bool {
		err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: name}, &lll.EtcdMember{})
		return apierrors.IsNotFound(err)
	}
	if !gone("test-zzz") {
		t.Fatalf("test-zzz should have been picked (newest + name-DESC tiebreak)")
	}
	for _, n := range []string{"test-aaa", "test-mid", "test-seed"} {
		if gone(n) {
			t.Fatalf("member %s should NOT have been picked by scaleDown", n)
		}
	}
}

// TestScaleDown_OneToZeroPatchesDormant covers the scale-to-zero
// "pause" path. When desired=0 and only one member remains, scaleDown
// must Patch Spec.Dormant=true on the surviving member rather than
// Delete it. The member CR (and its PVC owner-ref) stays in place; the
// member controller's dormant gate handles deleting the Pod.
//
// This is the new mechanism replacing the old "delete CR + reparent PVC
// to cluster + status.DormantMember" dance. With the CR kept alive
// across the pause, there is no Create-by-fixed-name on resume, no
// PVC reparenting, no cross-resource cache race.
func TestScaleDown_OneToZeroPatchesDormant(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(0),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 0, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	seed := lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-seed1", Namespace: "ns",
			CreationTimestamp: metav1.Now(),
			Labels:            memberLabels("test", "test-seed1"),
		},
		Spec: lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x", Bootstrap: true},
	}
	c, _ := newTestClient(t, cluster, &seed)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.scaleDown(ctx, cluster, []lll.EtcdMember{seed}); err != nil {
		t.Fatalf("scaleDown: %v", err)
	}
	// The CR must still exist (no Delete on 1→0), with Spec.Dormant=true.
	got := mustGet(t, c, "test-seed1", "ns", &lll.EtcdMember{})
	if !got.Spec.Dormant {
		t.Fatalf("scaleDown 1→0 must Patch Spec.Dormant=true; got Dormant=%v", got.Spec.Dormant)
	}
	if !got.DeletionTimestamp.IsZero() {
		t.Fatalf("CR must NOT have a DeletionTimestamp after 1→0; the pause keeps the CR alive")
	}
}

// TestScaleUp_WakesFromDormant covers the scale-back-up path. When a
// dormant member exists and scaleUp runs, it must Patch
// Spec.Dormant=false on that member rather than create anything. The
// member controller's reconcile loop then brings the Pod back up
// against the unchanged PVC, and etcd resumes from its data dir with
// the same ClusterID and member ID.
//
// No etcd-client calls happen during wake — the cluster is still
// offline at the moment of the Patch.
func TestScaleUp_WakesFromDormant(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(1),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 1, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	dormant := lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-saved1", Namespace: "ns",
			Labels: memberLabels("test", "test-saved1"),
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: buildInitialCluster("http", []string{"test-saved1"}, "test", "ns"),
			ClusterToken:   "ns-test-x", Bootstrap: true, Dormant: true,
		},
	}
	c, _ := newTestClient(t, cluster, &dormant)
	fe := newFakeEtcd(0xdead)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.scaleUp(ctx, cluster, []lll.EtcdMember{dormant}); err != nil {
		t.Fatalf("scaleUp wake: %v", err)
	}
	// Wake must not hit etcd — the cluster is offline at this moment.
	if n := len(fe.addCalls) + len(fe.addLearnerCalls) + len(fe.promoteCalls) + len(fe.removeCalls); n != 0 {
		t.Fatalf("wake must not call any etcd RPC; got add=%v addLearner=%v promote=%v remove=%v",
			fe.addCalls, fe.addLearnerCalls, fe.promoteCalls, fe.removeCalls)
	}
	got := mustGet(t, c, "test-saved1", "ns", &lll.EtcdMember{})
	if got.Spec.Dormant {
		t.Fatalf("wake must Patch Spec.Dormant=false; still got Dormant=true")
	}
	// Other fields are preserved (no full overwrite).
	if got.Spec.Version != "3.5.17" || got.Spec.InitialCluster == "" {
		t.Fatalf("wake-from-dormant must preserve Spec.Version and Spec.InitialCluster; got %+v", got.Spec)
	}
}

// TestUpdateStatus_PausedClusterReportsPausedCondition: a paused cluster
// (spec.replicas=0, sole member dormant) must NOT report Available=True.
// The "ready == desired" arm of the health switch would trigger that
// with ready=0/desired=0, claiming a fully serving cluster when nothing
// is running. The Paused branch in updateStatus takes precedence and
// surfaces Available=False/Paused, with a message naming the parked
// PVC derived from the dormant member's name.
func TestUpdateStatus_PausedClusterReportsPausedCondition(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(0),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 0, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	dormant := lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-saved1", Namespace: "ns", Labels: memberLabels("test", "test-saved1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x", Bootstrap: true, Dormant: true},
	}
	c, _ := newTestClient(t, cluster, &dormant)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.updateStatus(ctx, cluster, []lll.EtcdMember{dormant}); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)

	condByType := func(name string) *metav1.Condition {
		for i := range cluster.Status.Conditions {
			if cluster.Status.Conditions[i].Type == name {
				return &cluster.Status.Conditions[i]
			}
		}
		return nil
	}
	av := condByType(lll.ClusterAvailable)
	if av == nil || av.Status != metav1.ConditionFalse || av.Reason != "Paused" {
		t.Fatalf("Available condition = %+v, want False/Paused", av)
	}
	prog := condByType(lll.ClusterProgressing)
	if prog == nil || prog.Status != metav1.ConditionFalse || prog.Reason != "Paused" {
		t.Fatalf("Progressing condition = %+v, want False/Paused", prog)
	}
	deg := condByType(lll.ClusterDegraded)
	if deg == nil || deg.Status != metav1.ConditionFalse || deg.Reason != "Paused" {
		t.Fatalf("Degraded condition = %+v, want False/Paused", deg)
	}
	if !strings.Contains(av.Message, "data-test-saved1") {
		t.Fatalf("dormant Paused message should name the parked PVC; got %q", av.Message)
	}
}

// TestUpdateStatus_PausedFreshZeroMessageDifferentiates: a cluster
// created with replicas=0 from scratch (no DormantMember, no PVC, no
// data) must NOT have a Paused message that claims data is being
// preserved — there is nothing to preserve. Dashboards and runbooks
// that key on the condition message would be misled otherwise.
func TestUpdateStatus_PausedFreshZeroMessageDifferentiates(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(0),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			// No ClusterID (never bootstrapped), no DormantMember (never
			// had a live member to park).
			Observed: &lll.ObservedClusterSpec{
				Replicas: 0, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.updateStatus(ctx, cluster, nil); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)

	var av *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterAvailable {
			av = &cluster.Status.Conditions[i]
		}
	}
	if av == nil {
		t.Fatalf("Available condition missing")
	}
	if av.Status != metav1.ConditionFalse || av.Reason != "Paused" {
		t.Fatalf("Available condition = %+v, want False/Paused", av)
	}
	if strings.Contains(av.Message, "data is preserved") {
		t.Fatalf("fresh-zero Paused message must NOT claim data preservation; got %q", av.Message)
	}
}

// TestUpdateStatus_PausedMessageHonestForMemoryMember verifies the
// Paused condition does not claim "data is preserved on PVC data-X"
// when the dormant member is memory-backed (no PVC ever existed; the
// tmpfs evaporated with the Pod). The CRD's CEL admission rule now
// rejects replicas=0+memory at Create/Update, but an operator upgrade
// landing on a pre-existing dormant memory cluster (which slipped in
// before the rule was installed) still reaches this branch — defence
// in depth. The condition message must not mislead runbooks or
// dashboards about durability in that case.
func TestUpdateStatus_PausedMessageHonestForMemoryMember(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(0),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "128Mi"), Medium: lll.StorageMediumMemory},
		},
		Status: lll.EtcdClusterStatus{
			Observed: &lll.ObservedClusterSpec{
				Replicas: 0,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "128Mi"), Medium: lll.StorageMediumMemory},
			},
		},
	}
	dormant := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "c-x", Namespace: "ns", Labels: memberLabels("c", "c-x")},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "c",
			Version:     "3.5.17",
			Storage:     lll.StorageSpec{Size: quickQty(t, "128Mi"), Medium: lll.StorageMediumMemory},
			Dormant:     true,
		},
	}
	c, _ := newTestClient(t, cluster, dormant)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.updateStatus(ctx, cluster, []lll.EtcdMember{*dormant}); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}
	got := mustGet(t, c, "c", "ns", &lll.EtcdCluster{})

	var av *metav1.Condition
	for i := range got.Status.Conditions {
		if got.Status.Conditions[i].Type == lll.ClusterAvailable {
			av = &got.Status.Conditions[i]
		}
	}
	if av == nil {
		t.Fatalf("Available condition missing")
	}
	if av.Status != metav1.ConditionFalse || av.Reason != "Paused" {
		t.Fatalf("Available condition = %+v, want False/Paused", av)
	}
	if strings.Contains(av.Message, "PVC data-") || strings.Contains(av.Message, "data is preserved") {
		t.Fatalf("memory-member Paused message must not claim a PVC or preserved data; got %q", av.Message)
	}
}

// TestReconcile_FreshZeroReplicasSkipsBootstrap: an EtcdCluster created
// with spec.replicas=0 from scratch must NOT bootstrap a transient seed.
// Without the short-circuit, the reconcile loop would create a seed
// EtcdMember, let etcd form a 1-node cluster, latch ClusterID, then
// immediately scale down to 0 and "pause" a cluster that was never
// actually used.
func TestReconcile_FreshZeroReplicasSkipsBootstrap(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("uid-1")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(0),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	// Run multiple reconciles to land past the token+observed snapshot
	// and into the bootstrap branch (which must short-circuit).
	reconcileUntilStable(t, r, c, "test", "ns", 6)

	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 0 {
		names := make([]string, 0, len(members.Items))
		for _, m := range members.Items {
			names = append(names, m.Name)
		}
		t.Fatalf("no EtcdMember should be created for a fresh replicas=0 cluster; got %v", names)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed == nil || cluster.Status.Observed.Replicas != 0 {
		t.Fatalf("observed.Replicas = %+v, want 0", cluster.Status.Observed)
	}
	if cluster.Status.ClusterID != "" {
		t.Fatalf("ClusterID should NOT be latched (no bootstrap happened); got %q", cluster.Status.ClusterID)
	}
}

// TestReconcile_FreshZeroToOneAdoptsNewSpec covers the related bug
// surfaced during smoke: when a cluster is created with replicas=0
// (paused from the start) and the user later sets replicas=1, the
// spec-change-adoption path must fire so the bootstrap branch can
// create a seed on the next reconcile. The previous check in
// reconciliationComplete required ClusterID!="" — but a fresh-zero
// cluster never bootstraps anything, so ClusterID stays empty
// forever, the "complete && !specEqualsObserved" branch never fires,
// and observed.Replicas never adopts the new spec.
func TestReconcile_FreshZeroToOneAdoptsNewSpec(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("uid-1")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(0),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	// Settle into the fresh-paused steady state: observed.replicas=0, no members.
	reconcileUntilStable(t, r, c, "test", "ns", 6)
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed == nil || cluster.Status.Observed.Replicas != 0 {
		t.Fatalf("pre-flip: observed.Replicas = %+v, want 0", cluster.Status.Observed)
	}

	// User scales up to 1.
	cluster.Spec.Replicas = ptrInt32(1)
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatalf("Update spec=1: %v", err)
	}

	// Run reconciles until something happens. Specifically: observed.Replicas
	// must adopt the new spec, and the bootstrap branch must create a seed
	// EtcdMember.
	reconcileUntilStable(t, r, c, "test", "ns", 6)

	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed == nil || cluster.Status.Observed.Replicas != 1 {
		t.Fatalf("after spec=1 + reconcile: observed.Replicas = %+v, want 1", cluster.Status.Observed)
	}
	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("expected exactly one seed EtcdMember after fresh-zero → 1; got %d", len(members.Items))
	}
	if !members.Items[0].Spec.Bootstrap {
		t.Fatalf("the lone member must be the bootstrap seed; got Bootstrap=%v", members.Items[0].Spec.Bootstrap)
	}
}

// TestReconcile_PausedDormantMessageNamesPVC covers the steady-state
// call-site: passing `running` instead of `active` to updateStatus
// strips the dormant member from the slice, so findDormantMember
// returns nil and the Paused branch falls back to the fresh-zero
// "no data has been written" message — wrong, because a dormant
// member with a preserved PVC clearly exists. The fix is to pass the
// full active set; updateStatus re-derives running internally.
func TestReconcile_PausedDormantMessageNamesPVC(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(0),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 0, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	dormant := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-saved1", Namespace: "ns",
			Labels: memberLabels("test", "test-saved1"),
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: buildInitialCluster("http", []string{"test-saved1"}, "test", "ns"),
			ClusterToken:   "ns-test-x", Bootstrap: true, Dormant: true,
		},
	}
	c, _ := newTestClient(t, cluster, dormant)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	var av *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterAvailable {
			av = &cluster.Status.Conditions[i]
		}
	}
	if av == nil {
		t.Fatalf("Available condition missing")
	}
	if av.Status != metav1.ConditionFalse || av.Reason != "Paused" {
		t.Fatalf("Available = %+v, want False/Paused", av)
	}
	if !strings.Contains(av.Message, "data-test-saved1") {
		t.Fatalf("Paused message must name the parked PVC; got %q", av.Message)
	}
}

// TestReconcile_DormantToOneAdoptsNewSpec covers a regression caught
// during smoke: when the cluster is paused (one dormant member,
// observed.Replicas=0) and the user sets spec.replicas=1,
// reconciliationComplete must return true so the spec-change-adoption
// path snapshots the new spec and the wake step can fire. The check
// uses the `running` (non-dormant) member count — passing `active`
// would count the dormant CR against the target and `1 != 0` would
// keep the cluster wedged forever.
func TestReconcile_DormantToOneAdoptsNewSpec(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(1),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 0, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	dormant := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-saved1", Namespace: "ns",
			Labels: memberLabels("test", "test-saved1"),
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: buildInitialCluster("http", []string{"test-saved1"}, "test", "ns"),
			ClusterToken:   "ns-test-x", Bootstrap: true, Dormant: true,
		},
	}
	c, _ := newTestClient(t, cluster, dormant)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	// First reconcile adopts the new spec (observed.Replicas: 0 → 1).
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.Observed == nil || cluster.Status.Observed.Replicas != 1 {
		t.Fatalf("observed.Replicas should adopt spec=1; got %+v", cluster.Status.Observed)
	}

	// Second reconcile reaches scaleUp and Patches Dormant=false.
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile (wake): %v", err)
	}
	got := mustGet(t, c, "test-saved1", "ns", &lll.EtcdMember{})
	if got.Spec.Dormant {
		t.Fatalf("dormant member should have Spec.Dormant=false after wake; still got true")
	}
}

// TestScaleDown_WaitsForInFlightDeletion covers reviewer issue #3: while one
// EtcdMember is being deleted (DeletionTimestamp set, finalizer pending), no
// further deletion may be issued. Concurrent finalizers running MemberRemove
// against stale snapshots of the etcd cluster can race past quorum.
func TestScaleDown_WaitsForInFlightDeletion(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(1),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 1,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	now := metav1.Now()
	// 5 EtcdMembers: test-0 through test-4. test-4 is being deleted.
	for i := 0; i < 5; i++ {
		m := &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%d", i),
				Namespace: "ns",
				Labels:    memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			Spec:   lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test"},
			Status: lll.EtcdMemberStatus{PodName: fmt.Sprintf("test-%d", i), MemberID: "abc", Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}}},
		}
		if i == 4 {
			m.DeletionTimestamp = &now
			m.Finalizers = []string{MemberFinalizer}
		}
		objs = append(objs, m)
	}
	c, _ := newTestClient(t, objs...)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter while waiting for in-flight deletion; got %+v", res)
	}

	// Assert no other member got a DeletionTimestamp.
	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList)
	for _, m := range memberList.Items {
		if m.Name == "test-4" {
			continue
		}
		if !m.DeletionTimestamp.IsZero() {
			t.Fatalf("member %s was unexpectedly deleted while test-4 deletion in flight", m.Name)
		}
	}
}

// TestSetClusterCondition_PopulatesObservedGeneration covers reviewer issue
// #8: conditions must carry the cluster's current Generation so consumers
// can tell whether the condition reflects the latest spec.
func TestSetClusterCondition_PopulatesObservedGeneration(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", Generation: 7},
	}
	setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionTrue, "TestReason", "test")
	if len(cluster.Status.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(cluster.Status.Conditions))
	}
	if cluster.Status.Conditions[0].ObservedGeneration != 7 {
		t.Fatalf("ObservedGeneration = %d, want 7", cluster.Status.Conditions[0].ObservedGeneration)
	}
}

// TestSetClusterCondition_ReturnsFalseIfNoChange covers reviewer issue #5:
// the helper signals when nothing actually changed so callers can skip the
// Status().Update and avoid a write-storm in terminal state.
func TestSetClusterCondition_ReturnsFalseIfNoChange(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", Generation: 1},
	}
	if !setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionTrue, "Reason", "msg") {
		t.Fatalf("first call should report changed=true")
	}
	if setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionTrue, "Reason", "msg") {
		t.Fatalf("second identical call should report changed=false")
	}
	if !setClusterCondition(cluster, lll.ClusterAvailable, metav1.ConditionTrue, "Reason", "different message") {
		t.Fatalf("call with different message should report changed=true")
	}
}

// TestTryDiscoverCluster_RejectsPartialMembership covers reviewer issue #2:
// the cluster ID must only latch when MemberList returns exactly one member
// matching the seed. A response with a different name (a spurious peer,
// stale cache, etc.) must not be anchored.
func TestTryDiscoverCluster_RejectsPartialMembership(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Bootstrap: true},
		Status:     lll.EtcdMemberStatus{PodName: "test-0"},
	}
	c, _ := newTestClient(t, cluster, seed)
	// fakeEtcd returns a member with the wrong name.
	fe := newFakeEtcd(0xdeadbeef, &etcdserverpb.Member{
		ID: 0xff, Name: "wrong-name", PeerURLs: []string{peerURL("http", "wrong-name", "test", "ns")},
	})
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))
	if _, err := r.tryDiscoverCluster(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("tryDiscoverCluster: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.ClusterID != "" {
		t.Fatalf("ClusterID should NOT be latched when MemberList returns an unexpected member; got %q", cluster.Status.ClusterID)
	}
}

// TestTryDiscoverCluster_LatchesOnValidResponse: with the right seed name
// and exactly one member returned, ClusterID is recorded in %016x form.
func TestTryDiscoverCluster_LatchesOnValidResponse(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Bootstrap: true},
		Status:     lll.EtcdMemberStatus{PodName: "test-0"},
	}
	c, _ := newTestClient(t, cluster, seed)
	fe := newFakeEtcd(0xabc, &etcdserverpb.Member{
		ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")},
	})
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))
	if _, err := r.tryDiscoverCluster(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("tryDiscoverCluster: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.ClusterID != "0000000000000abc" {
		t.Fatalf("ClusterID should be zero-padded hex; got %q", cluster.Status.ClusterID)
	}
}

// TestDeadlineExceeded_NoChurnWhenSteady covers reviewer issue #5: while
// parked in a terminal state the controller must not write status on every
// reconcile.
func TestDeadlineExceeded_NoChurnWhenSteady(t *testing.T) {
	now := metav1.Now()
	past := metav1.NewTime(now.Add(-time.Hour))
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &past,
			Conditions: []metav1.Condition{
				{Type: lll.ClusterProgressing, Status: metav1.ConditionFalse, Reason: "BootstrapFailed", Message: "bootstrap deadline exceeded; delete the cluster and recreate to recover", LastTransitionTime: now},
				{Type: lll.ClusterAvailable, Status: metav1.ConditionFalse, Reason: "BootstrapFailed", Message: "could not bootstrap 3-member cluster within deadline", LastTransitionTime: now},
			},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	rvBefore := mustGet(t, c, "test", "ns", &lll.EtcdCluster{}).ResourceVersion
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != 0 || res.Requeue {
		t.Fatalf("terminal state must return no requeue; got %+v", res)
	}
	rvAfter := mustGet(t, c, "test", "ns", &lll.EtcdCluster{}).ResourceVersion
	if rvBefore != rvAfter {
		t.Fatalf("ResourceVersion changed (%q -> %q) on no-op terminal reconcile", rvBefore, rvAfter)
	}
}

// TestTryDiscoverCluster_FindsSeedByBootstrapFlag: even if list order puts
// a non-seed member first, discovery must anchor to the Bootstrap=true CR.
// Names are apiserver-assigned via GenerateName, so seed identity comes
// from Spec.Bootstrap, not from a predictable name. Without this anchor,
// a sibling CR landing earlier in the slice would silently mis-target
// discovery.
func TestTryDiscoverCluster_FindsSeedByBootstrapFlag(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	// Non-seed sibling at slice index 0 (Bootstrap=false), seed at index 1.
	sibling := lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-sib1", Namespace: "ns", Labels: memberLabels("test", "test-sib1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
		Status:     lll.EtcdMemberStatus{PodName: "test-sib1"},
	}
	seed := lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-seed1", Namespace: "ns", Labels: memberLabels("test", "test-seed1")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Bootstrap: true, InitialCluster: "x"},
		Status:     lll.EtcdMemberStatus{PodName: "test-seed1"},
	}
	c, _ := newTestClient(t, cluster, &sibling, &seed)
	fe := newFakeEtcd(0xabc, &etcdserverpb.Member{
		ID: 0xa01, Name: seed.Name, PeerURLs: []string{peerURL("http", seed.Name, "test", "ns")},
	})
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.tryDiscoverCluster(ctx, cluster, []lll.EtcdMember{sibling, seed}); err != nil {
		t.Fatalf("tryDiscoverCluster: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.ClusterID != "0000000000000abc" {
		t.Fatalf("ClusterID should be latched via seed lookup; got %q", cluster.Status.ClusterID)
	}
}

// TestTryDiscoverCluster_EmptySliceDoesNotPanic covers B3: a future caller
// passing an empty slice must not panic the controller.
func TestTryDiscoverCluster_EmptySliceDoesNotPanic(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("tryDiscoverCluster panicked on empty slice: %v", r)
		}
	}()
	res, err := r.tryDiscoverCluster(ctx, cluster, nil)
	if err != nil {
		t.Fatalf("tryDiscoverCluster: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected requeue when no seed available; got %+v", res)
	}
}

// TestEnsureService_ReconcilesOwnedFields covers B7: drift on the fields
// the operator owns (selector, our ports, publishNotReadyAddresses) is
// reconciled, but user-added labels/annotations and extra ports survive.
func TestEnsureService_ReconcilesOwnedFields(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
	}
	// Pre-create the headless Service with a broken selector and an extra
	// label/port that should survive reconciliation.
	preexisting := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test",
			Namespace:   "ns",
			Labels:      map[string]string{"injected-by-webhook": "yes"},
			Annotations: map[string]string{"some-webhook.io/managed": "true"},
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: false, // wrong — drift to fix
			Selector:                 map[string]string{"unrelated": "wrong"},
			Ports: []corev1.ServicePort{
				// Extra user-added port — must survive.
				{Name: "metrics", Port: 9100, Protocol: corev1.ProtocolTCP},
			},
		},
	}
	c, _ := newTestClient(t, cluster, preexisting)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if err := r.ensureService(ctx, cluster, "test", corev1.ServiceSpec{
		ClusterIP:                corev1.ClusterIPNone,
		PublishNotReadyAddresses: true,
		Selector:                 map[string]string{LabelCluster: "test"},
		Ports: []corev1.ServicePort{
			{Name: "client", Port: 2379},
			{Name: "peer", Port: 2380},
		},
	}); err != nil {
		t.Fatalf("ensureService: %v", err)
	}

	got := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test"}, got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.Selector[LabelCluster] != "test" {
		t.Fatalf("Selector not restored: %+v", got.Spec.Selector)
	}
	if !got.Spec.PublishNotReadyAddresses {
		t.Fatalf("PublishNotReadyAddresses not restored")
	}
	// User additions must survive.
	if got.Labels["injected-by-webhook"] != "yes" {
		t.Fatalf("user label was stripped: %+v", got.Labels)
	}
	if got.Annotations["some-webhook.io/managed"] != "true" {
		t.Fatalf("user annotation was stripped: %+v", got.Annotations)
	}
	foundMetrics, foundClient, foundPeer := false, false, false
	for _, p := range got.Spec.Ports {
		switch p.Name {
		case "metrics":
			foundMetrics = true
		case "client":
			foundClient = p.Port == 2379
		case "peer":
			foundPeer = p.Port == 2380
		}
	}
	if !foundMetrics {
		t.Fatalf("user-added metrics port was stripped: %+v", got.Spec.Ports)
	}
	if !foundClient || !foundPeer {
		t.Fatalf("required ports not present: %+v", got.Spec.Ports)
	}
}

// TestReconcile_FirstPassInitsTokenAndObserved covers B8: a brand-new
// cluster should have BOTH clusterToken AND observed populated after one
// Reconcile pass, not two.
func TestReconcile_FirstPassInitsTokenAndObserved(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("uid-1")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.ClusterToken == "" {
		t.Fatalf("ClusterToken should be set after one reconcile pass")
	}
	if cluster.Status.Observed == nil {
		t.Fatalf("Observed should be set after one reconcile pass (combined with token init)")
	}
}

// TestScaleUp_WaitsForAllMembersReady covers blocker #1: scale-up must not
// fire MemberAddAsLearner while existing members are still NotReady. Doing
// so stacks joiners against a half-formed cluster.
func TestScaleUp_WaitsForAllMembersReady(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(5), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 5, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	// test-0 Ready, test-1 and test-2 NotReady (still joining).
	for i := 0; i < 3; i++ {
		status := metav1.ConditionFalse
		if i == 0 {
			status = metav1.ConditionTrue
		}
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
			Status: lll.EtcdMemberStatus{
				PodName:    fmt.Sprintf("test-%d", i),
				MemberID:   "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: status, Reason: "x", LastTransitionTime: now}},
			},
		})
	}
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(fe.addLearnerCalls) > 0 || len(fe.addCalls) > 0 {
		t.Fatalf("scale-up should not fire MemberAdd*/Promote while existing members are NotReady; got addCalls=%v addLearnerCalls=%v",
			fe.addCalls, fe.addLearnerCalls)
	}
}

// TestScaleUp_InitialClusterMatchesEtcdMembership covers crash recovery
// between MemberAddAsLearner and the Spec.InitialCluster patch. In the
// new flow the EtcdMember CR is Created *before* MemberAddAsLearner, so
// the leftover-state on the next reconcile is: a pending CR (empty
// Spec.InitialCluster) whose peer URL is already registered in etcd.
// scaleUp must (a) adopt the pending CR rather than create another, (b)
// detect the registered peer URL and skip the AddAsLearner call, (c)
// fill Spec.InitialCluster from etcd's authoritative view (which
// includes the still-Name-empty joiner) so the pod's --initial-cluster
// flag matches what etcd will report when it tries to join.
func TestScaleUp_InitialClusterMatchesEtcdMembership(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 4, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
			Status: lll.EtcdMemberStatus{
				PodName: fmt.Sprintf("test-%d", i), MemberID: "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
			},
		})
	}
	// Pending CR from a prior reconcile that crashed after MemberAddAsLearner
	// but before the InitialCluster patch. Owned by the cluster (mirrors
	// what SetControllerReference would have written).
	tru := true
	pending := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pndng", Namespace: "ns", Labels: clusterLabels("test"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdCluster",
				Name: "test", UID: types.UID("cluster-uid"), Controller: &tru,
			}},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			ClusterToken: "ns-test-x",
			// InitialCluster intentionally empty — that's the pending state.
		},
	}
	objs = append(objs, pending)
	c, _ := newTestClient(t, objs...)
	// Etcd already has 4 members. The pending member's peer URL is
	// registered (joiner not yet reported in, so etcd's Name=="").
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa03, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa04, Name: "", PeerURLs: []string{peerURL("http", "test-pndng", "test", "ns")}, IsLearner: true},
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	// The orphan learner CANNOT be promoted yet — its pod doesn't exist
	// (the member controller blocks on Spec.InitialCluster being set), so
	// it can't catch up with the leader. Any reasonable etcd returns
	// "learner not ready" here. Wire that error so the test pins the
	// "patch must happen *before* promote attempts" guarantee. Without
	// pending-completion-first ordering, scaleUp would short-circuit on
	// MemberPromote's error and deadlock the cluster.
	fe.promoteErr = errors.New("etcdserver: can only promote a learner member which is in sync with leader")
	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))
	if _, err := r.scaleUp(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	// MemberAddAsLearner must NOT have been called — peer URL is already in
	// etcd from the pre-crash reconcile.
	if len(fe.addLearnerCalls) != 0 {
		t.Fatalf("MemberAddAsLearner should not fire when peer URL already registered; got %v", fe.addLearnerCalls)
	}
	// MemberPromote must NOT have been called either — the orphan learner
	// is still un-synced and a promote attempt would error out, but the
	// pending-completion path runs first and short-circuits.
	if len(fe.promoteCalls) != 0 {
		t.Fatalf("MemberPromote must not fire while a pending CR exists; got %v", fe.promoteCalls)
	}
	// Pending CR's Spec.InitialCluster must now reflect etcd's full view
	// (including the recovered joiner).
	got := &lll.EtcdMember{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test-pndng"}, got); err != nil {
		t.Fatalf("pending EtcdMember disappeared: %v", err)
	}
	wantIC := buildInitialCluster("http", []string{"test-0", "test-1", "test-2", "test-pndng"}, "test", "ns")
	if got.Spec.InitialCluster != wantIC {
		t.Fatalf("Spec.InitialCluster mismatch:\n  got  %q\n  want %q", got.Spec.InitialCluster, wantIC)
	}
}

// TestScaleUp_RecoversFromCrashBetweenAddAndPatch is the integration-level
// counterpart of TestScaleUp_InitialClusterMatchesEtcdMembership: rather
// than calling scaleUp directly, it runs Reconcile against the
// post-crash state and asserts the deadlock cannot occur. The fake etcd
// is rigged so MemberPromote always errors (the orphan learner can
// never sync without its pod), so any reconcile path that promotes
// before patching wedges forever. The test passes only if Reconcile
// completes the InitialCluster patch.
func TestScaleUp_RecoversFromCrashBetweenAddAndPatch(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(4), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 4, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
			Status: lll.EtcdMemberStatus{
				PodName: fmt.Sprintf("test-%d", i), MemberID: "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
			},
		})
	}
	tru := true
	pending := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pndng", Namespace: "ns", Labels: clusterLabels("test"),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd-operator.cozystack.io/v1alpha2", Kind: "EtcdCluster",
				Name: "test", UID: types.UID("cluster-uid"), Controller: &tru,
			}},
		},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			ClusterToken: "ns-test-x",
		},
	}
	objs = append(objs, pending)
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa03, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa04, Name: "", PeerURLs: []string{peerURL("http", "test-pndng", "test", "ns")}, IsLearner: true},
	)
	// Promote always errors — the orphan learner is permanently stuck
	// until its pod comes up (which requires the InitialCluster patch).
	fe.promoteErr = errors.New("etcdserver: can only promote a learner member which is in sync with leader")
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := mustGet(t, c, "test-pndng", "ns", &lll.EtcdMember{})
	if got.Spec.InitialCluster == "" {
		t.Fatalf("pending CR's Spec.InitialCluster was not patched — deadlock would result in production")
	}
	if len(fe.addLearnerCalls) != 0 {
		t.Fatalf("MemberAddAsLearner should not fire when peer URL already registered; got %v", fe.addLearnerCalls)
	}
}

// TestScaleUp_AddsAsLearner covers blocker #1 happy path: when current <
// desired AND existing members are all Ready, scale-up uses
// MemberAddAsLearner (not voting MemberAdd).
func TestScaleUp_AddsAsLearner(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 2; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
			Status: lll.EtcdMemberStatus{
				PodName:    fmt.Sprintf("test-%d", i),
				MemberID:   "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
			},
		})
	}
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))
	if _, err := r.scaleUp(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	if len(fe.addCalls) > 0 {
		t.Fatalf("scale-up must use AddAsLearner, not voting MemberAdd; got addCalls=%v", fe.addCalls)
	}
	// With GenerateName the new member's name is apiserver-assigned, so
	// we can't predict the exact peer URL. Assert shape instead: exactly
	// one AddAsLearner against a "<cluster>-*" peer URL.
	if len(fe.addLearnerCalls) != 1 {
		t.Fatalf("expected one MemberAddAsLearner call; got %v", fe.addLearnerCalls)
	}
	gotURL := fe.addLearnerCalls[0]
	if !strings.HasPrefix(gotURL, "http://test-") || !strings.HasSuffix(gotURL, ".test.ns.svc:2380") {
		t.Fatalf("AddAsLearner peer URL %q does not match expected GenerateName shape", gotURL)
	}
	// And the new CR carries that peer URL via its assigned name.
	members := &lll.EtcdMemberList{}
	_ = c.List(ctx, members, client.InNamespace("ns"))
	if len(members.Items) != 3 {
		t.Fatalf("expected 3 EtcdMembers after scale-up (2 existing + 1 new); got %d", len(members.Items))
	}
}

// TestScaleUp_PromotesExistingLearner covers the second half of the learner
// flow: when etcd already has a learner (typical state on the iteration
// after MemberAddAsLearner + pod start), scale-up calls MemberPromote
// rather than adding another learner.
func TestScaleUp_PromotesExistingLearner(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 2; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
			Status: lll.EtcdMemberStatus{
				PodName:    fmt.Sprintf("test-%d", i),
				MemberID:   "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
			},
		})
	}
	c, _ := newTestClient(t, objs...)
	const learnerID uint64 = 0xbb01
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: learnerID, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}, IsLearner: true},
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))
	if _, err := r.scaleUp(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}
	if len(fe.promoteCalls) != 1 || fe.promoteCalls[0] != learnerID {
		t.Fatalf("expected MemberPromote(%x); got %v", learnerID, fe.promoteCalls)
	}
	if len(fe.addLearnerCalls) > 0 {
		t.Fatalf("must not add another learner while one is pending promotion; got %v", fe.addLearnerCalls)
	}
}

// TestReconcile_PromotesLearnerWhenCurrentEqualsDesired covers a subtle
// fallout of the learner flow: the iteration that adds the *last* learner
// reaches current==desired before the learner is promoted. If scale-up
// stops being entered at that point, the final learner stays unpromoted
// forever. Reconcile's steady-state path must call promotePendingLearner.
func TestReconcile_PromotesLearnerWhenCurrentEqualsDesired(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
			Status: lll.EtcdMemberStatus{
				PodName:    fmt.Sprintf("test-%d", i),
				MemberID:   "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
			},
		})
	}
	c, _ := newTestClient(t, objs...)
	const learnerID uint64 = 0xbb01
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: learnerID, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}, IsLearner: true},
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(fe.promoteCalls) != 1 || fe.promoteCalls[0] != learnerID {
		t.Fatalf("expected MemberPromote(%x) at steady state; got %v", learnerID, fe.promoteCalls)
	}
}

// TestReconcile_ShortCircuitsOnDeletion covers blocker #3: when the cluster
// has a DeletionTimestamp, Reconcile must not run ensureServices.
func TestReconcile_ShortCircuitsOnDeletion(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test", Namespace: "ns",
			DeletionTimestamp: &now,
			Finalizers:        []string{"keep-me-alive"}, // fake client refuses to track delete-with-zero-finalizers
		},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead))}

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	svcs := &corev1.ServiceList{}
	_ = c.List(ctx, svcs, client.InNamespace("ns"))
	if len(svcs.Items) != 0 {
		var names []string
		for _, s := range svcs.Items {
			names = append(names, s.Name)
		}
		t.Fatalf("Service should not be created for a Terminating cluster; got %v", names)
	}
}

// TestClusterUpdateStatus_NoChurnInSteadyState covers the review blocker:
// at steady state the cluster controller's updateStatus must not rewrite
// status every 30s. ResourceVersion stays stable across a no-op reconcile.
func TestClusterUpdateStatus_NoChurnInSteadyState(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "0000000000000abc",
			ReadyMembers: 3,
			Selector:     "etcd-operator.cozystack.io/cluster=test",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			Conditions: []metav1.Condition{
				{Type: lll.ClusterAvailable, Status: metav1.ConditionTrue, Reason: "QuorumHealthy", Message: "All members are ready", LastTransitionTime: now},
				{Type: lll.ClusterDegraded, Status: metav1.ConditionFalse, Reason: "QuorumHealthy", Message: "", LastTransitionTime: now},
				{Type: lll.ClusterProgressing, Status: metav1.ConditionFalse, Reason: "Reconciled", Message: "actual state matches status.observed", LastTransitionTime: now},
			},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test", InitialCluster: "x"},
			Status: lll.EtcdMemberStatus{
				PodName: fmt.Sprintf("test-%d", i), MemberID: "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
			},
		})
	}
	c, _ := newTestClient(t, objs...)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xabc,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa03, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}},
	))}

	rvBefore := mustGet(t, c, "test", "ns", &lll.EtcdCluster{}).ResourceVersion
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	rvAfter := mustGet(t, c, "test", "ns", &lll.EtcdCluster{}).ResourceVersion
	if rvBefore != rvAfter {
		t.Fatalf("ResourceVersion changed (%q -> %q) on a no-op steady-state reconcile", rvBefore, rvAfter)
	}
}

// TestTryDiscoverCluster_NoChurnOnSameError covers recommended item A:
// repeated retries against the same etcd-client error must not bump
// resourceVersion every time. The setClusterCondition return value gates
// the Status().Update.
func TestTryDiscoverCluster_NoChurnOnSameError(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Bootstrap: true},
		Status:     lll.EtcdMemberStatus{PodName: "test-0"},
	}
	c, _ := newTestClient(t, cluster, seed)
	r := &EtcdClusterReconciler{
		Client: c, Scheme: testScheme(t),
		EtcdClientFactory: failingFactory(errors.New("dial timeout")),
	}

	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))

	// First call writes the condition.
	if _, err := r.tryDiscoverCluster(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("tryDiscoverCluster: %v", err)
	}
	rvAfterFirst := mustGet(t, c, "test", "ns", &lll.EtcdCluster{}).ResourceVersion

	// Second call with the same error must NOT bump RV.
	mustGet(t, c, "test", "ns", cluster)
	if _, err := r.tryDiscoverCluster(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("tryDiscoverCluster (second): %v", err)
	}
	rvAfterSecond := mustGet(t, c, "test", "ns", &lll.EtcdCluster{}).ResourceVersion
	if rvAfterFirst != rvAfterSecond {
		t.Fatalf("resourceVersion changed on identical retry (%q -> %q)", rvAfterFirst, rvAfterSecond)
	}
}

// TestTryDiscoverCluster_WaitingForSeed covers recommended items B+F: when
// the seed's Pod hasn't been created yet, discovery must not dial etcd
// (would just time out) and must surface Progressing=True/WaitingForSeed —
// distinct from Available=False/ClusterUnreachable, which is for "pod is
// up but etcd isn't answering".
func TestTryDiscoverCluster_WaitingForSeed(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	// Seed exists as a CR but its pod hasn't been created yet (PodName empty).
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Bootstrap: true},
		// Status.PodName intentionally empty.
	}
	c, _ := newTestClient(t, cluster, seed)
	// Factory MUST NOT be invoked.
	r := &EtcdClusterReconciler{
		Client: c, Scheme: testScheme(t),
		EtcdClientFactory: failingFactory(errors.New("must not be called")),
	}

	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))
	res, err := r.tryDiscoverCluster(ctx, cluster, memberList.Items)
	if err != nil {
		t.Fatalf("tryDiscoverCluster: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Fatalf("expected RequeueAfter; got %+v", res)
	}
	mustGet(t, c, "test", "ns", cluster)
	var prog *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterProgressing {
			prog = &cluster.Status.Conditions[i]
		}
	}
	if prog == nil || prog.Status != metav1.ConditionTrue || prog.Reason != "WaitingForSeed" {
		t.Fatalf("Progressing condition = %+v, want True/WaitingForSeed", prog)
	}
}

// TestTryDiscoverCluster_LatchesAvailableTrueOnSuccess covers recommended
// item C: when discovery succeeds, the controller clears any stale
// Available=False condition that earlier retries wrote, so the cluster
// doesn't sit Available=False until the next reconcile cycle.
func TestTryDiscoverCluster_LatchesAvailableTrueOnSuccess(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
			Conditions: []metav1.Condition{{
				// Stale from a prior retry.
				Type: lll.ClusterAvailable, Status: metav1.ConditionFalse,
				Reason: "ClusterUnreachable", Message: "MemberList failed: dial timeout",
				LastTransitionTime: now,
			}},
		},
	}
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", Labels: memberLabels("test", "test-0")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Bootstrap: true},
		Status:     lll.EtcdMemberStatus{PodName: "test-0"},
	}
	c, _ := newTestClient(t, cluster, seed)
	fe := newFakeEtcd(0xabc, &etcdserverpb.Member{
		ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")},
	})
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	memberList := &lll.EtcdMemberList{}
	_ = c.List(ctx, memberList, client.InNamespace("ns"))
	if _, err := r.tryDiscoverCluster(ctx, cluster, memberList.Items); err != nil {
		t.Fatalf("tryDiscoverCluster: %v", err)
	}
	mustGet(t, c, "test", "ns", cluster)
	var avail *metav1.Condition
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == lll.ClusterAvailable {
			avail = &cluster.Status.Conditions[i]
		}
	}
	if avail == nil || avail.Status != metav1.ConditionTrue || avail.Reason != "ClusterDiscovered" {
		t.Fatalf("Available = %+v, want True/ClusterDiscovered", avail)
	}
}

// silence unused imports
var _ = etcdserverpb.Member{}

// TestBootstrap_PropagatesStorageMediumToSeed verifies the wiring:
// EtcdCluster.spec.storage.medium=Memory must end up on the seed
// EtcdMember's spec and on Status.Observed.Storage.Medium (so the
// locking pattern protects subsequent reconciles from a mid-flight
// medium flip).
//
// Without this propagation the member controller would silently default
// to a PVC volume even though the user asked for memory backing — a
// catastrophic silent failure for the feature.
func TestBootstrap_PropagatesStorageMediumToSeed(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "256Mi"), Medium: lll.StorageMediumMemory},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	reconcileUntilStable(t, r, c, "test", "ns", 8)

	// Observed snapshot must record the medium.
	got := mustGet(t, c, "test", "ns", &lll.EtcdCluster{})
	if got.Status.Observed == nil {
		t.Fatalf("Status.Observed must be set after first reconciles")
	}
	if got.Status.Observed.Storage.Medium != lll.StorageMediumMemory {
		t.Fatalf("Observed.Storage.Medium = %q, want %q", got.Status.Observed.Storage.Medium, lll.StorageMediumMemory)
	}

	// Seed EtcdMember must inherit the medium.
	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("expected exactly one seed member; got %d", len(members.Items))
	}
	if members.Items[0].Spec.Storage.Medium != lll.StorageMediumMemory {
		t.Fatalf("seed Spec.Storage.Medium = %q, want %q",
			members.Items[0].Spec.Storage.Medium, lll.StorageMediumMemory)
	}
}

// TestSpecEqualsObserved_StorageMediumMatters guards the locking pattern:
// flipping spec.storage.medium with an unchanged observed must be treated
// as "spec differs from observed" so the deadline gate fires when the
// in-flight target is reached. Without this the user could change the
// medium mid-flight and the operator would silently lock the old value
// permanently.
func TestSpecEqualsObserved_StorageMediumMatters(t *testing.T) {
	cluster := &lll.EtcdCluster{
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi"), Medium: lll.StorageMediumMemory},
		},
		Status: lll.EtcdClusterStatus{
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi"), Medium: lll.StorageMediumDefault}, // mismatch
			},
		},
	}
	if specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return false when storage.medium differs")
	}

	// Match restored — should be true.
	cluster.Status.Observed.Storage.Medium = lll.StorageMediumMemory
	if !specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return true when all fields including storage.medium match")
	}
}

// TestSpecEqualsObserved_ResourcesMatter guards the locking pattern for
// the new spec.resources field. A mid-flight resource bump must register
// as "spec ≠ observed" so the deadline gate gets a chance to fire when
// the in-flight target is reached, then snapshot the new sizing.
func TestSpecEqualsObserved_ResourcesMatter(t *testing.T) {
	cluster := &lll.EtcdCluster{
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("500m"),
				},
			},
		},
		Status: lll.EtcdClusterStatus{
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"), // mismatch
					},
				},
			},
		},
	}
	if specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return false when resources differ")
	}

	cluster.Status.Observed.Resources = *cluster.Spec.Resources.DeepCopy()
	if !specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return true when resources match")
	}
}

// TestSpecEqualsObserved_AdditionalMetadataMatters guards the locking
// pattern for spec.additionalMetadata: like its siblings (resources,
// affinity, topologySpreadConstraints) it is latched through
// status.observed, so a mid-flight metadata edit must register as
// "spec ≠ observed" to be snapshotted once the in-flight target lands.
func TestSpecEqualsObserved_AdditionalMetadataMatters(t *testing.T) {
	cluster := &lll.EtcdCluster{
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			AdditionalMetadata: &lll.AdditionalMetadata{
				Labels: map[string]string{"cozystack.io/tenant": "foo"},
			},
		},
		Status: lll.EtcdClusterStatus{
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
				// AdditionalMetadata nil — mismatch.
			},
		},
	}
	if specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return false when additionalMetadata differs")
	}

	cluster.Status.Observed.AdditionalMetadata = cluster.Spec.AdditionalMetadata.DeepCopy()
	if !specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return true when additionalMetadata matches")
	}
}

// TestSpecEqualsObserved_OptionsMatter guards the locking pattern for
// spec.options: like its siblings (resources, affinity, additionalMetadata)
// it is latched through status.observed, so a mid-flight tuning edit must
// register as "spec ≠ observed" to be snapshotted once the in-flight
// target lands.
func TestSpecEqualsObserved_OptionsMatter(t *testing.T) {
	quota := int64(10200547328)
	cluster := &lll.EtcdCluster{
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Options: &lll.EtcdOptions{
				QuotaBackendBytes:       &quota,
				AutoCompactionMode:      lll.AutoCompactionModePeriodic,
				AutoCompactionRetention: "5m",
			},
		},
		Status: lll.EtcdClusterStatus{
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
				// Options nil — mismatch.
			},
		},
	}
	if specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return false when options differ")
	}

	cluster.Status.Observed.Options = cluster.Spec.Options.DeepCopy()
	if !specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return true when options match")
	}
}

// TestBootstrap_PropagatesOptionsToSeed verifies the wiring of
// spec.options onto the seed EtcdMember at cluster creation. The member
// controller reads its own Spec.Options at buildPod time, so any
// propagation gap shows up as the seed's etcd running with built-in
// defaults instead of the user's tuning (e.g. no backend quota).
func TestBootstrap_PropagatesOptionsToSeed(t *testing.T) {
	ctx := context.Background()
	quota := int64(10200547328)
	snapCount := int64(10000)
	opts := &lll.EtcdOptions{
		QuotaBackendBytes:       &quota,
		AutoCompactionMode:      lll.AutoCompactionModePeriodic,
		AutoCompactionRetention: "5m",
		SnapshotCount:           &snapCount,
	}
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Options:  opts,
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	reconcileUntilStable(t, r, c, "test", "ns", 8)

	got := mustGet(t, c, "test", "ns", &lll.EtcdCluster{})
	if got.Status.Observed == nil {
		t.Fatalf("Status.Observed must be set after first reconciles")
	}
	if !equality.Semantic.DeepEqual(got.Status.Observed.Options, opts) {
		t.Errorf("Observed.Options = %+v; want %+v", got.Status.Observed.Options, opts)
	}

	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("expected exactly one seed member; got %d", len(members.Items))
	}
	if !equality.Semantic.DeepEqual(members.Items[0].Spec.Options, opts) {
		t.Fatalf("seed Spec.Options = %+v; want %+v", members.Items[0].Spec.Options, opts)
	}
}

// TestBootstrap_PropagatesResourcesToSeed verifies the wiring of
// spec.resources onto the seed EtcdMember at cluster creation. The
// member controller reads its own Spec.Resources at buildPod time, so
// any propagation gap shows up as the seed's container running with
// the operator's hardcoded fall-through defaults instead of the
// user's sizing.
func TestBootstrap_PropagatesResourcesToSeed(t *testing.T) {
	ctx := context.Background()
	wantCPU := resource.MustParse("750m")
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas:  ptrInt32(3),
			Version:   "3.5.17",
			Storage:   lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: wantCPU}},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	reconcileUntilStable(t, r, c, "test", "ns", 8)

	got := mustGet(t, c, "test", "ns", &lll.EtcdCluster{})
	if got.Status.Observed == nil {
		t.Fatalf("Status.Observed must be set after first reconciles")
	}
	if got.Status.Observed.Resources.Requests.Cpu().Cmp(wantCPU) != 0 {
		t.Fatalf("Observed.Resources.Requests.cpu = %v; want %v", got.Status.Observed.Resources.Requests.Cpu(), wantCPU)
	}

	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("expected exactly one seed member; got %d", len(members.Items))
	}
	if members.Items[0].Spec.Resources.Requests.Cpu().Cmp(wantCPU) != 0 {
		t.Fatalf("seed Spec.Resources.Requests.cpu = %v; want %v",
			members.Items[0].Spec.Resources.Requests.Cpu(), wantCPU)
	}
}

// TestBootstrap_PropagatesSchedulingAndMetadataToSeed covers the new
// scheduling/metadata fields: affinity and topologySpreadConstraints latch
// through Observed onto the seed member, and additionalMetadata is both
// copied onto the seed's spec (for the Pod) and merged onto the seed CR's
// own labels/annotations.
func TestBootstrap_PropagatesSchedulingAndMetadataToSeed(t *testing.T) {
	ctx := context.Background()
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
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas:                  ptrInt32(3),
			Version:                   "3.5.17",
			Storage:                   lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Affinity:                  aff,
			TopologySpreadConstraints: tsc,
			AdditionalMetadata: &lll.AdditionalMetadata{
				Labels:      map[string]string{"cozystack.io/tenant": "foo"},
				Annotations: map[string]string{"example.com/note": "bar"},
			},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	reconcileUntilStable(t, r, c, "test", "ns", 8)

	got := mustGet(t, c, "test", "ns", &lll.EtcdCluster{})
	if got.Status.Observed == nil {
		t.Fatalf("Status.Observed must be set after first reconciles")
	}
	if !equality.Semantic.DeepEqual(got.Status.Observed.Affinity, aff) {
		t.Errorf("Observed.Affinity = %+v; want %+v", got.Status.Observed.Affinity, aff)
	}
	if !equality.Semantic.DeepEqual(got.Status.Observed.TopologySpreadConstraints, tsc) {
		t.Errorf("Observed.TopologySpreadConstraints = %+v; want %+v", got.Status.Observed.TopologySpreadConstraints, tsc)
	}
	if got.Status.Observed.AdditionalMetadata == nil ||
		got.Status.Observed.AdditionalMetadata.Labels["cozystack.io/tenant"] != "foo" {
		t.Errorf("Observed.AdditionalMetadata not latched: %+v", got.Status.Observed.AdditionalMetadata)
	}

	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("expected exactly one seed member; got %d", len(members.Items))
	}
	seed := members.Items[0]
	if !equality.Semantic.DeepEqual(seed.Spec.Affinity, aff) {
		t.Errorf("seed Spec.Affinity = %+v; want %+v", seed.Spec.Affinity, aff)
	}
	if !equality.Semantic.DeepEqual(seed.Spec.TopologySpreadConstraints, tsc) {
		t.Errorf("seed Spec.TopologySpreadConstraints = %+v; want %+v", seed.Spec.TopologySpreadConstraints, tsc)
	}
	if seed.Spec.AdditionalMetadata == nil || seed.Spec.AdditionalMetadata.Labels["cozystack.io/tenant"] != "foo" {
		t.Errorf("seed Spec.AdditionalMetadata not propagated: %+v", seed.Spec.AdditionalMetadata)
	}
	if seed.Labels["cozystack.io/tenant"] != "foo" {
		t.Errorf("seed CR label not merged: cozystack.io/tenant = %q", seed.Labels["cozystack.io/tenant"])
	}
	if seed.Labels["app.kubernetes.io/managed-by"] != "etcd-operator" {
		t.Errorf("operator-owned seed label clobbered: app.kubernetes.io/managed-by = %q", seed.Labels["app.kubernetes.io/managed-by"])
	}
	if seed.Annotations["example.com/note"] != "bar" {
		t.Errorf("seed CR annotation not merged: example.com/note = %q", seed.Annotations["example.com/note"])
	}
}

// TestScaleUp_PropagatesResourcesToNewMember covers the second of the
// two member-creation sites: scaleUp's fresh-CR path. Bootstrap is
// already covered by TestBootstrap_PropagatesResourcesToSeed; this one
// exercises the operator's "no pending CR, no learner, scale up by one"
// arm to make sure the same Resources propagation lives there too.
func TestScaleUp_PropagatesResourcesToNewMember(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	wantCPU := resource.MustParse("750m")

	// Cluster targets 2 replicas; 1 ready voter already exists. scaleUp
	// must create a fresh second member carrying Observed.Resources.
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas:  ptrInt32(2),
			Version:   "3.5.17",
			Storage:   lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: wantCPU}},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas:  2,
				Version:   "3.5.17",
				Storage:   lll.StorageSpec{Size: quickQty(t, "1Gi")},
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: wantCPU}},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-seed", Namespace: "ns", Labels: memberLabels("test", "test-seed")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", InitialCluster: "x", ClusterToken: "ns-test-x"},
		Status: lll.EtcdMemberStatus{
			PodName: "test-seed", MemberID: "a01", IsVoter: true,
			Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
		},
	}
	c, _ := newTestClient(t, cluster, seed)
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-seed", PeerURLs: []string{peerURL("http", "test-seed", "test", "ns")}},
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.scaleUp(ctx, cluster, []lll.EtcdMember{*seed}); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}

	all := &lll.EtcdMemberList{}
	_ = c.List(ctx, all, client.InNamespace("ns"))
	var fresh *lll.EtcdMember
	for i := range all.Items {
		if all.Items[i].Name != seed.Name {
			fresh = &all.Items[i]
			break
		}
	}
	if fresh == nil {
		t.Fatalf("scaleUp must create a second member; got only %d", len(all.Items))
	}
	if fresh.Spec.Resources.Requests.Cpu().Cmp(wantCPU) != 0 {
		t.Fatalf("scaled-up member Spec.Resources.Requests.cpu = %v; want %v",
			fresh.Spec.Resources.Requests.Cpu(), wantCPU)
	}
}

// TestScaleUp_PropagatesOptionsToNewMember covers the second member-
// creation site for spec.options: scaleUp's fresh-CR path. Bootstrap is
// covered by TestBootstrap_PropagatesOptionsToSeed; this one makes sure
// a member added later runs with the same tuning as the seed.
func TestScaleUp_PropagatesOptionsToNewMember(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	quota := int64(10200547328)
	opts := &lll.EtcdOptions{
		QuotaBackendBytes:       &quota,
		AutoCompactionMode:      lll.AutoCompactionModePeriodic,
		AutoCompactionRetention: "5m",
	}

	// Cluster targets 2 replicas; 1 ready voter already exists. scaleUp
	// must create a fresh second member carrying Observed.Options.
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(2),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			Options:  opts,
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-test-x",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 2,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
				Options:  opts,
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}
	seed := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-seed", Namespace: "ns", Labels: memberLabels("test", "test-seed")},
		Spec:       lll.EtcdMemberSpec{ClusterName: "test", Version: "3.5.17", InitialCluster: "x", ClusterToken: "ns-test-x"},
		Status: lll.EtcdMemberStatus{
			PodName: "test-seed", MemberID: "a01", IsVoter: true,
			Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
		},
	}
	c, _ := newTestClient(t, cluster, seed)
	fe := newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-seed", PeerURLs: []string{peerURL("http", "test-seed", "test", "ns")}},
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	if _, err := r.scaleUp(ctx, cluster, []lll.EtcdMember{*seed}); err != nil {
		t.Fatalf("scaleUp: %v", err)
	}

	all := &lll.EtcdMemberList{}
	_ = c.List(ctx, all, client.InNamespace("ns"))
	var fresh *lll.EtcdMember
	for i := range all.Items {
		if all.Items[i].Name != seed.Name {
			fresh = &all.Items[i]
			break
		}
	}
	if fresh == nil {
		t.Fatalf("scaleUp must create a second member; got only %d", len(all.Items))
	}
	if !equality.Semantic.DeepEqual(fresh.Spec.Options, opts) {
		t.Fatalf("scaled-up member Spec.Options = %+v; want %+v", fresh.Spec.Options, opts)
	}
}

// TestUpdateStatus_SetsScaleSelector covers the /scale subresource
// contract: Status.Selector must carry the label-selector form for the
// VPA admission controller (and any other Scales().Get() consumer) to
// find the cluster's Pods. The minimal selector keyed on LabelCluster
// matches every Pod the operator emits.
func TestUpdateStatus_SetsScaleSelector(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "smk", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.5.17",
			Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "ns-smk-x", ClusterID: "0000000000000abc",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xabc))}

	if _, err := r.updateStatus(ctx, cluster, nil); err != nil {
		t.Fatalf("updateStatus: %v", err)
	}

	got := mustGet(t, c, "smk", "ns", &lll.EtcdCluster{})
	want := "etcd-operator.cozystack.io/cluster=smk"
	if got.Status.Selector != want {
		t.Fatalf("Status.Selector = %q; want %q", got.Status.Selector, want)
	}
}

// TestPDBMaxUnavailable pins the disruption budget formula. The
// PodDisruptionBudget protects voting members; this is the single
// place where the (n-1)/2 floor is computed.
func TestPDBMaxUnavailable(t *testing.T) {
	cases := []struct {
		voters int32
		want   int32
	}{
		{voters: 0, want: 0},
		{voters: 1, want: 0},
		{voters: 2, want: 0},
		{voters: 3, want: 1},
		{voters: 4, want: 1},
		{voters: 5, want: 2},
		{voters: 6, want: 2},
		{voters: 7, want: 3},
	}
	for _, tc := range cases {
		got := pdbMaxUnavailable(tc.voters)
		if got != tc.want {
			t.Fatalf("pdbMaxUnavailable(%d) = %d, want %d", tc.voters, got, tc.want)
		}
	}
}

// TestReconcilePDB_CreatesWithVoterSelector verifies that a cluster
// with voters gets a PDB whose selector restricts to LabelRole=
// RoleVoter (not just LabelCluster). Without this restriction, a
// learner Pod's eviction would consume budget and a voter would be
// over-protected during scale-up windows.
func TestReconcilePDB_CreatesWithVoterSelector(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.reconcilePDB(ctx, cluster, 3); err != nil {
		t.Fatalf("reconcilePDB: %v", err)
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test"}, pdb); err != nil {
		t.Fatalf("Get PDB: %v", err)
	}
	if pdb.Spec.MaxUnavailable == nil || pdb.Spec.MaxUnavailable.IntValue() != 1 {
		t.Fatalf("MaxUnavailable = %v, want 1", pdb.Spec.MaxUnavailable)
	}
	want := map[string]string{LabelCluster: "test", LabelRole: RoleVoter}
	if !reflect.DeepEqual(pdb.Spec.Selector.MatchLabels, want) {
		t.Fatalf("PDB selector MatchLabels = %v, want %v", pdb.Spec.Selector.MatchLabels, want)
	}
}

// TestAdditionalMetadata_AppliedToServiceAndPDB covers the cross-cutting
// metadata path on the two cluster-level objects that aren't members: the
// created Service and PDB must carry the extra labels/annotations, without
// the user shadowing operator-owned labels.
func TestAdditionalMetadata_AppliedToServiceAndPDB(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			AdditionalMetadata: &lll.AdditionalMetadata{
				Labels: map[string]string{
					"cozystack.io/tenant":          "foo",
					"app.kubernetes.io/managed-by": "evil",
				},
				Annotations: map[string]string{"example.com/note": "bar"},
			},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.reconcilePDB(ctx, cluster, 3); err != nil {
		t.Fatalf("reconcilePDB: %v", err)
	}
	pdb := &policyv1.PodDisruptionBudget{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test"}, pdb); err != nil {
		t.Fatalf("Get PDB: %v", err)
	}
	if pdb.Labels["cozystack.io/tenant"] != "foo" {
		t.Errorf("PDB label cozystack.io/tenant = %q, want foo", pdb.Labels["cozystack.io/tenant"])
	}
	if pdb.Labels["app.kubernetes.io/managed-by"] != "etcd-operator" {
		t.Errorf("PDB operator-owned label clobbered: app.kubernetes.io/managed-by = %q", pdb.Labels["app.kubernetes.io/managed-by"])
	}
	if pdb.Annotations["example.com/note"] != "bar" {
		t.Errorf("PDB annotation example.com/note = %q, want bar", pdb.Annotations["example.com/note"])
	}

	if err := r.ensureService(ctx, cluster, "test-svc", corev1.ServiceSpec{}); err != nil {
		t.Fatalf("ensureService: %v", err)
	}
	svc := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test-svc"}, svc); err != nil {
		t.Fatalf("Get Service: %v", err)
	}
	if svc.Labels["cozystack.io/tenant"] != "foo" {
		t.Errorf("Service label cozystack.io/tenant = %q, want foo", svc.Labels["cozystack.io/tenant"])
	}
	if svc.Annotations["example.com/note"] != "bar" {
		t.Errorf("Service annotation example.com/note = %q, want bar", svc.Annotations["example.com/note"])
	}
}

// TestReconcilePDB_UpdatesOnVoterCountChange verifies the in-place
// Patch path: existing PDB's MaxUnavailable is updated rather than the
// PDB being deleted+recreated. PDB Selector is immutable on the
// apiserver side; the test also implicitly guards against an
// accidental Selector change attempting that path.
func TestReconcilePDB_UpdatesOnVoterCountChange(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.reconcilePDB(ctx, cluster, 3); err != nil {
		t.Fatalf("reconcilePDB(3): %v", err)
	}
	if err := r.reconcilePDB(ctx, cluster, 5); err != nil {
		t.Fatalf("reconcilePDB(5): %v", err)
	}

	pdb := &policyv1.PodDisruptionBudget{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test"}, pdb); err != nil {
		t.Fatalf("Get PDB: %v", err)
	}
	if pdb.Spec.MaxUnavailable.IntValue() != 2 {
		t.Fatalf("MaxUnavailable after 3→5 transition = %d, want 2", pdb.Spec.MaxUnavailable.IntValue())
	}
}

// TestReconcilePDB_DeletesWhenNoVoters covers pre-bootstrap, paused,
// and wedged states: a PDB with zero matching Pods (because the
// label-bearing voters don't exist) would be inert anyway, but its
// staleness — particularly with a non-zero MaxUnavailable from a
// prior state — would mislead operators reading kubectl get pdb.
func TestReconcilePDB_DeletesWhenNoVoters(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.reconcilePDB(ctx, cluster, 3); err != nil {
		t.Fatalf("reconcilePDB(3): %v", err)
	}
	if err := r.reconcilePDB(ctx, cluster, 0); err != nil {
		t.Fatalf("reconcilePDB(0): %v", err)
	}

	err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "test"}, &policyv1.PodDisruptionBudget{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("PDB must be deleted when voterCount=0; got err=%v", err)
	}

	// Idempotent: reconcilePDB(0) on a not-found PDB is a no-op.
	if err := r.reconcilePDB(ctx, cluster, 0); err != nil {
		t.Fatalf("reconcilePDB(0) on absent PDB must be a no-op; got %v", err)
	}
}

// TestSyncIsVoter_PatchesFromMemberList verifies the bridge between
// etcd's MemberList view and CR-side Status.IsVoter. Matching is by
// peer URL — etcd Member.Name is empty during the post-MemberAdd
// pre-Pod-running window and would mis-match. Idempotent: members
// already in the desired state must not get re-patched.
func TestSyncIsVoter_PatchesFromMemberList(t *testing.T) {
	ctx := context.Background()

	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"}}

	mkMember := func(name string, currentIsVoter bool) *lll.EtcdMember {
		return &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: memberLabels("test", name)},
			Spec:       lll.EtcdMemberSpec{ClusterName: "test"},
			Status:     lll.EtcdMemberStatus{IsVoter: currentIsVoter},
		}
	}
	voter := mkMember("test-v", false)   // etcd says voter; CR says false → expect patch to true
	learner := mkMember("test-l", true)  // etcd says learner; CR says true → expect patch to false
	stable := mkMember("test-s", true)   // etcd says voter; CR says true → no patch
	unknown := mkMember("test-u", false) // not in etcd's list yet → no patch

	c, _ := newTestClient(t, cluster, voter, learner, stable, unknown)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	listResp := &clientv3.MemberListResponse{
		Members: []*etcdserverpb.Member{
			{ID: 0x1, PeerURLs: []string{peerURL("http", "test-v", "test", "ns")}, IsLearner: false},
			{ID: 0x2, PeerURLs: []string{peerURL("http", "test-l", "test", "ns")}, IsLearner: true},
			{ID: 0x3, PeerURLs: []string{peerURL("http", "test-s", "test", "ns")}, IsLearner: false},
		},
	}

	members := []lll.EtcdMember{*voter, *learner, *stable, *unknown}
	if err := r.syncIsVoter(ctx, cluster, members, listResp); err != nil {
		t.Fatalf("syncIsVoter: %v", err)
	}

	check := func(name string, want bool) {
		t.Helper()
		got := &lll.EtcdMember{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: name}, got); err != nil {
			t.Fatalf("Get %s: %v", name, err)
		}
		if got.Status.IsVoter != want {
			t.Fatalf("%s.Status.IsVoter = %v, want %v", name, got.Status.IsVoter, want)
		}
	}
	check("test-v", true)  // patched up
	check("test-l", false) // patched down
	check("test-s", true)  // unchanged (already true)
	check("test-u", false) // unchanged (not in etcd list yet)
}

// TestBootstrap_PreStampsSeedIsVoter verifies the seed's IsVoter is
// set to true at create time (rather than waiting for the first
// MemberList round). This closes the bootstrap-window protection
// gap: the member controller can apply role=voter on the first Pod
// reconcile and the PDB sees the labelled Pod on the next cluster
// reconcile.
func TestBootstrap_PreStampsSeedIsVoter(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}

	reconcileUntilStable(t, r, c, "test", "ns", 8)

	members := &lll.EtcdMemberList{}
	if err := c.List(ctx, members, client.InNamespace("ns")); err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("expected one seed; got %d", len(members.Items))
	}
	if !members.Items[0].Status.IsVoter {
		t.Fatalf("seed Status.IsVoter must be true after bootstrap; got %v", members.Items[0].Status.IsVoter)
	}
}

// TestReconcileTLSCertificates_EmitsExpectedCertificates exercises the
// operator-driven cert-manager path. For a cluster with both client (mTLS)
// and peer Certificates configured we expect three Certificate resources
// in the cluster's namespace, each owned by the EtcdCluster, with:
//
//   - the conventional secretName ("<cluster>-server-tls",
//     "<cluster>-operator-client-tls", "<cluster>-peer-tls") that the
//     BYO mount path consumes,
//   - the right issuerRef (name, kind, group=cert-manager.io),
//   - SANs covering both wildcard short and FQDN forms for server and
//     peer (peer SAN's FQDN form is load-bearing — etcd's peer-mTLS
//     verifier reverse-DNS-looks-up the connecting IP and matches the
//     resulting PTR against the cert's DNS list),
//   - the EKU set that satisfies etcd's expectations (server+client on
//     both server and peer certs; clientAuth only on operator-client).
//
// The fake client used here supports unstructured Get/Create, which the
// reconciler uses to stay free of the cert-manager Go module dependency.
func TestReconcileTLSCertificates_EmitsExpectedCertificates(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
					ServerIssuerRef:         lll.IssuerReference{Name: "my-ca", Kind: "Issuer"},
					OperatorClientIssuerRef: &lll.IssuerReference{Name: "my-ca", Kind: "Issuer"},
				}},
				Peer: &lll.PeerTLS{CertManager: &lll.PeerCertManagerTLS{
					IssuerRef: lll.IssuerReference{Name: "my-peer-ca", Kind: "Issuer"},
				}},
			},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdead)), CertManagerAvailable: true}

	if err := r.reconcileTLSCertificates(ctx, cluster); err != nil {
		t.Fatalf("reconcileTLSCertificates: %v", err)
	}

	// Three Certificates, each owned by the cluster, with the right
	// secretName / issuerRef / SAN-presence invariants.
	type want struct {
		name        string
		secretName  string
		issuer      string
		dnsContains []string // substrings expected in the SAN list
		hasIP127001 bool
		usages      []string
	}
	wants := []want{
		{
			name:       "etcd-server",
			secretName: "etcd-server-tls",
			issuer:     "my-ca",
			dnsContains: []string{
				"*.etcd.ns.svc",
				"*.etcd.ns.svc.cluster.local",
				"localhost",
			},
			hasIP127001: true,
			usages:      []string{"server auth", "client auth"},
		},
		{
			name:       "etcd-operator-client",
			secretName: "etcd-operator-client-tls",
			issuer:     "my-ca",
			usages:     []string{"client auth"},
		},
		{
			name:       "etcd-peer",
			secretName: "etcd-peer-tls",
			issuer:     "my-peer-ca",
			dnsContains: []string{
				"*.etcd.ns.svc",
				"*.etcd.ns.svc.cluster.local",
			},
			usages: []string{"server auth", "client auth"},
		},
	}

	for _, w := range wants {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
		})
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: w.name}, got); err != nil {
			t.Fatalf("Get Certificate/%s: %v", w.name, err)
		}
		// Owner-ref points at the EtcdCluster.
		ownerOK := false
		for _, o := range got.GetOwnerReferences() {
			if o.Kind == "EtcdCluster" && o.Name == cluster.Name && o.UID == cluster.UID {
				ownerOK = true
				break
			}
		}
		if !ownerOK {
			t.Fatalf("Certificate/%s missing EtcdCluster owner ref; got %v", w.name, got.GetOwnerReferences())
		}
		sn, _, _ := unstructured.NestedString(got.Object, "spec", "secretName")
		if sn != w.secretName {
			t.Fatalf("Certificate/%s spec.secretName = %q; want %q", w.name, sn, w.secretName)
		}
		issuerName, _, _ := unstructured.NestedString(got.Object, "spec", "issuerRef", "name")
		issuerGroup, _, _ := unstructured.NestedString(got.Object, "spec", "issuerRef", "group")
		if issuerName != w.issuer {
			t.Fatalf("Certificate/%s spec.issuerRef.name = %q; want %q", w.name, issuerName, w.issuer)
		}
		if issuerGroup != "cert-manager.io" {
			t.Fatalf("Certificate/%s spec.issuerRef.group = %q; want cert-manager.io", w.name, issuerGroup)
		}
		if len(w.dnsContains) > 0 {
			dnsAny, _, _ := unstructured.NestedSlice(got.Object, "spec", "dnsNames")
			dns := make([]string, 0, len(dnsAny))
			for _, x := range dnsAny {
				if s, ok := x.(string); ok {
					dns = append(dns, s)
				}
			}
			for _, wantSub := range w.dnsContains {
				found := false
				for _, d := range dns {
					if d == wantSub {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("Certificate/%s missing DNS SAN %q; got %v", w.name, wantSub, dns)
				}
			}
		}
		if w.hasIP127001 {
			ipsAny, _, _ := unstructured.NestedSlice(got.Object, "spec", "ipAddresses")
			ips := make([]string, 0, len(ipsAny))
			for _, x := range ipsAny {
				if s, ok := x.(string); ok {
					ips = append(ips, s)
				}
			}
			found := false
			for _, ip := range ips {
				if ip == "127.0.0.1" {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Certificate/%s missing 127.0.0.1 IP SAN; got %v", w.name, ips)
			}
		}
		usagesAny, _, _ := unstructured.NestedSlice(got.Object, "spec", "usages")
		usages := make([]string, 0, len(usagesAny))
		for _, x := range usagesAny {
			if s, ok := x.(string); ok {
				usages = append(usages, s)
			}
		}
		for _, want := range w.usages {
			found := false
			for _, u := range usages {
				if u == want {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("Certificate/%s missing usage %q; got %v", w.name, want, usages)
			}
		}
	}
}

// TestReconcile_CertManagerMissingIsTerminal exercises the full
// Reconcile against a cluster with spec.tls.client.certManager set and
// CertManagerAvailable=false. The contract has three pieces, ALL of
// which need the gate to live at the top of Reconcile rather than
// inside the cert-emission helper:
//
//  1. The Available condition reads CertManagerNotInstalled (not
//     ClusterUnreachable or WaitingForSeed, which is what later stages
//     of Reconcile would write if they got to run).
//  2. No EtcdMember CR is bootstrapped — the member controller would
//     park it indefinitely waiting for a Secret that's never coming.
//  3. No cert-manager.io/v1 GVK is touched (no Create, no Get) —
//     controller-runtime's cached client would otherwise set up an
//     informer for the unknown kind on first Get and the reflector
//     would retry-LIST forever.
//
// Pinning the contract via the helper alone (the prior version of this
// test) misses pieces 1 and 2: Reconcile would clobber the condition
// and create the EtcdMember anyway. This integration-shaped variant
// catches that.
func TestReconcile_CertManagerMissingIsTerminal(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			TLS: &lll.EtcdClusterTLS{Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
				ServerIssuerRef: lll.IssuerReference{Name: "my-ca"},
			}}},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{
		Client:               c,
		Scheme:               testScheme(t),
		EtcdClientFactory:    factoryReturning(newFakeEtcd(0xdead)),
		CertManagerAvailable: false,
	}

	// Run Reconcile a couple of times — once to install the condition,
	// then again to make sure no later reconcile path overrides it.
	for i := 0; i < 2; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: "etcd", Namespace: "ns"}}); err != nil {
			t.Fatalf("Reconcile #%d: unexpected error: %v", i, err)
		}
	}

	got := mustGet(t, c, "etcd", "ns", &lll.EtcdCluster{})
	var avail *metav1.Condition
	for i := range got.Status.Conditions {
		if got.Status.Conditions[i].Type == lll.ClusterAvailable {
			avail = &got.Status.Conditions[i]
			break
		}
	}
	if avail == nil {
		t.Fatalf("expected Available condition; got %v", got.Status.Conditions)
	}
	if avail.Status != metav1.ConditionFalse {
		t.Fatalf("Available.Status = %q; want False", avail.Status)
	}
	if avail.Reason != "CertManagerNotInstalled" {
		t.Fatalf("Available.Reason = %q; want CertManagerNotInstalled (any other reason means a later reconcile path clobbered it)", avail.Reason)
	}

	// No EtcdMember was bootstrapped.
	members := &lll.EtcdMemberList{}
	_ = c.List(ctx, members, client.InNamespace("ns"))
	if len(members.Items) != 0 {
		t.Fatalf("expected zero EtcdMember CRs for a terminally-misconfigured cluster; got %d", len(members.Items))
	}

	// No Certificate was created.
	certs := &unstructured.UnstructuredList{}
	certs.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "CertificateList",
	})
	_ = c.List(ctx, certs, client.InNamespace("ns"))
	if len(certs.Items) != 0 {
		t.Fatalf("no cert-manager Certificate should be created when CertManagerAvailable=false; got %d", len(certs.Items))
	}
}

// TestReconcileTLSCertificates_ServerTLSOnlyOmitsOperatorClient pins the
// mTLS toggle in cert-manager mode: an unset OperatorClientIssuerRef
// must NOT emit an operator-client Certificate. Without this guard, the
// operator would silently materialise an operator-client Secret the
// member controller's mTLS branch would then try to consume.
func TestReconcileTLSCertificates_ServerTLSOnlyOmitsOperatorClient(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			TLS: &lll.EtcdClusterTLS{Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
				ServerIssuerRef: lll.IssuerReference{Name: "my-ca"},
				// OperatorClientIssuerRef left nil — server-TLS only.
			}}},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), CertManagerAvailable: true}

	if err := r.reconcileTLSCertificates(ctx, cluster); err != nil {
		t.Fatalf("reconcileTLSCertificates: %v", err)
	}

	// 1. Operator-client Certificate must NOT exist.
	opCert := &unstructured.Unstructured{}
	opCert.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
	})
	err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "etcd-operator-client"}, opCert)
	if err == nil {
		t.Fatalf("server-TLS-only mode must NOT emit operator-client Certificate; got %+v", opCert.Object)
	}

	// 2. Server Certificate MUST still exist, fully wired. Regressing
	//    this (an empty emit, or one missing issuerRef/SANs) would slip
	//    past the original narrower test.
	serverCert := &unstructured.Unstructured{}
	serverCert.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
	})
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "etcd-server"}, serverCert); err != nil {
		t.Fatalf("Get Certificate/etcd-server: %v", err)
	}
	sn, _, _ := unstructured.NestedString(serverCert.Object, "spec", "secretName")
	if sn != "etcd-server-tls" {
		t.Fatalf("server Certificate spec.secretName = %q; want etcd-server-tls", sn)
	}
	issuer, _, _ := unstructured.NestedString(serverCert.Object, "spec", "issuerRef", "name")
	if issuer != "my-ca" {
		t.Fatalf("server Certificate spec.issuerRef.name = %q; want my-ca", issuer)
	}
	group, _, _ := unstructured.NestedString(serverCert.Object, "spec", "issuerRef", "group")
	if group != "cert-manager.io" {
		t.Fatalf("server Certificate spec.issuerRef.group = %q; want cert-manager.io", group)
	}
	dnsAny, _, _ := unstructured.NestedSlice(serverCert.Object, "spec", "dnsNames")
	if len(dnsAny) == 0 {
		t.Fatalf("server Certificate has empty dnsNames; want wildcard + FQDN + service + localhost")
	}
	ipsAny, _, _ := unstructured.NestedSlice(serverCert.Object, "spec", "ipAddresses")
	foundLoopback := false
	for _, ip := range ipsAny {
		if s, ok := ip.(string); ok && s == "127.0.0.1" {
			foundLoopback = true
			break
		}
	}
	if !foundLoopback {
		t.Fatalf("server Certificate missing 127.0.0.1 IP SAN; got %v", ipsAny)
	}
}

// TestReconcileTLSCertificates_ClusterIssuerKind exercises the
// ClusterIssuer code path, which is the standard production setup
// (one cluster-wide CA serving many namespaces). Without this the
// regression "operator silently writes kind: Issuer regardless of
// what the user set" would slip through the prior test which only
// covered the namespaced-Issuer kind.
func TestReconcileTLSCertificates_ClusterIssuerKind(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "ns", UID: types.UID("cluster-uid")},
		Spec: lll.EtcdClusterSpec{
			TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
					ServerIssuerRef:         lll.IssuerReference{Name: "shared-ca", Kind: "ClusterIssuer"},
					OperatorClientIssuerRef: &lll.IssuerReference{Name: "shared-ca", Kind: "ClusterIssuer"},
				}},
				Peer: &lll.PeerTLS{CertManager: &lll.PeerCertManagerTLS{
					IssuerRef: lll.IssuerReference{Name: "shared-peer-ca", Kind: "ClusterIssuer"},
				}},
			},
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), CertManagerAvailable: true}

	if err := r.reconcileTLSCertificates(ctx, cluster); err != nil {
		t.Fatalf("reconcileTLSCertificates: %v", err)
	}

	for _, n := range []string{"etcd-server", "etcd-operator-client", "etcd-peer"} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
		})
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: n}, got); err != nil {
			t.Fatalf("Certificate/%s not created: %v", n, err)
		}
		kind, _, _ := unstructured.NestedString(got.Object, "spec", "issuerRef", "kind")
		if kind != "ClusterIssuer" {
			t.Fatalf("Certificate/%s spec.issuerRef.kind = %q; want ClusterIssuer", n, kind)
		}
	}
}

// TestEnsureCertificate_DoesNotPatchExistingCertificate covers
// reviewer blocker 3: an existing Certificate may carry fields the
// operator doesn't own (cert-manager-defaulted revisionHistoryLimit,
// privateKey.rotationPolicy, etc.). The reconciler must NOT issue a
// Patch on every reconcile that would null those out. Pre-populate a
// Certificate with extra spec keys and assert ensureCertificate
// leaves it untouched.
func TestEnsureCertificate_DoesNotPatchExistingCertificate(t *testing.T) {
	ctx := context.Background()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "ns", UID: types.UID("cluster-uid")},
	}

	preExisting := &unstructured.Unstructured{}
	preExisting.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "cert-manager.io", Version: "v1", Kind: "Certificate",
	})
	preExisting.SetName("etcd-server")
	preExisting.SetNamespace("ns")
	preExisting.Object["spec"] = map[string]any{
		"secretName": "etcd-server-tls",
		"commonName": "etcd-server",
		"issuerRef": map[string]any{
			"name":  "my-ca",
			"kind":  "Issuer",
			"group": "cert-manager.io",
		},
		// Fields cert-manager would default — operator doesn't own them.
		"revisionHistoryLimit": int64(1),
		"privateKey": map[string]any{
			"algorithm":      "RSA",
			"size":           int64(2048),
			"rotationPolicy": "Never",
			"encoding":       "PKCS1",
		},
	}

	c, _ := newTestClient(t, cluster, preExisting)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), CertManagerAvailable: true}

	if err := r.ensureCertificate(ctx, cluster, certificateSpec{
		name:       "etcd-server",
		secretName: "etcd-server-tls",
		commonName: "etcd-server",
		issuerRef:  lll.IssuerReference{Name: "my-ca", Kind: "Issuer"},
		dnsNames:   []string{"localhost"},
		usages:     []string{"server auth", "client auth"},
	}); err != nil {
		t.Fatalf("ensureCertificate: %v", err)
	}

	// Re-read the Certificate. cert-manager-defaulted fields must still
	// be present at their original values.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(preExisting.GroupVersionKind())
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "etcd-server"}, got); err != nil {
		t.Fatalf("re-Get: %v", err)
	}
	rhl, found, _ := unstructured.NestedInt64(got.Object, "spec", "revisionHistoryLimit")
	if !found || rhl != 1 {
		t.Fatalf("spec.revisionHistoryLimit lost or mutated; got %v (found=%v)", rhl, found)
	}
	rp, found, _ := unstructured.NestedString(got.Object, "spec", "privateKey", "rotationPolicy")
	if !found || rp != "Never" {
		t.Fatalf("spec.privateKey.rotationPolicy lost or mutated; got %v (found=%v)", rp, found)
	}
	enc, found, _ := unstructured.NestedString(got.Object, "spec", "privateKey", "encoding")
	if !found || enc != "PKCS1" {
		t.Fatalf("spec.privateKey.encoding lost or mutated; got %v (found=%v)", enc, found)
	}
}
