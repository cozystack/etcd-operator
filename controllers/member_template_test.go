package controllers

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// steadyCluster returns a converged three-member cluster whose latched
// target is observed, with members still carrying an older template.
func steadyCluster(t *testing.T, observed *lll.ObservedClusterSpec) []client.Object {
	t.Helper()
	now := metav1.Now()
	spec := lll.EtcdClusterSpec{
		Replicas:  ptrInt32(observed.Replicas),
		Version:   observed.Version,
		Storage:   observed.Storage,
		Resources: observed.Resources,
		Options:   observed.Options,
		ExtraArgs: observed.ExtraArgs,

		ImageRepository: observed.ImageRepository,
		ImageTagSuffix:  observed.ImageTagSuffix,
	}
	objs := []client.Object{&lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec:       spec,
		Status: lll.EtcdClusterStatus{
			ClusterToken:     "ns-test-x",
			ClusterID:        "deadbeef",
			Observed:         observed,
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(time.Hour)},
		},
	}}
	for i := range 3 {
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("test-%d", i), Namespace: "ns", Labels: memberLabels("test", fmt.Sprintf("test-%d", i))},
			Spec: lll.EtcdMemberSpec{
				ClusterName:    "test",
				Version:        "3.5.17",
				Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi")},
				InitialCluster: "x",
				Options:        &lll.EtcdOptions{SnapshotCount: ptrInt64(10000)},
				ExtraArgs:      map[string]string{"log-level": "info"},
			},
			Status: lll.EtcdMemberStatus{
				PodName:    fmt.Sprintf("test-%d", i),
				MemberID:   "abc",
				Conditions: []metav1.Condition{{Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady", LastTransitionTime: now}},
			},
		})
	}
	return objs
}

func steadyEtcd() *fakeEtcd {
	return newFakeEtcd(0xdeadbeef,
		&etcdserverpb.Member{ID: 0xa01, Name: "test-0", PeerURLs: []string{peerURL("http", "test-0", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa02, Name: "test-1", PeerURLs: []string{peerURL("http", "test-1", "test", "ns")}},
		&etcdserverpb.Member{ID: 0xa03, Name: "test-2", PeerURLs: []string{peerURL("http", "test-2", "test", "ns")}},
	)
}

func reconcileTest(t *testing.T, c client.Client) {
	t.Helper()
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(steadyEtcd())}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// TestReconcile_SyncsMemberTemplatesAtSteadyState: once a spec change is
// latched, existing members carry the new Pod template, so a Pod deleted to
// roll the change comes back with it.
func TestReconcile_SyncsMemberTemplatesAtSteadyState(t *testing.T) {
	observed := &lll.ObservedClusterSpec{
		Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: quickQty(t, "2Gi")}},
		Options:   &lll.EtcdOptions{SnapshotCount: ptrInt64(5000)},
		ExtraArgs: map[string]string{"log-level": "debug", "threek-mode": "dry-run"},
	}
	c, _ := newTestClient(t, steadyCluster(t, observed)...)
	reconcileTest(t, c)

	for i := range 3 {
		m := mustGet(t, c, fmt.Sprintf("test-%d", i), "ns", &lll.EtcdMember{})
		if !equality.Semantic.DeepEqual(m.Spec.Options, observed.Options) {
			t.Errorf("%s options = %+v, want %+v", m.Name, m.Spec.Options, observed.Options)
		}
		if !equality.Semantic.DeepEqual(m.Spec.ExtraArgs, observed.ExtraArgs) {
			t.Errorf("%s extraArgs = %v, want %v", m.Name, m.Spec.ExtraArgs, observed.ExtraArgs)
		}
		if !equality.Semantic.DeepEqual(m.Spec.Resources, observed.Resources) {
			t.Errorf("%s resources = %+v, want %+v", m.Name, m.Spec.Resources, observed.Resources)
		}

		pod := (&EtcdMemberReconciler{}).buildPod(m, true)
		cmd := pod.Spec.Containers[0].Command
		for _, flag := range []string{"--snapshot-count=5000", "--log-level=debug", "--threek-mode=dry-run"} {
			if !slices.Contains(cmd, flag) {
				t.Errorf("%s: a recreated Pod lacks %s: %v", m.Name, flag, cmd)
			}
		}
	}
}

// TestReconcile_SyncLeavesOtherMemberFieldsAlone: only the Pod-template
// fields follow the cluster. Storage is bound to the member's volume and the
// version is an upgrade, so neither changes under an existing member.
func TestReconcile_SyncLeavesOtherMemberFieldsAlone(t *testing.T) {
	observed := &lll.ObservedClusterSpec{
		Replicas: 3, Version: "3.6.1", Storage: lll.StorageSpec{Size: quickQty(t, "8Gi")},
		ExtraArgs: map[string]string{"threek-mode": "dry-run"},
	}
	c, _ := newTestClient(t, steadyCluster(t, observed)...)
	reconcileTest(t, c)

	m := mustGet(t, c, "test-0", "ns", &lll.EtcdMember{})
	if m.Spec.Version != "3.5.17" {
		t.Errorf("version = %s, want the member's own 3.5.17", m.Spec.Version)
	}
	if got := m.Spec.Storage.Size.String(); got != "1Gi" {
		t.Errorf("storage size = %s, want the member's own 1Gi", got)
	}
}

// TestReconcile_NoMemberWritesWhenTemplatesMatch: a converged cluster whose
// members already carry the latched template is not rewritten every pass.
func TestReconcile_NoMemberWritesWhenTemplatesMatch(t *testing.T) {
	observed := &lll.ObservedClusterSpec{
		Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		Options:   &lll.EtcdOptions{SnapshotCount: ptrInt64(10000)},
		ExtraArgs: map[string]string{"log-level": "info"},
	}
	writes := 0
	countMemberWrites := func(obj client.Object) {
		if _, ok := obj.(*lll.EtcdMember); ok {
			writes++
		}
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(steadyCluster(t, observed)...).
		WithStatusSubresource(&lll.EtcdCluster{}, &lll.EtcdMember{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				countMemberWrites(obj)
				return c.Patch(ctx, obj, patch, opts...)
			},
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				countMemberWrites(obj)
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()
	reconcileTest(t, c)
	if writes != 0 {
		t.Errorf("members written %d times without a template change", writes)
	}
}

// TestReconcile_SyncsDormantMemberTemplate: a paused cluster's dormant member
// is woken by scale-up, which creates its Pod before the cluster converges
// again, so the dormant member must already carry the latched template.
func TestReconcile_SyncsDormantMemberTemplate(t *testing.T) {
	observed := &lll.ObservedClusterSpec{
		Replicas: 0, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		ExtraArgs: map[string]string{"threek-mode": "dry-run"},
	}
	objs := steadyCluster(t, observed)[:2]
	dormant := objs[1].(*lll.EtcdMember)
	dormant.Spec.Dormant = true
	dormant.Status = lll.EtcdMemberStatus{MemberID: "abc"}
	c, _ := newTestClient(t, objs...)
	reconcileTest(t, c)

	m := mustGet(t, c, dormant.Name, "ns", &lll.EtcdMember{})
	if !equality.Semantic.DeepEqual(m.Spec.ExtraArgs, observed.ExtraArgs) {
		t.Errorf("dormant member extraArgs = %v, want %v", m.Spec.ExtraArgs, observed.ExtraArgs)
	}
}
