//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha2 "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// The cluster name is shared; each test gets its OWN namespace so one test's
// namespace teardown (which is asynchronous — the namespace lingers in
// Terminating) can't block the next test from creating content in it.
const defragCluster = "etcd"

// TestEtcdDefragReclaimsSpace proves the EtcdDefrag controller end to end on a
// real cluster: a member accrues reclaimable free space (write a few MB, delete
// it, compact — which frees pages logically but leaves the file allocated), an
// EtcdDefrag is created, and the controller defragments it so the physical
// DbSize shrinks and the run reaches phase Complete.
func TestEtcdDefragReclaimsSpace(t *testing.T) {
	ctx := context.Background()
	ns := "defrag-reclaim-e2e"
	createDefragNamespace(ctx, t, ns)

	if err := kube.Create(ctx, defragClusterObject(ns)); err != nil {
		t.Fatalf("create EtcdCluster: %v", err)
	}
	waitFor(ctx, t, 5*time.Minute, "cluster Available", etcdClusterAvailable(ns, defragCluster))
	waitFor(ctx, t, 2*time.Minute, "3 members ready", readyMembersIsIn(ns, defragCluster, 3))

	pod := aReadyMemberPod(ctx, t, ns)
	fragmentEtcd(ctx, t, ns, pod)
	frag := endpointDBSize(ctx, t, ns, pod)
	t.Logf("db after fragmenting: size=%d inUse=%d free=%d", frag.dbSize, frag.dbSizeInUse, frag.dbSize-frag.dbSizeInUse)
	if frag.dbSize-frag.dbSizeInUse < 1<<20 {
		t.Fatalf("expected >1Mi reclaimable free space after fragmenting, got %d", frag.dbSize-frag.dbSizeInUse)
	}

	// Ask for an unconditional defrag now.
	createEtcdDefrag(ctx, t, ns, "defrag-now", &etcdv1alpha2.DefragRule{All: true})

	waitFor(ctx, t, 3*time.Minute, "EtcdDefrag Complete", etcdDefragPhaseIs(ns, "defrag-now", etcdv1alpha2.EtcdDefragPhaseComplete))
	waitFor(ctx, t, 2*time.Minute, "physical DbSize reclaimed", func(ctx context.Context) error {
		now := endpointDBSize(ctx, t, ns, pod)
		if now.dbSize >= frag.dbSize {
			return fmt.Errorf("dbSize not reclaimed: was %d, still %d", frag.dbSize, now.dbSize)
		}
		return nil
	})
}

// The "defer-not-force while the cluster is unhealthy" invariant is covered
// deterministically by the controller unit test
// TestEtcdDefrag_DeferredWhenUnhealthy — it asserts no Defragment call plus the
// DefragDeferred event and DefragChecked=False/ClusterNotHealthy. It has no
// reliable e2e counterpart: deleting member Pods to break quorum races the
// operator, which recreates the PVC-backed Pods and heals faster than a test
// window can observe, so an e2e negative-window assertion is inherently flaky.

// ── helpers ─────────────────────────────────────────────────────────────────

func createDefragNamespace(ctx context.Context, t *testing.T, ns string) {
	t.Helper()
	nsObj := &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: ns},
	}
	if err := kube.Patch(ctx, nsObj, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		t.Fatalf("create namespace %s: %v", ns, err)
	}
	t.Cleanup(func() {
		_ = kube.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})
}

func defragClusterObject(ns string) *etcdv1alpha2.EtcdCluster {
	three := int32(3)
	return &etcdv1alpha2.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: defragCluster, Namespace: ns},
		Spec: etcdv1alpha2.EtcdClusterSpec{
			Replicas: &three,
			Version:  "3.6.11",
			Storage:  etcdv1alpha2.StorageSpec{Size: resource.MustParse("1Gi")},
		},
	}
}

func createEtcdDefrag(ctx context.Context, t *testing.T, ns, name string, rule *etcdv1alpha2.DefragRule) {
	t.Helper()
	d := &etcdv1alpha2.EtcdDefrag{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: etcdv1alpha2.EtcdDefragSpec{
			ClusterRef: corev1.LocalObjectReference{Name: defragCluster},
			Rule:       rule,
		},
	}
	if err := kube.Create(ctx, d); err != nil {
		t.Fatalf("create EtcdDefrag %s: %v", name, err)
	}
}

func etcdDefragPhaseIs(ns, name string, phase etcdv1alpha2.EtcdDefragPhase) func(context.Context) error {
	return func(ctx context.Context) error {
		d := &etcdv1alpha2.EtcdDefrag{}
		if err := kube.Get(ctx, client.ObjectKey{Namespace: ns, Name: name}, d); err != nil {
			return err
		}
		if d.Status.Phase != phase {
			return fmt.Errorf("EtcdDefrag %s phase=%q, want %q", name, d.Status.Phase, phase)
		}
		return nil
	}
}

func defragMemberNames(ctx context.Context, t *testing.T, ns string) []string {
	t.Helper()
	list := &etcdv1alpha2.EtcdMemberList{}
	if err := kube.List(ctx, list, client.InNamespace(ns),
		client.MatchingLabels{"etcd-operator.cozystack.io/cluster": defragCluster}); err != nil {
		t.Fatalf("list members: %v", err)
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].Name)
	}
	return names
}

func aReadyMemberPod(ctx context.Context, t *testing.T, ns string) string {
	t.Helper()
	for _, name := range defragMemberNames(ctx, t, ns) {
		p, err := clientset.CoreV1().Pods(ns).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			continue
		}
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Name == "etcd" && cs.Ready {
				return name
			}
		}
	}
	t.Fatal("no ready member pod found")
	return ""
}

type dbStat struct {
	dbSize      int64
	dbSizeInUse int64
	revision    int64
}

// endpointDBSize reads the member's own backend sizes via `etcdctl endpoint
// status -w json`.
func endpointDBSize(ctx context.Context, t *testing.T, ns, pod string) dbStat {
	t.Helper()
	out, stderr, err := podExec(ctx, ns, pod, "etcd",
		[]string{"etcdctl", "endpoint", "status", "-w", "json"})
	if err != nil {
		t.Fatalf("endpoint status on %s: %v (stderr: %s)", pod, err, stderr)
	}
	var rows []struct {
		Status struct {
			DbSize      int64 `json:"dbSize"`
			DbSizeInUse int64 `json:"dbSizeInUse"`
			Header      struct {
				Revision int64 `json:"revision"`
			} `json:"header"`
		} `json:"Status"`
	}
	if err := json.Unmarshal([]byte(out), &rows); err != nil || len(rows) == 0 {
		t.Fatalf("parse endpoint status %q: %v", out, err)
	}
	return dbStat{rows[0].Status.DbSize, rows[0].Status.DbSizeInUse, rows[0].Status.Header.Revision}
}

// fragmentEtcd creates reclaimable free space: write a few MB across many keys,
// delete them all, then compact (which frees the pages logically but leaves the
// file allocated — exactly what defrag reclaims). etcdctl runs one arg-only
// command per call (the etcd image is distroless, no shell), so the write loop
// is driven from Go.
func fragmentEtcd(ctx context.Context, t *testing.T, ns, pod string) {
	t.Helper()
	value := strings.Repeat("x", 8<<10) // 8Ki per key
	const keys = 512                    // ~4Mi of data
	for i := 0; i < keys; i++ {
		if _, stderr, err := podExec(ctx, ns, pod, "etcd",
			[]string{"etcdctl", "put", fmt.Sprintf("frag/%04d", i), value}); err != nil {
			t.Fatalf("etcdctl put %d: %v (stderr: %s)", i, err, stderr)
		}
	}
	if _, stderr, err := podExec(ctx, ns, pod, "etcd",
		[]string{"etcdctl", "del", "frag/", "--prefix"}); err != nil {
		t.Fatalf("etcdctl del: %v (stderr: %s)", err, stderr)
	}
	rev := endpointDBSize(ctx, t, ns, pod).revision
	if _, stderr, err := podExec(ctx, ns, pod, "etcd",
		[]string{"etcdctl", "compact", fmt.Sprintf("%d", rev), "--physical"}); err != nil {
		t.Fatalf("etcdctl compact: %v (stderr: %s)", err, stderr)
	}
}
