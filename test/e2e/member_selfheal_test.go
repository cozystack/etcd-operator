//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha2 "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	selfHealNamespace = "selfheal-e2e"
	selfHealCluster   = "etcd"
)

// TestPVCMemberCrashLoopSelfHeal proves the new self-heal path end to end on a
// real cluster: a PVC-backed member whose data dir is corrupted crash-loops,
// and once it passes the restart threshold (with the other two members holding
// quorum) the operator deletes it, GCs its PVC, and gap-fills a fresh member —
// the cluster returns to 3 ready members and still serves reads/writes.
//
// This exercises etcdContainerStuck + the quorum gate + finalizer MemberRemove
// + PVC owner-ref GC + cluster-controller gap-fill, none of which the unit
// tests (fake client) or the member-churn block in TestKamajiDataStore (normal
// MemberRemove path, never a crash-loop) reach.
//
// It is deliberately slow: CrashLoopBackOff caps backoff at 5m, so reaching the
// 5-restart threshold takes on the order of ten minutes. The waits below are
// sized for that.
func TestPVCMemberCrashLoopSelfHeal(t *testing.T) {
	ctx := context.Background()

	ns := &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: selfHealNamespace},
	}
	if err := kube.Patch(ctx, ns, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		t.Fatalf("create namespace %s: %v", selfHealNamespace, err)
	}
	t.Cleanup(func() {
		_ = kube.Delete(context.Background(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: selfHealNamespace}})
	})

	// A minimal plaintext 3-member PVC cluster — TLS is orthogonal to the
	// self-heal path under test, and plaintext lets etcdctl probe the cluster
	// without certs.
	three := int32(3)
	ec := &etcdv1alpha2.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: selfHealCluster, Namespace: selfHealNamespace},
		Spec: etcdv1alpha2.EtcdClusterSpec{
			Replicas: &three,
			Version:  "3.6.11",
			Storage:  etcdv1alpha2.StorageSpec{Size: resource.MustParse("1Gi")},
		},
	}
	if err := kube.Create(ctx, ec); err != nil {
		t.Fatalf("create EtcdCluster: %v", err)
	}

	waitFor(ctx, t, 5*time.Minute, "cluster Available", etcdClusterAvailable(selfHealNamespace, selfHealCluster))
	waitFor(ctx, t, 2*time.Minute, "3 members ready", readyMembersIs(selfHealCluster, 3))

	original := selfHealMembers(ctx, t)
	if len(original) != 3 {
		t.Fatalf("expected 3 members, got %d: %v", len(original), original)
	}
	victim := original[0]
	victimPVC := "data-" + victim
	t.Logf("corrupting data dir of victim member %q (pvc %q)", victim, victimPVC)

	corruptMemberDataDir(ctx, t, victim)

	// Force the etcd container to restart onto the now-corrupt data dir. PVC
	// members re-use their volume across Pod restarts, so the replacement Pod
	// reads the corrupted data and etcd crash-loops (it will not fall back to
	// --initial-cluster while a data dir is present).
	if err := kube.Delete(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: victim, Namespace: selfHealNamespace}}); err != nil {
		t.Fatalf("delete victim pod %q: %v", victim, err)
	}

	// Self-heal: the stuck member is deleted once it crosses the restart
	// threshold. Generous timeout for the CrashLoopBackOff ramp.
	waitFor(ctx, t, 15*time.Minute, fmt.Sprintf("stuck member %q deleted for replacement", victim),
		func(ctx context.Context) error {
			err := kube.Get(ctx, client.ObjectKey{Namespace: selfHealNamespace, Name: victim}, &etcdv1alpha2.EtcdMember{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("victim %q still present (crash-loop not yet past threshold)", victim)
		})

	// The corrupt member's PVC must be GC'd (owner-ref), discarding the bad
	// data dir so the replacement starts clean.
	waitFor(ctx, t, 5*time.Minute, fmt.Sprintf("victim PVC %q GC'd", victimPVC),
		func(ctx context.Context) error {
			err := kube.Get(ctx, client.ObjectKey{Namespace: selfHealNamespace, Name: victimPVC}, &corev1.PersistentVolumeClaim{})
			if apierrors.IsNotFound(err) {
				return nil
			}
			if err != nil {
				return err
			}
			return fmt.Errorf("PVC %q still present", victimPVC)
		})

	// A fresh member (new GenerateName, not the victim) gap-fills and the
	// cluster returns to 3 ready.
	waitFor(ctx, t, 10*time.Minute, "cluster back to 3 ready members with a fresh replacement",
		func(ctx context.Context) error {
			if err := readyMembersIs(selfHealCluster, 3)(ctx); err != nil {
				return err
			}
			names := selfHealMembersErr(ctx)
			for _, n := range names {
				if n == victim {
					return fmt.Errorf("victim %q still in member set: %v", victim, names)
				}
			}
			return nil
		})

	// The replaced cluster still serves reads and writes.
	assertEtcdReadWrite(ctx, t)
	t.Log("PVC member crash-loop was self-healed; cluster recovered to 3 members and still serves traffic")
}

// readyMembersIs returns a waitFor condition that the cluster reports `want`
// ready members.
func readyMembersIs(name string, want int32) func(context.Context) error {
	return func(ctx context.Context) error {
		ec := &etcdv1alpha2.EtcdCluster{}
		if err := kube.Get(ctx, client.ObjectKey{Namespace: selfHealNamespace, Name: name}, ec); err != nil {
			return err
		}
		if ec.Status.ReadyMembers != want {
			return fmt.Errorf("readyMembers=%d, want %d", ec.Status.ReadyMembers, want)
		}
		return nil
	}
}

// selfHealMembers returns the cluster's EtcdMember names, failing the test on
// a list error.
func selfHealMembers(ctx context.Context, t *testing.T) []string {
	t.Helper()
	list := &etcdv1alpha2.EtcdMemberList{}
	if err := kube.List(ctx, list, client.InNamespace(selfHealNamespace),
		client.MatchingLabels{"etcd-operator.cozystack.io/cluster": selfHealCluster}); err != nil {
		t.Fatalf("list members: %v", err)
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].Name)
	}
	return names
}

// selfHealMembersErr is the error-tolerant form for use inside waitFor (a list
// error returns an empty slice; the caller's own assertions then retry).
func selfHealMembersErr(ctx context.Context) []string {
	list := &etcdv1alpha2.EtcdMemberList{}
	if err := kube.List(ctx, list, client.InNamespace(selfHealNamespace),
		client.MatchingLabels{"etcd-operator.cozystack.io/cluster": selfHealCluster}); err != nil {
		return nil
	}
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].Name)
	}
	return names
}

// corruptMemberDataDir overwrites the head of every file in the member's data
// dir with random bytes, via an ephemeral container that mounts the same PVC
// volume. bbolt/WAL files with corrupt headers make etcd panic on startup —
// the data-loss signature the self-heal targets.
func corruptMemberDataDir(ctx context.Context, t *testing.T, member string) {
	t.Helper()
	pod, err := clientset.CoreV1().Pods(selfHealNamespace).Get(ctx, member, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get victim pod %q: %v", member, err)
	}
	pod.Spec.EphemeralContainers = append(pod.Spec.EphemeralContainers, corev1.EphemeralContainer{
		EphemeralContainerCommon: corev1.EphemeralContainerCommon{
			Name:    "corrupt-data",
			Image:   "busybox:1.36",
			Command: []string{"sh", "-c", "find /var/lib/etcd -type f -exec dd if=/dev/urandom of={} bs=4096 count=1 conv=notrunc \\; ; sync; echo corrupted"},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "data", MountPath: "/var/lib/etcd"},
			},
		},
	})
	if _, err := clientset.CoreV1().Pods(selfHealNamespace).UpdateEphemeralContainers(ctx, member, pod, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("add corrupt-data ephemeral container to %q: %v", member, err)
	}
	waitFor(ctx, t, 3*time.Minute, "data-dir corruption container finished", func(ctx context.Context) error {
		p, err := clientset.CoreV1().Pods(selfHealNamespace).Get(ctx, member, metav1.GetOptions{})
		if err != nil {
			return err
		}
		for _, cs := range p.Status.EphemeralContainerStatuses {
			if cs.Name != "corrupt-data" {
				continue
			}
			if cs.State.Terminated != nil {
				if cs.State.Terminated.ExitCode != 0 {
					t.Fatalf("corrupt-data exited %d: %s", cs.State.Terminated.ExitCode, cs.State.Terminated.Reason)
				}
				return nil
			}
			return fmt.Errorf("corrupt-data not finished: %+v", cs.State)
		}
		return fmt.Errorf("corrupt-data status not reported yet")
	})
}

// assertEtcdReadWrite puts and reads back a key via etcdctl in a ready member,
// proving the recovered cluster serves traffic. Plaintext endpoint (the test
// cluster has no TLS).
func assertEtcdReadWrite(ctx context.Context, t *testing.T) {
	t.Helper()
	pods := &corev1.PodList{}
	if err := kube.List(ctx, pods, client.InNamespace(selfHealNamespace),
		client.MatchingLabels{"etcd-operator.cozystack.io/cluster": selfHealCluster}); err != nil {
		t.Fatalf("list member pods: %v", err)
	}
	var podName string
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase != corev1.PodRunning {
			continue
		}
		for _, cs := range p.Status.ContainerStatuses {
			if cs.Name == "etcd" && cs.Ready {
				podName = p.Name
				break
			}
		}
		if podName != "" {
			break
		}
	}
	if podName == "" {
		t.Fatalf("no ready etcd member pod to probe")
	}

	const key, val = "/e2e/selfheal-probe", "ok"
	if _, stderr, err := podExec(ctx, selfHealNamespace, podName, "etcd", []string{
		"etcdctl", "--endpoints=http://localhost:2379", "put", key, val,
	}); err != nil {
		t.Fatalf("etcdctl put: %v (stderr: %s)", err, stderr)
	}
	stdout, stderr, err := podExec(ctx, selfHealNamespace, podName, "etcd", []string{
		"etcdctl", "--endpoints=http://localhost:2379", "get", key, "--print-value-only",
	})
	if err != nil {
		t.Fatalf("etcdctl get: %v (stderr: %s)", err, stderr)
	}
	if got := trimSpace(stdout); got != val {
		t.Fatalf("etcdctl get %q = %q, want %q", key, got, val)
	}
}

// trimSpace strips trailing whitespace/newline from etcdctl output without
// pulling in strings just for this.
func trimSpace(s string) string {
	for len(s) > 0 {
		c := s[len(s)-1]
		if c == '\n' || c == '\r' || c == ' ' || c == '\t' {
			s = s[:len(s)-1]
			continue
		}
		break
	}
	return s
}
