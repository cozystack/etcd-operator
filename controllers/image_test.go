package controllers

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

func TestResolveEtcdImage_ClusterOverrides(t *testing.T) {
	member := func(repo, suffix string) *lll.EtcdMember {
		return &lll.EtcdMember{Spec: lll.EtcdMemberSpec{Version: "3.6.1", ImageRepository: repo, ImageTagSuffix: suffix}}
	}
	cases := []struct {
		name        string
		member      *lll.EtcdMember
		defaultRepo string
		want        string
	}{
		{"cluster repository wins over the operator default", member("ghcr.io/aenix-io/threek", ""), "registry.internal/mirror/etcd", "ghcr.io/aenix-io/threek:v3.6.1"},
		{"cluster repository without an operator default", member("ghcr.io/aenix-io/threek", ""), "", "ghcr.io/aenix-io/threek:v3.6.1"},
		{"suffix after the version", member("", "-threek.0.1.0"), "", EtcdImage + ":v3.6.1-threek.0.1.0"},
		{"suffix on the operator default", member("", "-fips"), "registry.internal/mirror/etcd", "registry.internal/mirror/etcd:v3.6.1-fips"},
		{"repository and suffix", member("ghcr.io/aenix-io/threek", "-threek.0.1.0"), "registry.internal/mirror/etcd", "ghcr.io/aenix-io/threek:v3.6.1-threek.0.1.0"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveEtcdImage(tc.member, tc.defaultRepo); got != tc.want {
				t.Errorf("image = %q, want %q", got, tc.want)
			}
		})
	}
}

// The restore init container runs from the etcd image too, so it must follow
// the cluster's image as well.
func TestBuildPod_ClusterImageForEtcdAndRestore(t *testing.T) {
	r := &EtcdMemberReconciler{Scheme: testScheme(t), OperatorImage: "operator:latest", EtcdImageRepository: "registry.internal/mirror/etcd"}
	m := seedMember(&lll.RestoreSpec{Source: lll.SnapshotLocation{
		PVC: &lll.PVCSnapshotLocation{ClaimName: "snap-pvc", SubPath: "b1.db"},
	}})
	m.Spec.ImageRepository = "ghcr.io/aenix-io/threek"
	m.Spec.ImageTagSuffix = "-threek.0.1.0"
	pod := r.buildPod(m, false)

	want := "ghcr.io/aenix-io/threek:v3.6.4-threek.0.1.0"
	if got := pod.Spec.Containers[0].Image; got != want {
		t.Errorf("etcd image = %q, want %q", got, want)
	}
	ic, ok := findInitContainer(pod, "restore")
	if !ok {
		t.Fatalf("restore initContainer missing")
	}
	if ic.Image != want {
		t.Errorf("restore image = %q, want %q", ic.Image, want)
	}
}

func TestSpecEqualsObserved_ImageMatters(t *testing.T) {
	for name, set := range map[string]func(*lll.EtcdClusterSpec){
		"repository": func(s *lll.EtcdClusterSpec) { s.ImageRepository = "ghcr.io/aenix-io/threek" },
		"tag suffix": func(s *lll.EtcdClusterSpec) { s.ImageTagSuffix = "-threek.0.1.0" },
	} {
		t.Run(name, func(t *testing.T) {
			cluster := &lll.EtcdCluster{
				Spec: lll.EtcdClusterSpec{Replicas: ptrInt32(3), Version: "3.6.1", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}},
				Status: lll.EtcdClusterStatus{Observed: &lll.ObservedClusterSpec{
					Replicas: 3, Version: "3.6.1", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
				}},
			}
			set(&cluster.Spec)
			if specEqualsObserved(cluster) {
				t.Fatalf("specEqualsObserved must return false when the image %s differs", name)
			}
			snapshotSpecIntoObserved(cluster)
			if !specEqualsObserved(cluster) {
				t.Fatalf("specEqualsObserved must return true once the image %s is latched", name)
			}
		})
	}
}

func TestBootstrap_SeedCarriesClusterImage(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3), Version: "3.6.1", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			ImageRepository: "ghcr.io/aenix-io/threek", ImageTagSuffix: "-threek.0.1.0",
		},
	}
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(newFakeEtcd(0xdeadbeef))}
	reconcileUntilStable(t, r, c, "test", "ns", 8)

	members := &lll.EtcdMemberList{}
	if err := c.List(context.Background(), members, client.InNamespace("ns")); err != nil {
		t.Fatal(err)
	}
	if len(members.Items) != 1 {
		t.Fatalf("members = %d, want the seed only", len(members.Items))
	}
	seed := members.Items[0]
	if seed.Spec.ImageRepository != "ghcr.io/aenix-io/threek" || seed.Spec.ImageTagSuffix != "-threek.0.1.0" {
		t.Errorf("seed image = %q + %q, want the cluster's", seed.Spec.ImageRepository, seed.Spec.ImageTagSuffix)
	}
}

// TestReconcile_SyncsClusterImageOntoExistingMembers: switching the image of
// a running cluster reaches every member, so deleting Pods one at a time
// rolls the cluster onto the new image.
func TestReconcile_SyncsClusterImageOntoExistingMembers(t *testing.T) {
	observed := &lll.ObservedClusterSpec{
		Replicas: 3, Version: "3.5.17", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
		Options:         &lll.EtcdOptions{SnapshotCount: ptrInt64(10000)},
		ExtraArgs:       map[string]string{"log-level": "info"},
		ImageRepository: "ghcr.io/aenix-io/threek",
		ImageTagSuffix:  "-threek.0.1.0",
	}
	c, _ := newTestClient(t, steadyCluster(t, observed)...)
	reconcileTest(t, c)

	m := mustGet(t, c, "test-1", "ns", &lll.EtcdMember{})
	pod := (&EtcdMemberReconciler{EtcdImageRepository: "registry.internal/mirror/etcd"}).buildPod(m, true)
	if got, want := pod.Spec.Containers[0].Image, "ghcr.io/aenix-io/threek:v3.5.17-threek.0.1.0"; got != want {
		t.Errorf("recreated Pod image = %q, want %q", got, want)
	}
}
