package controllers

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// TestImageAdmission_Envtest drives spec.imageRepository and
// spec.imageTagSuffix through a real apiserver with the generated CRDs, so
// the CEL rules that keep the tag pinned to spec.version are exercised as
// shipped. `make test` sets KUBEBUILDER_ASSETS; without it the test skips.
func TestImageAdmission_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; envtest harness not initialized")
	}
	env := &envtest.Environment{
		Scheme:                testScheme(t),
		CRDDirectoryPaths:     []string{filepath.Join("..", "charts", "etcd-operator", "crd-bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest start: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	k8s, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := context.Background()

	create := func(repo, suffix string) error {
		c := &lll.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{GenerateName: "image-", Namespace: "default"},
			Spec: lll.EtcdClusterSpec{
				Replicas:        ptrInt32(3),
				Version:         "3.6.1",
				Storage:         lll.StorageSpec{Size: resource.MustParse("128Mi")},
				ImageRepository: repo,
				ImageTagSuffix:  suffix,
			},
		}
		err := k8s.Create(ctx, c)
		if err == nil {
			_ = k8s.Delete(ctx, c)
		}
		return err
	}

	for _, tc := range []struct{ repo, suffix string }{
		{"ghcr.io/aenix-io/threek", ""},
		{"registry.internal:5000/mirror/etcd", ""},
		{"etcd", ""},
		{"", "-threek.0.1.0"},
		{"ghcr.io/aenix-io/threek", "-fips_1.2"},
	} {
		t.Run("accepts "+tc.repo+" "+tc.suffix, func(t *testing.T) {
			if err := create(tc.repo, tc.suffix); err != nil {
				t.Fatalf("apiserver rejected repository %q suffix %q: %v", tc.repo, tc.suffix, err)
			}
		})
	}

	for _, repo := range []string{"ghcr.io/aenix-io/threek:v3.6.1", "registry.internal:5000/etcd:latest", "ghcr.io/aenix-io/threek@sha256:0123"} {
		t.Run("rejects repository "+repo, func(t *testing.T) {
			err := create(repo, "")
			if err == nil || !strings.Contains(err.Error(), "spec.imageRepository is a repository without a tag or digest") {
				t.Fatalf("repository %q: error = %v, want the repository rule", repo, err)
			}
		})
	}

	for _, suffix := range []string{"threek", "-", "-a/b", "-a:b", "-a b"} {
		t.Run("rejects suffix "+suffix, func(t *testing.T) {
			err := create("", suffix)
			if err == nil || !strings.Contains(err.Error(), "spec.imageTagSuffix starts with '-'") {
				t.Fatalf("suffix %q: error = %v, want the suffix rule", suffix, err)
			}
		})
	}
}
