//go:build e2e

package e2e

import (
	"context"
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
	// imageOverrideNamespace isolates this test's clusters from the Kamaji
	// suite. labelCluster matches controllers.LabelCluster (kept as a literal
	// here, mirroring kamaji_datastore_test.go, to avoid importing the
	// controllers package into the e2e suite).
	imageOverrideNamespace = "airgap-e2e"
	labelCluster           = "etcd-operator.cozystack.io/cluster"

	// These three must stay in sync with hack/e2e.sh, which side-loads the
	// upstream etcd image under both mirror names and deploys the operator
	// with etcdImage.repository=operatorDefaultMirror. The version tracks
	// test/e2e/testdata/02-etcdcluster.yaml.
	imageOverrideVersion  = "3.6.11"
	operatorDefaultMirror = "registry.internal/mirror/etcd"
	perClusterMirror      = "registry.internal/percluster/etcd"
)

// TestEtcdImageOverride proves the air-gap image-override contract end to end
// against a real cluster — the thing the unit tests cannot: that the resolved
// repository actually reaches a member Pod and the member comes up pulling
// from it.
//
//   - Operator-wide default: a cluster with no spec.image resolves to the
//     operator's etcdImage.repository (chart value -> ETCD_IMAGE_REPOSITORY env
//     -> --etcd-image-repository flag -> resolveEtcdImage -> buildPod). Because
//     the harness points that default at a mirror whose name differs from the
//     built-in EtcdImage constant, a typo anywhere in that chain would surface
//     here as the wrong (or unpullable) image.
//   - Per-cluster override: a cluster with spec.image.repository outranks the
//     operator default, and spec.imagePullSecrets rides through to the Pod.
//
// Both clusters reach Available, which means the kubelet actually pulled the
// mirror reference (side-loaded as IfNotPresent) and the member joined quorum.
func TestEtcdImageOverride(t *testing.T) {
	ctx := context.Background()

	// TypeMeta is mandatory for server-side apply: the apiserver resolves the
	// target resource from apiVersion/Kind, which a Go-constructed object
	// (unlike one decoded from YAML) does not carry by default.
	ns := &corev1.Namespace{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{Name: imageOverrideNamespace},
	}
	if err := kube.Patch(ctx, ns, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		t.Fatalf("create namespace %s: %v", imageOverrideNamespace, err)
	}
	t.Cleanup(func() {
		_ = kube.Delete(context.Background(), &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: imageOverrideNamespace}})
	})

	t.Run("operator-wide default reaches the member Pod", func(t *testing.T) {
		const name = "etcd-default"
		createImageCluster(ctx, t, name, nil, nil)
		defer deleteImageCluster(ctx, t, name)

		waitFor(ctx, t, 5*time.Minute, name+" Available",
			etcdClusterAvailable(imageOverrideNamespace, name))

		img := etcdMemberImage(ctx, t, name)
		if want := operatorDefaultMirror + ":v" + imageOverrideVersion; img != want {
			t.Errorf("etcd member image = %q, want operator-wide mirror default %q", img, want)
		}
		// pullPolicy is deliberately not asserted on the live Pod: the apiserver
		// defaults any fixed (non-:latest) tag to IfNotPresent, so it reads back
		// as IfNotPresent regardless of whether the operator set it. The
		// operator-level contract (unset when no image block, propagated when
		// set) is covered by the unit tests TestResolveEtcdImage and
		// TestBuildPod_ImageOverrideAndPullSecrets.
	})

	t.Run("per-cluster spec.image outranks the default", func(t *testing.T) {
		const name = "etcd-percluster"
		// A pull-credentials Secret in the cluster's own namespace, referenced
		// by spec.imagePullSecrets. The side-loaded image needs no real pull,
		// but the Secret must exist and flow through to the Pod unchanged.
		pullSecret := "mirror-regcreds"
		sec := &corev1.Secret{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
			ObjectMeta: metav1.ObjectMeta{Name: pullSecret, Namespace: imageOverrideNamespace},
			Type:       corev1.SecretTypeDockerConfigJson,
			StringData: map[string]string{".dockerconfigjson": `{"auths":{}}`},
		}
		if err := kube.Patch(ctx, sec, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			t.Fatalf("create pull secret: %v", err)
		}

		createImageCluster(ctx, t, name,
			&etcdv1alpha2.EtcdImageSpec{Repository: perClusterMirror},
			[]corev1.LocalObjectReference{{Name: pullSecret}})
		defer deleteImageCluster(ctx, t, name)

		waitFor(ctx, t, 5*time.Minute, name+" Available",
			etcdClusterAvailable(imageOverrideNamespace, name))

		img := etcdMemberImage(ctx, t, name)
		if want := perClusterMirror + ":v" + imageOverrideVersion; img != want {
			t.Errorf("etcd member image = %q, want per-cluster mirror %q", img, want)
		}

		// The pull-secret passthrough (spec.imagePullSecrets -> member Pod) is
		// the genuinely e2e-only half of the contract.
		pod := etcdMemberPod(ctx, t, name)
		if len(pod.Spec.ImagePullSecrets) != 1 || pod.Spec.ImagePullSecrets[0].Name != pullSecret {
			t.Errorf("pod imagePullSecrets = %+v, want [%s]", pod.Spec.ImagePullSecrets, pullSecret)
		}
	})
}

// createImageCluster applies a minimal plaintext single-member EtcdCluster.
func createImageCluster(ctx context.Context, t *testing.T, name string,
	image *etcdv1alpha2.EtcdImageSpec, pullSecrets []corev1.LocalObjectReference) {
	t.Helper()
	one := int32(1)
	ec := &etcdv1alpha2.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: imageOverrideNamespace},
		Spec: etcdv1alpha2.EtcdClusterSpec{
			Replicas: &one,
			Version:  imageOverrideVersion,
			Storage:  etcdv1alpha2.StorageSpec{Size: resource.MustParse("1Gi")},
			Image:    image,
		},
	}
	ec.Spec.ImagePullSecrets = pullSecrets
	if err := kube.Create(ctx, ec); err != nil {
		t.Fatalf("create EtcdCluster %s: %v", name, err)
	}
}

func deleteImageCluster(ctx context.Context, t *testing.T, name string) {
	t.Helper()
	ec := &etcdv1alpha2.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: imageOverrideNamespace}}
	if err := kube.Delete(ctx, ec); err != nil && !apierrors.IsNotFound(err) {
		t.Errorf("delete EtcdCluster %s: %v", name, err)
	}
}

// etcdMemberPod returns one member Pod of the named cluster.
func etcdMemberPod(ctx context.Context, t *testing.T, cluster string) *corev1.Pod {
	t.Helper()
	pods := &corev1.PodList{}
	if err := kube.List(ctx, pods, client.InNamespace(imageOverrideNamespace),
		client.MatchingLabels{labelCluster: cluster}); err != nil {
		t.Fatalf("list member pods for %s: %v", cluster, err)
	}
	if len(pods.Items) == 0 {
		t.Fatalf("no member pods for cluster %s", cluster)
	}
	return &pods.Items[0]
}

// etcdMemberImage returns the etcd container's image from a member Pod of the
// named cluster.
func etcdMemberImage(ctx context.Context, t *testing.T, cluster string) string {
	t.Helper()
	pod := etcdMemberPod(ctx, t, cluster)
	for _, c := range pod.Spec.Containers {
		if c.Name == "etcd" {
			return c.Image
		}
	}
	t.Fatalf("pod %s has no etcd container", pod.Name)
	return "" // unreachable
}
