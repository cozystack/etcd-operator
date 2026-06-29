//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha2 "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	// imageOverrideNamespace isolates this test's cluster from the Kamaji
	// suite. labelCluster matches controllers.LabelCluster (kept as a literal
	// here, mirroring kamaji_datastore_test.go, to avoid importing the
	// controllers package into the e2e suite).
	imageOverrideNamespace = "airgap-e2e"
	labelCluster           = "etcd-operator.cozystack.io/cluster"

	// Must stay in sync with hack/e2e.sh, which side-loads the upstream etcd
	// image under operatorDefaultMirror and deploys the operator with
	// etcdImage.repository=operatorDefaultMirror. The version tracks
	// test/e2e/testdata/02-etcdcluster.yaml.
	imageOverrideVersion  = "3.6.11"
	operatorDefaultMirror = "registry.internal/mirror/etcd"
)

// TestEtcdImageOverride proves the air-gap contract end to end against a real
// cluster — the thing the unit tests cannot: that the operator-wide repository
// default actually reaches a member Pod and the member comes up pulling from
// it, and that spec.imagePullSecrets rides through to the Pod.
//
// The operator-wide default resolves through chart value -> ETCD_IMAGE_REPOSITORY
// env -> --etcd-image-repository flag -> resolveEtcdImage -> buildPod. Because
// the harness points that default at a mirror whose name differs from the
// built-in EtcdImage constant, a typo anywhere in that chain would surface here
// as the wrong (or unpullable) image. The cluster reaching Available means the
// kubelet actually pulled the mirror reference (side-loaded as IfNotPresent)
// and the member joined quorum.
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

	// A pull-credentials Secret in the cluster's own namespace, referenced by
	// spec.imagePullSecrets. The side-loaded image needs no real pull, but the
	// Secret must exist and flow through to the Pod unchanged.
	const name, pullSecret = "etcd-airgap", "mirror-regcreds"
	sec := &corev1.Secret{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{Name: pullSecret, Namespace: imageOverrideNamespace},
		Type:       corev1.SecretTypeDockerConfigJson,
		StringData: map[string]string{".dockerconfigjson": `{"auths":{}}`},
	}
	if err := kube.Patch(ctx, sec, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
		t.Fatalf("create pull secret: %v", err)
	}

	one := int32(1)
	ec := &etcdv1alpha2.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: imageOverrideNamespace},
		Spec: etcdv1alpha2.EtcdClusterSpec{
			Replicas:         &one,
			Version:          imageOverrideVersion,
			Storage:          etcdv1alpha2.StorageSpec{Size: resource.MustParse("1Gi")},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: pullSecret}},
		},
	}
	if err := kube.Create(ctx, ec); err != nil {
		t.Fatalf("create EtcdCluster %s: %v", name, err)
	}
	t.Cleanup(func() {
		_ = kube.Delete(context.Background(), &etcdv1alpha2.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: imageOverrideNamespace}})
	})

	waitFor(ctx, t, 5*time.Minute, name+" Available", etcdClusterAvailable(imageOverrideNamespace, name))

	pod := etcdMemberPod(ctx, t, name)

	// Operator-wide repository default reaches the member Pod's etcd container.
	var etcdImage string
	for _, c := range pod.Spec.Containers {
		if c.Name == "etcd" {
			etcdImage = c.Image
		}
	}
	if want := operatorDefaultMirror + ":v" + imageOverrideVersion; etcdImage != want {
		t.Errorf("etcd member image = %q, want operator-wide mirror default %q", etcdImage, want)
	}

	// Pull-secret passthrough (spec.imagePullSecrets -> member Pod).
	if len(pod.Spec.ImagePullSecrets) != 1 || pod.Spec.ImagePullSecrets[0].Name != pullSecret {
		t.Errorf("pod imagePullSecrets = %+v, want [%s]", pod.Spec.ImagePullSecrets, pullSecret)
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
