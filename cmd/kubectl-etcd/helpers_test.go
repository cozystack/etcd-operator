package main

import (
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// The awaitForward tests moved to internal/portforward alongside the
// port-forward machinery itself.

// ── TLS secret discovery on the etcd container ───────────────────────────────

// etcdTLSContainer builds an etcd container + Pod shaped like the operator's
// buildPod output: the client-TLS Secret volume mounted at
// /etc/etcd/tls/client and the given etcd args.
func etcdTLSContainer(args ...string) (*corev1.Pod, corev1.Container) {
	c := corev1.Container{
		Name:         "etcd",
		Command:      append([]string{"etcd"}, args...),
		VolumeMounts: []corev1.VolumeMount{{Name: "tls-client", MountPath: "/etc/etcd/tls/client"}},
	}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{c},
			Volumes: []corev1.Volume{{
				Name:         "tls-client",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "etcd-client-tls"}},
			}},
		},
	}
	return pod, c
}

func TestFindSecretNameForTLS_ServerTLSOnly(t *testing.T) {
	pod, c := etcdTLSContainer("--cert-file=/etc/etcd/tls/client/tls.crt")
	name, mTLS, err := findSecretNameForTLS(pod, c)
	if err != nil {
		t.Fatal(err)
	}
	if name != "etcd-client-tls" {
		t.Errorf("secret name = %q, want %q", name, "etcd-client-tls")
	}
	if mTLS {
		t.Error("no --client-cert-auth/--trusted-ca-file present, mTLS must be false")
	}
}

// mTLS is reported when the server demands a client certificate — via either
// the --trusted-ca-file or the --client-cert-auth flag.
func TestFindSecretNameForTLS_MTLSDetection(t *testing.T) {
	for _, flag := range []string{"--trusted-ca-file=/etc/etcd/tls/client/ca.crt", "--client-cert-auth=true", "--client-cert-auth"} {
		pod, c := etcdTLSContainer("--cert-file=/etc/etcd/tls/client/tls.crt", flag)
		_, mTLS, err := findSecretNameForTLS(pod, c)
		if err != nil {
			t.Fatalf("%s: %v", flag, err)
		}
		if !mTLS {
			t.Errorf("%s must flag mTLS", flag)
		}
	}
}

func TestFindSecretNameForTLS_NoCertFile(t *testing.T) {
	c := corev1.Container{Name: "etcd", Args: []string{"--listen-client-urls=http://0.0.0.0:2379"}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{c}}}
	_, _, err := findSecretNameForTLS(pod, c)
	if !errors.Is(err, errNoCertFile) {
		t.Errorf("expected errNoCertFile sentinel (plaintext cluster), got %v", err)
	}
}

func TestFindSecretNameForTLS_MountNotFound(t *testing.T) {
	c := corev1.Container{Name: "etcd", Args: []string{"--cert-file=/nowhere/tls.crt"}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{c}}}
	if _, _, err := findSecretNameForTLS(pod, c); err == nil {
		t.Error("expected an error when no volume backs the cert path")
	}
}

// ── Credential loading from a kubernetes.io/basic-auth Secret ───────────────

func TestLoadCredentials_Happy(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "ns1"},
		Data: map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte("root"),
			corev1.BasicAuthPasswordKey: []byte("s3cret"),
		},
	})
	u, p, err := loadCredentials(cs, "ns1", "creds")
	if err != nil {
		t.Fatal(err)
	}
	if u != "root" || p != "s3cret" {
		t.Errorf("got %q/%q, want root/s3cret", u, p)
	}
}

func TestLoadCredentials_DefaultsUsernameToRoot(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "ns1"},
		Data:       map[string][]byte{corev1.BasicAuthPasswordKey: []byte("p")},
	})
	u, _, err := loadCredentials(cs, "ns1", "creds")
	if err != nil {
		t.Fatal(err)
	}
	if u != "root" {
		t.Errorf("username default = %q, want root", u)
	}
}

func TestLoadCredentials_MissingPassword(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "ns1"},
		Data:       map[string][]byte{corev1.BasicAuthUsernameKey: []byte("root")},
	})
	if _, _, err := loadCredentials(cs, "ns1", "creds"); err == nil {
		t.Error("expected an error when the password key is missing/empty")
	}
}

func TestLoadCredentials_NamespacedRef(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "creds", Namespace: "other"},
		Data:       map[string][]byte{corev1.BasicAuthPasswordKey: []byte("p")},
	})
	// Default namespace is ns1, but the "other/creds" ref must override it.
	_, p, err := loadCredentials(cs, "ns1", "other/creds")
	if err != nil || p != "p" {
		t.Errorf("namespace/name ref not honored: p=%q err=%v", p, err)
	}
}
