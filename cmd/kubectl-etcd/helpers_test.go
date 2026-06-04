package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// ── Blocker #2: a failed/never-ready port-forward must not hang ──────────────

func TestAwaitForward_Ready(t *testing.T) {
	ready := make(chan struct{}, 1)
	close(ready)
	if err := awaitForward(ready, make(chan error, 1), make(chan struct{}, 1), time.Second); err != nil {
		t.Fatalf("ready forward should succeed, got %v", err)
	}
}

func TestAwaitForward_ErrorBeforeReady(t *testing.T) {
	// ForwardPorts returns an error without ever closing readyChan — the old
	// code blocked on <-readyChan forever. awaitForward must return the error.
	forwardErr := make(chan error, 1)
	forwardErr <- fmt.Errorf("dial tcp: connection refused")
	err := awaitForward(make(chan struct{}), forwardErr, make(chan struct{}, 1), time.Second)
	if err == nil {
		t.Fatal("a forward failure must return an error, not hang or succeed")
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error should wrap the forward failure, got %v", err)
	}
}

func TestAwaitForward_Timeout(t *testing.T) {
	stop := make(chan struct{}, 1)
	// Nothing ever signals ready and no error arrives → must time out, not hang.
	err := awaitForward(make(chan struct{}), make(chan error, 1), stop, 10*time.Millisecond)
	if err == nil {
		t.Fatal("a never-ready forward must time out, not hang")
	}
	select {
	case <-stop:
	default:
		t.Error("timeout must close stopChan to tear the forwarder down")
	}
}

// ── Blocker #3: TLS discovery and credential loading ────────────────────────

// etcdTLSPod returns a Pod whose etcd container points --trusted-ca-file at a
// secret-backed volume mount, mirroring what the operator builds.
func etcdTLSPod() (*corev1.Pod, corev1.Container) {
	c := corev1.Container{
		Name:         "etcd",
		Command:      []string{"etcd"},
		Args:         []string{"--trusted-ca-file=/etc/etcd/pki/ca/ca.crt"},
		VolumeMounts: []corev1.VolumeMount{{Name: "ca", MountPath: "/etc/etcd/pki/ca"}},
	}
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{c},
			Volumes: []corev1.Volume{{
				Name:         "ca",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "etcd-ca"}},
			}},
		},
	}
	return pod, c
}

func TestFindSecretNameForTLS_Happy(t *testing.T) {
	pod, c := etcdTLSPod()
	name, err := findSecretNameForTLS(pod, c)
	if err != nil {
		t.Fatal(err)
	}
	if name != "etcd-ca" {
		t.Errorf("secret name = %q, want %q", name, "etcd-ca")
	}
}

func TestFindSecretNameForTLS_NoCAFlag(t *testing.T) {
	c := corev1.Container{Name: "etcd", Args: []string{"--listen-client-urls=http://0.0.0.0:2379"}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{c}}}
	_, err := findSecretNameForTLS(pod, c)
	if !errors.Is(err, errNoTrustedCAFile) {
		t.Errorf("expected errNoTrustedCAFile sentinel, got %v", err)
	}
}

func TestFindSecretNameForTLS_MountNotFound(t *testing.T) {
	c := corev1.Container{Name: "etcd", Args: []string{"--trusted-ca-file=/nowhere/ca.crt"}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{c}}}
	if _, err := findSecretNameForTLS(pod, c); err == nil {
		t.Error("expected an error when no volume backs the CA path")
	}
}

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

func TestExtractTLSFiles_Happy(t *testing.T) {
	cert, key := selfSignedPEM(t)
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-ca", Namespace: "ns1"},
		Data:       map[string][]byte{"ca.crt": cert, "tls.crt": cert, "tls.key": key},
	})
	pool, clientCert, err := extractTLSFiles(cs, "ns1", "etcd-ca")
	if err != nil {
		t.Fatal(err)
	}
	if pool == nil || clientCert == nil {
		t.Fatal("expected a non-nil CA pool and client certificate")
	}
}

func TestExtractTLSFiles_MissingCA(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-ca", Namespace: "ns1"},
		Data:       map[string][]byte{"tls.crt": []byte("x"), "tls.key": []byte("y")},
	})
	if _, _, err := extractTLSFiles(cs, "ns1", "etcd-ca"); err == nil {
		t.Error("expected an error when ca.crt is absent from the secret")
	}
}

func TestGetTLSConfig_Plaintext(t *testing.T) {
	c := corev1.Container{Name: "etcd", Args: []string{"--listen-client-urls=http://0.0.0.0:2379"}}
	pod := &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{c}}}
	cfg, err := getTLSConfig(fake.NewSimpleClientset(), pod, "ns1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Errorf("a plaintext cluster should yield a nil TLS config, got %+v", cfg)
	}
}

func TestGetTLSConfig_TLS(t *testing.T) {
	cert, key := selfSignedPEM(t)
	pod, _ := etcdTLSPod()
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-ca", Namespace: "ns1"},
		Data:       map[string][]byte{"ca.crt": cert, "tls.crt": cert, "tls.key": key},
	})
	cfg, err := getTLSConfig(cs, pod, "ns1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg == nil {
		t.Fatal("expected a TLS config for a TLS-enabled cluster")
	}
	if cfg.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %d, want TLS 1.2 (%d)", cfg.MinVersion, tls.VersionTLS12)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 1 {
		t.Error("expected a populated RootCAs pool and exactly one client certificate")
	}
}

// selfSignedPEM returns a fresh self-signed certificate and its EC private key,
// PEM-encoded — enough to satisfy x509 parsing and tls.X509KeyPair in tests.
func selfSignedPEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-etcd"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}
