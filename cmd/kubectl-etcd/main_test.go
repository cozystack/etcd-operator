package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/spf13/cobra"
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// Blocker #1: a setup failure must propagate out of the command (RunE) so
// rootCmd.Execute() can translate it into a non-zero exit code, rather than the
// old Run that printed the error and let the process exit 0. PodName is unset,
// so setupEtcdClient fails before touching any cluster — no kubeconfig needed.
func TestStatusCmd_PropagatesSetupError(t *testing.T) {
	cmd := createStatusCmd(&Config{}) // PodName == "" → setup fails fast
	if cmd.RunE == nil {
		t.Fatal("status command must use RunE so failures set a non-zero exit code")
	}
	if err := cmd.RunE(cmd, nil); err == nil {
		t.Fatal("expected an error when the pod name is unset; got nil (would exit 0)")
	}
}

// Every leaf subcommand must be RunE, not Run — a leaf using Run silently
// swallows setup/operation errors and exits 0.
func TestLeafCommandsUseRunE(t *testing.T) {
	config := &Config{}
	leaves := []*cobra.Command{
		createStatusCmd(config),
		createDefragCmd(config),
		createCompactCmd(config),
		createForfeitLeadershipCmd(config),
		createLeaveCmd(config),
		createMembersCmd(config),
		createRemoveMemberCmd(config),
		createAddMemberCmd(config),
		createSnapshotCmd(config),
	}
	// `alarm` is a parent command; its leaves are list/disarm.
	leaves = append(leaves, createAlarmCmd(config).Commands()...)

	for _, c := range leaves {
		if c.RunE == nil {
			t.Errorf("command %q must use RunE (Run swallows errors and exits 0)", c.Name())
		}
	}
}

// Blocker #2: DbSize 0 (freshly initialized member) must not render "NaN%".
func TestInUsePercent_ZeroDbSize(t *testing.T) {
	got := inUsePercent(0, 0)
	if got != "0.00%" {
		t.Errorf("inUsePercent(0,0) = %q, want %q (must not be NaN%%)", got, "0.00%")
	}
	if strings.Contains(got, "NaN") {
		t.Error("inUsePercent must never produce NaN")
	}
}

func TestInUsePercent_Normal(t *testing.T) {
	if got := inUsePercent(200, 50); got != "25.00%" {
		t.Errorf("inUsePercent(200,50) = %q, want %q", got, "25.00%")
	}
}

// Blocker #3: the ERRORS column advertised in the header must actually be
// populated by statusRow. Also guards #2 end-to-end (no NaN in the row).
func TestStatusRow_IncludesErrorsAndNoNaN(t *testing.T) {
	status := &clientv3.StatusResponse{
		Header:      &pb.ResponseHeader{MemberId: 0xabc},
		DbSize:      0, // exercise the divide-by-zero guard within the row
		DbSizeInUse: 0,
		Errors:      []string{"NOSPACE", "CORRUPT"},
	}

	row := statusRow(status)

	if strings.Contains(row, "NaN") {
		t.Errorf("statusRow produced NaN: %q", row)
	}
	for _, want := range []string{"NOSPACE", "CORRUPT"} {
		if !strings.Contains(row, want) {
			t.Errorf("statusRow %q is missing reported error %q", row, want)
		}
	}
	if !strings.Contains(statusHeader(), "ERRORS") {
		t.Fatal("header lost its ERRORS column")
	}
}

// testKeypairPEM generates a self-signed certificate and returns its
// (certPEM, keyPEM). The cert doubles as a CA bundle in tests — RootCAs
// assembly only needs valid PEM, not a real chain.
func testKeypairPEM(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-etcd"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// tlsPod builds a Pod shaped like the operator's buildPod output
// (controllers/etcdmember_controller.go): an "etcd" container with the given
// args and the tls-client Secret volume mounted at /etc/etcd/tls/client.
func tlsPod(args ...string) *corev1.Pod {
	return &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:    "etcd",
				Command: append([]string{"etcd"}, args...),
				VolumeMounts: []corev1.VolumeMount{{
					Name: "tls-client", MountPath: "/etc/etcd/tls/client", ReadOnly: true,
				}},
			}},
			Volumes: []corev1.Volume{{
				Name: "tls-client",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{SecretName: "test-client-tls"},
				},
			}},
		},
	}
}

func tlsSecret(data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "test-client-tls", Namespace: "default"},
		Data:       data,
	}
}

// Server-TLS-only mode: the operator emits --cert-file/--key-file but NOT
// --trusted-ca-file (that flag is mTLS-only). The plugin must still detect
// TLS and build a RootCAs-only config — keying detection on
// --trusted-ca-file made it dial the TLS listener in plaintext. ca.crt alone
// must suffice: the server never demands a client certificate in this mode.
func TestGetTLSConfig_ServerTLSOnly(t *testing.T) {
	caPEM, _ := testKeypairPEM(t)
	pod := tlsPod(
		"--cert-file=/etc/etcd/tls/client/tls.crt",
		"--key-file=/etc/etcd/tls/client/tls.key",
	)
	clientset := fake.NewClientset(tlsSecret(map[string][]byte{"ca.crt": caPEM}))

	tlsConfig, err := getTLSConfig(clientset, pod, "default")
	if err != nil {
		t.Fatalf("getTLSConfig: %v", err)
	}
	if tlsConfig == nil {
		t.Fatal("server-TLS-only cluster yielded a nil *tls.Config (would dial plaintext at a TLS listener)")
	}
	if tlsConfig.RootCAs == nil {
		t.Error("RootCAs is nil; the server certificate cannot be verified")
	}
	if len(tlsConfig.Certificates) != 0 {
		t.Errorf("expected no client certificate in server-TLS-only mode, got %d", len(tlsConfig.Certificates))
	}
}

// mTLS mode: --client-cert-auth/--trusted-ca-file present, so the client
// keypair is required and must be loaded into the config.
func TestGetTLSConfig_MTLS(t *testing.T) {
	certPEM, keyPEM := testKeypairPEM(t)
	pod := tlsPod(
		"--cert-file=/etc/etcd/tls/client/tls.crt",
		"--key-file=/etc/etcd/tls/client/tls.key",
		"--client-cert-auth=true",
		"--trusted-ca-file=/etc/etcd/tls/client/ca.crt",
	)
	clientset := fake.NewClientset(tlsSecret(map[string][]byte{
		"ca.crt": certPEM, "tls.crt": certPEM, "tls.key": keyPEM,
	}))

	tlsConfig, err := getTLSConfig(clientset, pod, "default")
	if err != nil {
		t.Fatalf("getTLSConfig: %v", err)
	}
	if tlsConfig == nil || tlsConfig.RootCAs == nil {
		t.Fatal("mTLS cluster must yield a config with RootCAs set")
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("expected the client certificate to be loaded for mTLS, got %d", len(tlsConfig.Certificates))
	}
}

// mTLS without a keypair in the Secret must fail up front with a clear
// message naming the missing key, not at handshake time.
func TestGetTLSConfig_MTLSRequiresKeypair(t *testing.T) {
	caPEM, _ := testKeypairPEM(t)
	pod := tlsPod(
		"--cert-file=/etc/etcd/tls/client/tls.crt",
		"--key-file=/etc/etcd/tls/client/tls.key",
		"--client-cert-auth=true",
		"--trusted-ca-file=/etc/etcd/tls/client/ca.crt",
	)
	clientset := fake.NewClientset(tlsSecret(map[string][]byte{"ca.crt": caPEM}))

	if _, err := getTLSConfig(clientset, pod, "default"); err == nil || !strings.Contains(err.Error(), "tls.crt") {
		t.Fatalf("expected an error naming the missing %q key, got %v", "tls.crt", err)
	}
}

// Plaintext cluster (no --cert-file at all) still dials without TLS.
func TestGetTLSConfig_Plaintext(t *testing.T) {
	pod := tlsPod() // no TLS flags
	clientset := fake.NewClientset()

	tlsConfig, err := getTLSConfig(clientset, pod, "default")
	if err != nil {
		t.Fatalf("getTLSConfig: %v", err)
	}
	if tlsConfig != nil {
		t.Errorf("plaintext cluster must yield a nil *tls.Config, got %+v", tlsConfig)
	}
}

// forfeit-leadership must not pick a learner: etcd rejects MoveLeader to a
// learner, so with leader + learner + voter the voter must be chosen.
func TestLeadershipTarget_SkipsLearner(t *testing.T) {
	members := []*pb.Member{
		{ID: 1, Name: "leader"},
		{ID: 2, Name: "learner", IsLearner: true},
		{ID: 3, Name: "voter"},
	}

	target := leadershipTarget(members, 1)
	if target == nil {
		t.Fatal("expected a target, got nil")
	}
	if target.ID != 3 {
		t.Errorf("leadershipTarget picked member %d (%s), want the voter (3)", target.ID, target.Name)
	}
}

// A cluster where the only other member is a learner has no eligible target.
func TestLeadershipTarget_NoEligibleMember(t *testing.T) {
	members := []*pb.Member{
		{ID: 1, Name: "leader"},
		{ID: 2, Name: "learner", IsLearner: true},
	}
	if target := leadershipTarget(members, 1); target != nil {
		t.Errorf("expected nil (no eligible member), got member %d", target.ID)
	}
}
