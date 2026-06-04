//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/tools/remotecommand"
	"k8s.io/client-go/transport/spdy"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha2 "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	// e2eNamespace must stay in sync with two other copies of the literal:
	// the fixtures (test/e2e/testdata/*.yaml, metadata.namespace) and the
	// harness (hack/e2e.sh: `datastore.nameOverride=kamaji-e2e` and the
	// diagnostics dump). Changing one without the others breaks the suite
	// in confusing ways (Kamaji's manager looks up the DataStore by name).
	e2eNamespace = "kamaji-e2e"
	clusterName  = "etcd"
	tcpName      = "tenant1"
	proofName    = "e2e-proof" // ConfigMap created via the tenant API, then grepped out of etcd
)

// TestKamajiDataStore proves the documented consumer story end to end: an
// operator-managed, full-mTLS EtcdCluster serves as the backing store of a
// Kamaji DataStore, a TenantControlPlane comes up on it, its API answers,
// and objects written through the tenant API land as keys in our etcd.
func TestKamajiDataStore(t *testing.T) {
	ctx := context.Background()
	fixtures := fixturePaths(t)

	// 00-namespace, 01-pki — namespace and the cert-manager PKI.
	applyFixture(ctx, t, fixtures[0])
	applyFixture(ctx, t, fixtures[1])
	waitFor(ctx, t, 2*time.Minute, "PKI secrets issued", func(ctx context.Context) error {
		if err := secretExists(e2eNamespace, "etcd-ca-tls", "tls.crt", "tls.key")(ctx); err != nil {
			return err
		}
		return secretExists(e2eNamespace, "kamaji-etcd-client-tls", "tls.crt", "tls.key")(ctx)
	})

	// 02-etcdcluster — the cluster under test.
	applyFixture(ctx, t, fixtures[2])
	waitFor(ctx, t, 5*time.Minute, "EtcdCluster Available", etcdClusterAvailable(e2eNamespace, clusterName))
	waitFor(ctx, t, 1*time.Minute, "operator-issued TLS secrets", func(ctx context.Context) error {
		for _, name := range []string{
			clusterName + "-server-tls",
			clusterName + "-operator-client-tls",
			clusterName + "-peer-tls",
		} {
			if err := secretExists(e2eNamespace, name, "tls.crt", "tls.key")(ctx); err != nil {
				return err
			}
		}
		return nil
	})
	for _, svc := range []string{clusterName, clusterName + "-client"} {
		if err := kube.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: svc}, &corev1.Service{}); err != nil {
			t.Fatalf("expected Service %s/%s: %v", e2eNamespace, svc, err)
		}
	}

	// 03-datastore, 04-tenantcontrolplane — the Kamaji side.
	applyFixture(ctx, t, fixtures[3])
	applyFixture(ctx, t, fixtures[4])
	waitFor(ctx, t, 10*time.Minute, "TenantControlPlane Ready", func(ctx context.Context) error {
		status, err := unstructuredField(ctx, "kamaji.clastix.io/v1alpha1", "TenantControlPlane",
			e2eNamespace, tcpName, "status", "kubernetesResources", "version", "status")
		if err != nil {
			return err
		}
		if status != "Ready" {
			return fmt.Errorf("status.kubernetesResources.version.status=%q", status)
		}
		return nil
	})

	// Reach the tenant API server through a port-forward and prove it works.
	tenant, stop := tenantClient(ctx, t)
	defer stop()

	rt, err := rest.TransportFor(tenant)
	if err != nil {
		t.Fatalf("build tenant transport: %v", err)
	}
	code, body := httpGet(t, rt, tenant.Host+"/healthz")
	if code != http.StatusOK || body != "ok" {
		t.Fatalf("tenant /healthz: code=%d body=%q", code, body)
	}
	t.Log("tenant /healthz ok")

	tenantSet, err := kubernetes.NewForConfig(tenant)
	if err != nil {
		t.Fatalf("build tenant clientset: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: proofName, Namespace: "default"},
		Data:       map[string]string{"written-by": "etcd-operator-e2e"},
	}
	if _, err := tenantSet.CoreV1().ConfigMaps("default").Create(ctx, cm, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("create ConfigMap via tenant API: %v", err)
	}
	got, err := tenantSet.CoreV1().ConfigMaps("default").Get(ctx, proofName, metav1.GetOptions{})
	if err != nil || got.Data["written-by"] != "etcd-operator-e2e" {
		t.Fatalf("read ConfigMap back via tenant API: %v (data=%v)", err, got.Data)
	}
	t.Log("tenant API ConfigMap roundtrip ok")

	// The ConfigMap must exist as a key in *our* etcd — that is the whole
	// point of the DataStore wiring.
	keys := etcdKeys(ctx, t)
	if !strings.Contains(keys, proofName) {
		t.Fatalf("etcd key dump does not contain %q;\nfirst 2000 bytes:\n%s", proofName, truncate(keys, 2000))
	}
	t.Logf("found %q among etcd keys", proofName)

	// Teardown — reverse order, waiting where deletion is asynchronous.
	deleteAndWait(ctx, t, "kamaji.clastix.io/v1alpha1", "TenantControlPlane", e2eNamespace, tcpName, 5*time.Minute)
	deleteAndWait(ctx, t, "kamaji.clastix.io/v1alpha1", "DataStore", "", "kamaji-e2e", 2*time.Minute)
	ec := &etcdv1alpha2.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Namespace: e2eNamespace, Name: clusterName}}
	if err := kube.Delete(ctx, ec); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("delete EtcdCluster: %v", err)
	}
	waitFor(ctx, t, 5*time.Minute, "etcd pods gone", func(ctx context.Context) error {
		pods := &corev1.PodList{}
		if err := kube.List(ctx, pods, client.InNamespace(e2eNamespace),
			client.MatchingLabels{"etcd-operator.cozystack.io/cluster": clusterName}); err != nil {
			return err
		}
		if n := len(pods.Items); n > 0 {
			return fmt.Errorf("%d etcd pods still present", n)
		}
		return nil
	})
}

// tenantClient builds a rest.Config for the tenant API server: admin
// kubeconfig from the Kamaji-generated Secret, transport rerouted through a
// port-forward to the apiserver pod (the kubeconfig points at a ClusterIP
// that is unreachable from outside the management cluster).
func tenantClient(ctx context.Context, t *testing.T) (*rest.Config, func()) {
	t.Helper()

	secretName, err := unstructuredField(ctx, "kamaji.clastix.io/v1alpha1", "TenantControlPlane",
		e2eNamespace, tcpName, "status", "kubeconfig", "admin", "secretName")
	if err != nil {
		t.Fatalf("read admin kubeconfig secret name: %v", err)
	}
	sec := &corev1.Secret{}
	if err := kube.Get(ctx, client.ObjectKey{Namespace: e2eNamespace, Name: secretName}, sec); err != nil {
		t.Fatalf("get kubeconfig secret %s: %v", secretName, err)
	}
	var kubeconfig []byte
	for _, key := range []string{"super-admin.conf", "admin.conf", "super-admin.svc", "admin.svc"} {
		if len(sec.Data[key]) > 0 {
			kubeconfig = sec.Data[key]
			break
		}
	}
	if kubeconfig == nil {
		t.Fatalf("no kubeconfig key in secret %s (keys: %v)", secretName, mapKeys(sec.Data))
	}
	tenant, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		t.Fatalf("parse tenant kubeconfig: %v", err)
	}

	localPort, stop := portForwardAPIServer(ctx, t)

	// Keep certificate verification honest: ServerName stays the original
	// endpoint host (present in the apiserver cert SANs), only the dial
	// target moves to the forwarded port.
	host, _, err := net.SplitHostPort(strings.TrimPrefix(tenant.Host, "https://"))
	if err != nil {
		t.Fatalf("parse tenant host %q: %v", tenant.Host, err)
	}
	tenant.TLSClientConfig.ServerName = host
	tenant.Host = fmt.Sprintf("https://127.0.0.1:%d", localPort)
	return tenant, stop
}

// portForwardAPIServer forwards a random local port to port 6443 of the
// first Ready tenant apiserver pod and returns the local port.
func portForwardAPIServer(ctx context.Context, t *testing.T) (uint16, func()) {
	t.Helper()

	deploy, err := clientset.AppsV1().Deployments(e2eNamespace).Get(ctx, tcpName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get tenant control plane deployment: %v", err)
	}
	selector := metav1.FormatLabelSelector(deploy.Spec.Selector)
	pods, err := clientset.CoreV1().Pods(e2eNamespace).List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil || len(pods.Items) == 0 {
		t.Fatalf("list tenant control plane pods (%s): %v", selector, err)
	}
	var podName string
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning {
			podName = p.Name
			break
		}
	}
	if podName == "" {
		t.Fatalf("no running tenant control plane pod among %d", len(pods.Items))
	}

	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(e2eNamespace).Name(podName).SubResource("portforward")
	transport, upgrader, err := spdy.RoundTripperFor(cfg)
	if err != nil {
		t.Fatalf("build spdy roundtripper: %v", err)
	}
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", &url.URL{
		Scheme: req.URL().Scheme, Host: req.URL().Host, Path: req.URL().Path,
	})

	stopCh := make(chan struct{})
	readyCh := make(chan struct{})
	fw, err := portforward.New(dialer, []string{"0:6443"}, stopCh, readyCh, nil, &testWriter{t})
	if err != nil {
		t.Fatalf("create port-forward: %v", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- fw.ForwardPorts() }()
	select {
	case <-readyCh:
	case err := <-errCh:
		t.Fatalf("port-forward to %s: %v", podName, err)
	case <-time.After(30 * time.Second):
		t.Fatalf("port-forward to %s: timeout", podName)
	}
	ports, err := fw.GetPorts()
	if err != nil || len(ports) == 0 {
		t.Fatalf("get forwarded ports: %v", err)
	}
	t.Logf("port-forward 127.0.0.1:%d -> %s:6443", ports[0].Local, podName)
	return ports[0].Local, func() { close(stopCh) }
}

// etcdKeys dumps all keys from the etcd cluster by exec-ing etcdctl inside
// a member pod, authenticating with the server keypair (issued with
// clientAuth EKU for exactly this kind of loopback use). Member pods carry
// operator-generated names, so the pod is found via the cluster label.
func etcdKeys(ctx context.Context, t *testing.T) string {
	t.Helper()
	pods := &corev1.PodList{}
	if err := kube.List(ctx, pods, client.InNamespace(e2eNamespace),
		client.MatchingLabels{"etcd-operator.cozystack.io/cluster": clusterName}); err != nil || len(pods.Items) == 0 {
		t.Fatalf("list etcd member pods: %v (found %d)", err, len(pods.Items))
	}
	stdout, stderr, err := podExec(ctx, e2eNamespace, pods.Items[0].Name, "etcd", []string{
		"etcdctl",
		"--endpoints=https://localhost:2379",
		"--cert=/etc/etcd/tls/client/tls.crt",
		"--key=/etc/etcd/tls/client/tls.key",
		"--cacert=/etc/etcd/tls/client/ca.crt",
		"get", "", "--prefix", "--keys-only",
	})
	if err != nil {
		t.Fatalf("etcdctl key dump: %v (stderr: %s)", err, stderr)
	}
	return stdout
}

func podExec(ctx context.Context, namespace, pod, container string, command []string) (string, string, error) {
	req := clientset.CoreV1().RESTClient().Post().
		Resource("pods").Namespace(namespace).Name(pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)
	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return "", "", err
	}
	var stdout, stderr bytes.Buffer
	err = exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr})
	return stdout.String(), stderr.String(), err
}

// deleteAndWait deletes an unstructured object and waits until it is gone.
func deleteAndWait(ctx context.Context, t *testing.T, apiVersion, kind, namespace, name string, timeout time.Duration) {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	obj.SetNamespace(namespace)
	obj.SetName(name)
	if err := kube.Delete(ctx, obj); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("delete %s %s/%s: %v", kind, namespace, name, err)
	}
	waitFor(ctx, t, timeout, fmt.Sprintf("%s %s/%s deleted", kind, namespace, name), func(ctx context.Context) error {
		err := kube.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj.DeepCopy())
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		return fmt.Errorf("still present")
	})
}

type testWriter struct{ t *testing.T }

func (w *testWriter) Write(p []byte) (int, error) {
	w.t.Logf("port-forward: %s", strings.TrimSpace(string(p)))
	return len(p), nil
}

func mapKeys(m map[string][]byte) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
