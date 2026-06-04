//go:build e2e

// Package e2e holds integration tests that run against a real cluster
// (kind in CI) with the operator, cert-manager and Kamaji installed.
// hack/e2e.sh provisions all of that and then runs `make test-e2e`.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdv1alpha2 "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const fieldOwner = client.FieldOwner("etcd-operator-e2e")

var (
	cfg       *rest.Config
	kube      client.Client
	clientset *kubernetes.Clientset
)

func TestMain(m *testing.M) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	c, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, nil).ClientConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: cannot load kubeconfig: %v\n", err)
		os.Exit(1)
	}
	cfg = c

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
	if err := etcdv1alpha2.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
	kube, err = client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: cannot build client: %v\n", err)
		os.Exit(1)
	}
	clientset, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e: cannot build clientset: %v\n", err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// applyFixture server-side-applies every document of a testdata YAML file.
// Unknown-to-the-scheme kinds (cert-manager, Kamaji) go through as
// unstructured, so only the CRDs have to exist on the cluster.
func applyFixture(ctx context.Context, t *testing.T, path string) []*unstructured.Unstructured {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	var applied []*unstructured.Unstructured
	dec := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(raw)))
	for {
		doc, err := dec.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("parse fixture %s: %v", path, err)
		}
		obj := &unstructured.Unstructured{}
		if err := utilyaml.Unmarshal(doc, &obj.Object); err != nil {
			t.Fatalf("decode fixture %s: %v", path, err)
		}
		if len(obj.Object) == 0 { // comment-only document
			continue
		}
		if err := kube.Patch(ctx, obj, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			t.Fatalf("apply %s %s/%s from %s: %v",
				obj.GetKind(), obj.GetNamespace(), obj.GetName(), path, err)
		}
		t.Logf("applied %s %s/%s", obj.GetKind(), obj.GetNamespace(), obj.GetName())
		applied = append(applied, obj)
	}
	return applied
}

// fixturePaths returns testdata/*.yaml sorted by name; the two-digit
// prefixes encode apply order.
func fixturePaths(t *testing.T) []string {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("testdata", "*.yaml"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no fixtures found: %v", err)
	}
	sort.Strings(paths)
	return paths
}

// waitFor polls fn until it returns nil or the timeout elapses. fn's last
// error is included in the failure to make timeouts diagnosable.
func waitFor(ctx context.Context, t *testing.T, timeout time.Duration, desc string, fn func(context.Context) error) {
	t.Helper()
	t.Logf("waiting up to %s for %s", timeout, desc)
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		lastErr = fn(cctx)
		cancel()
		if lastErr == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s: %v", desc, lastErr)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("context cancelled waiting for %s: %v (last: %v)", desc, ctx.Err(), lastErr)
		case <-time.After(5 * time.Second):
		}
	}
}

// secretExists fails the wait round unless the Secret exists and carries
// all the listed keys with non-empty values.
func secretExists(namespace, name string, keys ...string) func(context.Context) error {
	return func(ctx context.Context) error {
		sec := &corev1.Secret{}
		if err := kube.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, sec); err != nil {
			return err
		}
		for _, k := range keys {
			if len(sec.Data[k]) == 0 {
				return fmt.Errorf("secret %s/%s missing key %q", namespace, name, k)
			}
		}
		return nil
	}
}

// etcdClusterAvailable checks the typed EtcdCluster Available condition.
func etcdClusterAvailable(namespace, name string) func(context.Context) error {
	return func(ctx context.Context) error {
		ec := &etcdv1alpha2.EtcdCluster{}
		if err := kube.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, ec); err != nil {
			return err
		}
		cond := apimeta.FindStatusCondition(ec.Status.Conditions, etcdv1alpha2.ClusterAvailable)
		if cond == nil {
			return fmt.Errorf("condition %s not reported yet", etcdv1alpha2.ClusterAvailable)
		}
		if cond.Status != "True" {
			return fmt.Errorf("condition %s=%s (%s): %s", cond.Type, cond.Status, cond.Reason, cond.Message)
		}
		return nil
	}
}

// unstructuredField reads a dotted path from an unstructured object fetched
// live from the cluster.
func unstructuredField(ctx context.Context, apiVersion, kind, namespace, name string, fields ...string) (string, error) {
	obj := &unstructured.Unstructured{}
	obj.SetAPIVersion(apiVersion)
	obj.SetKind(kind)
	if err := kube.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, obj); err != nil {
		return "", err
	}
	val, found, err := unstructured.NestedString(obj.Object, fields...)
	if err != nil || !found {
		return "", fmt.Errorf("field %v not found on %s %s/%s: %v", fields, kind, namespace, name, err)
	}
	return val, nil
}

// httpGet performs a plain GET against url with the given TLS-configured
// transport and returns the body.
func httpGet(t *testing.T, rt http.RoundTripper, url string) (int, string) {
	t.Helper()
	httpClient := &http.Client{Transport: rt, Timeout: 15 * time.Second}
	resp, err := httpClient.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("GET %s: read body: %v", url, err)
	}
	return resp.StatusCode, string(body)
}
