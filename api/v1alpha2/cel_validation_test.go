/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha2_test

import (
	"context"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// The four CEL XValidation rules on EtcdClusterSpec, end-to-end against
// a real apiserver via envtest. CEL validation is apiserver-side, so
// unit-testing against an in-process fake client cannot exercise these
// contracts — envtest is the right test seam.

func skipIfNoEnvtest(t *testing.T) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; envtest harness not initialized")
	}
}

func TestCEL_StorageMediumImmutable(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("immut-medium")
	c.Spec.Storage.Medium = lll.StorageMediumDefault
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create initial PVC-backed cluster: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	// Flip the medium — must be rejected.
	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.Storage.Medium = lll.StorageMediumMemory
	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted storage.medium flip; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storage.medium is immutable") {
		t.Fatalf("error did not mention immutability: %v", err)
	}
}

func TestCEL_StorageMustBeNonZeroForMemoryOnCreate(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("memstore-zero")
	c.Spec.Storage.Medium = lll.StorageMediumMemory
	c.Spec.Storage.Size = resource.MustParse("0")

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted storage=0 with medium=Memory; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storage.size must be > 0") {
		t.Fatalf("error did not mention non-zero storage requirement: %v", err)
	}
}

func TestCEL_ReplicasZeroWithMemoryRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	// Create-time rejection.
	c := validCluster("zero-mem-create")
	c.Spec.Storage.Medium = lll.StorageMediumMemory
	c.Spec.Replicas = ptr32(0)

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted replicas=0 + Memory on Create; expected rejection")
	}
	if !strings.Contains(err.Error(), "replicas=0 with spec.storage.medium=Memory") {
		t.Fatalf("error did not mention replicas+Memory rejection on Create: %v", err)
	}

	// Update-time rejection: start at 3+Memory, scale to 0 → must be rejected.
	live := validCluster("zero-mem-update")
	live.Spec.Storage.Medium = lll.StorageMediumMemory
	live.Spec.Replicas = ptr32(3)
	if err := k8s.Create(ctx, live); err != nil {
		t.Fatalf("Create baseline memory cluster: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, live) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(live), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.Replicas = ptr32(0)
	err = k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted replicas: 3→0 on memory cluster; expected rejection")
	}
	if !strings.Contains(err.Error(), "replicas=0 with spec.storage.medium=Memory") {
		t.Fatalf("error did not mention replicas+Memory rejection on Update: %v", err)
	}
}

func TestCEL_StorageShrinkRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("storage-shrink")
	c.Spec.Storage.Size = resource.MustParse("1Gi")
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.Storage.Size = resource.MustParse("512Mi")

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted storage shrink 1Gi→512Mi; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storage.size cannot be shrunk") {
		t.Fatalf("error did not mention shrink rejection: %v", err)
	}

	// Growing is fine — sanity check that the rule only blocks shrink.
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get for grow check: %v", err)
	}
	got.Spec.Storage.Size = resource.MustParse("2Gi")
	if err := k8s.Update(ctx, got); err != nil {
		t.Fatalf("growing storage rejected unexpectedly: %v", err)
	}
}

// TestCEL_StorageMustBeNonZero_IntegerInput exercises the kubectl-style
// integer input path (`storage.size: 0` without quotes in YAML, which the
// apiserver receives as a JSON number rather than a string). The
// typed Go API always serializes Quantity as a string, so the
// TestCEL_StorageMustBeNonZeroForMemoryOnCreate test above can't reach
// this code path. CEL's `quantity()` function requires a string;
// without explicit string() coercion, an integer input trips a
// "no such overload" CEL runtime error instead of returning the
// intended human-readable message.
func TestCEL_StorageMustBeNonZero_IntegerInput(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "etcd-operator.cozystack.io",
		Version: "v1alpha2",
		Kind:    "EtcdCluster",
	})
	u.SetName("memstore-zero-int")
	u.SetNamespace("default")
	u.Object["spec"] = map[string]any{
		"replicas": int64(3),
		"version":  "3.5.17",
		"storage": map[string]any{
			"size":   int64(0), // <-- the case we care about
			"medium": "Memory",
		},
	}

	err := k8s.Create(ctx, u)
	if err == nil {
		_ = k8s.Delete(ctx, u)
		t.Fatalf("apiserver accepted storage.size=0 (integer) with medium=Memory; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storage.size must be > 0") {
		t.Fatalf("error did not surface the intended message (CEL coercion missing?): %v", err)
	}
}

// TestCEL_TLSAddOnExistingClusterRejected verifies that flipping a plaintext
// cluster to TLS post-create is rejected. Pointer-field immutability is
// enforced at the spec level via the explicit has(self.tls)==has(oldSelf.tls)
// rule rather than `self == oldSelf` on the field, because the latter only
// fires when both sides are populated.
func TestCEL_TLSAddOnExistingClusterRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("tls-add")
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create plaintext cluster: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.TLS = &lll.EtcdClusterTLS{
		Client: &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		},
	}

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted TLS being added to existing plaintext cluster; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.tls cannot be added") {
		t.Fatalf("error did not mention add/remove rejection: %v", err)
	}
}

// TestCEL_TLSRemoveOnExistingClusterRejected mirrors the previous case for
// the TLS→plaintext direction.
func TestCEL_TLSRemoveOnExistingClusterRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("tls-remove")
	c.Spec.TLS = &lll.EtcdClusterTLS{
		Client: &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		},
	}
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create TLS cluster: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.TLS = nil

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted TLS being removed from existing TLS cluster; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.tls cannot be added") {
		t.Fatalf("error did not mention add/remove rejection: %v", err)
	}
}

// TestCEL_TLSSubfieldChangeRejected verifies that the inner-secret-ref
// immutability rule fires when both sides have tls set but content differs.
// Toggling mTLS on/off post-create or swapping secret refs is the
// intended blocked path.
func TestCEL_TLSSubfieldChangeRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("tls-subfield")
	c.Spec.TLS = &lll.EtcdClusterTLS{
		Client: &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		},
	}
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create TLS cluster: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.TLS.Client.OperatorClientSecretRef = &corev1.LocalObjectReference{Name: "fake-op-client-tls"}

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted mTLS toggle (added operatorClientSecretRef); expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.tls is immutable") {
		t.Fatalf("error did not mention subtree immutability: %v", err)
	}
}

// TestCEL_StorageClassNameAddRejected verifies that adding a
// storageClassName after Create is rejected. A PVC's storageClassName
// is itself immutable, so the operator can only honour a value chosen
// at cluster-creation time.
func TestCEL_StorageClassNameAddRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("sc-add")
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create cluster without storageClassName: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	sc := "replicated"
	got.Spec.Storage.StorageClassName = &sc

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted storageClassName being added to existing cluster; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storage.storageClassName cannot be added") {
		t.Fatalf("error did not mention add/remove rejection: %v", err)
	}
}

// TestCEL_StorageClassNameRemoveRejected mirrors the add case for the
// removal direction.
func TestCEL_StorageClassNameRemoveRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("sc-remove")
	sc := "replicated"
	c.Spec.Storage.StorageClassName = &sc
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create cluster with storageClassName: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.Storage.StorageClassName = nil

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted storageClassName being cleared; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storage.storageClassName cannot be added") {
		t.Fatalf("error did not mention add/remove rejection: %v", err)
	}
}

// TestCEL_StorageClassNameChangeRejected verifies that swapping the
// storageClassName for a different value post-create is rejected by
// the content-immutability rule (separate from the add/remove rule).
func TestCEL_StorageClassNameChangeRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("sc-change")
	first := "replicated"
	c.Spec.Storage.StorageClassName = &first
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	second := "local"
	got.Spec.Storage.StorageClassName = &second

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted storageClassName value swap; expected rejection")
	}
	if !strings.Contains(err.Error(), "spec.storage.storageClassName is immutable") {
		t.Fatalf("error did not mention content immutability: %v", err)
	}
}

// TestCEL_TLSClientCertManagerAndSecretRefMutuallyExclusive pins the
// two-sources-are-an-error contract on the client subtree. Either BYO
// secrets or operator-driven cert-manager — never both.
func TestCEL_TLSClientCertManagerAndSecretRefMutuallyExclusive(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("tls-both-client")
	c.Spec.TLS = &lll.EtcdClusterTLS{Client: &lll.ClientTLS{
		ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		CertManager: &lll.ClientCertManagerTLS{
			ServerIssuerRef: lll.IssuerReference{Name: "my-ca"},
		},
	}}

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted both serverSecretRef and certManager; expected rejection")
	}
	if !strings.Contains(err.Error(), "exactly one of spec.tls.client.serverSecretRef or spec.tls.client.certManager") {
		t.Fatalf("error did not mention mutual exclusion: %v", err)
	}
}

// TestCEL_TLSClientNeitherSecretRefNorCertManager pins the
// neither-source-is-an-error contract: a ClientTLS subtree with neither
// field set is a config that produces no Secret material at all, so it
// must be rejected at admission.
func TestCEL_TLSClientNeitherSecretRefNorCertManager(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("tls-neither-client")
	c.Spec.TLS = &lll.EtcdClusterTLS{Client: &lll.ClientTLS{}}

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted empty ClientTLS; expected rejection")
	}
	if !strings.Contains(err.Error(), "exactly one of spec.tls.client.serverSecretRef or spec.tls.client.certManager") {
		t.Fatalf("error did not mention exactly-one rule: %v", err)
	}
}

// TestCEL_TLSClientOperatorSecretRefCannotMixWithCertManager covers the
// finer-grained mTLS-toggle rule: even when the user picks certManager
// as the source for the server cert, the OPERATOR client cert toggle
// must use certManager.operatorClientIssuerRef, not the BYO
// operatorClientSecretRef.
func TestCEL_TLSClientOperatorSecretRefCannotMixWithCertManager(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("tls-mixed-mtls")
	c.Spec.TLS = &lll.EtcdClusterTLS{Client: &lll.ClientTLS{
		CertManager: &lll.ClientCertManagerTLS{
			ServerIssuerRef: lll.IssuerReference{Name: "my-ca"},
		},
		OperatorClientSecretRef: &corev1.LocalObjectReference{Name: "fake-op-client-tls"},
	}}

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted operatorClientSecretRef + certManager; expected rejection")
	}
	if !strings.Contains(err.Error(), "operatorClientSecretRef cannot be combined with certManager") {
		t.Fatalf("error did not mention mTLS-source mixing: %v", err)
	}
}

// TestCEL_TLSPeerCertManagerAndSecretRefMutuallyExclusive mirrors the
// client-side mutual-exclusion test for the peer subtree.
func TestCEL_TLSPeerCertManagerAndSecretRefMutuallyExclusive(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("tls-both-peer")
	c.Spec.TLS = &lll.EtcdClusterTLS{Peer: &lll.PeerTLS{
		SecretRef:   &corev1.LocalObjectReference{Name: "fake-peer-tls"},
		CertManager: &lll.PeerCertManagerTLS{IssuerRef: lll.IssuerReference{Name: "my-peer-ca"}},
	}}

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted both peer.secretRef and peer.certManager; expected rejection")
	}
	if !strings.Contains(err.Error(), "exactly one of spec.tls.peer.secretRef or spec.tls.peer.certManager") {
		t.Fatalf("error did not mention peer mutual exclusion: %v", err)
	}
}

// TestCEL_HappyPathAccepts is a negative-side guard: a fully valid
// cluster spec must pass the apiserver. Catches accidental rule
// inversions and over-broad CEL expressions.
func TestCEL_HappyPathAccepts(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	cases := []struct {
		name string
		mut  func(*lll.EtcdCluster)
	}{
		{
			name: "PVC default",
			mut:  func(c *lll.EtcdCluster) {},
		},
		{
			name: "memory with positive storage",
			mut: func(c *lll.EtcdCluster) {
				c.Spec.Storage.Medium = lll.StorageMediumMemory
				c.Spec.Storage.Size = resource.MustParse("256Mi")
			},
		},
		{
			name: "replicas zero with PVC backend",
			mut: func(c *lll.EtcdCluster) {
				c.Spec.Replicas = ptr32(0)
				// PVC default; the wedge rule only fires for Memory.
			},
		},
		{
			name: "tls full mTLS on create",
			mut: func(c *lll.EtcdCluster) {
				c.Spec.TLS = &lll.EtcdClusterTLS{
					Client: &lll.ClientTLS{
						ServerSecretRef:         &corev1.LocalObjectReference{Name: "fake-server-tls"},
						OperatorClientSecretRef: &corev1.LocalObjectReference{Name: "fake-op-client-tls"},
					},
					Peer: &lll.PeerTLS{
						SecretRef: &corev1.LocalObjectReference{Name: "fake-peer-tls"},
					},
				}
			},
		},
		{
			name: "storage class name on create",
			mut: func(c *lll.EtcdCluster) {
				sc := "replicated"
				c.Spec.Storage.StorageClassName = &sc
			},
		},
		{
			name: "cert-manager mTLS on create",
			mut: func(c *lll.EtcdCluster) {
				c.Spec.TLS = &lll.EtcdClusterTLS{
					Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
						ServerIssuerRef:         lll.IssuerReference{Name: "my-ca"},
						OperatorClientIssuerRef: &lll.IssuerReference{Name: "my-ca"},
					}},
					Peer: &lll.PeerTLS{CertManager: &lll.PeerCertManagerTLS{
						IssuerRef: lll.IssuerReference{Name: "my-peer-ca"},
					}},
				}
			},
		},
		{
			name: "cert-manager mTLS with ClusterIssuer kind",
			mut: func(c *lll.EtcdCluster) {
				c.Spec.TLS = &lll.EtcdClusterTLS{
					Client: &lll.ClientTLS{CertManager: &lll.ClientCertManagerTLS{
						ServerIssuerRef:         lll.IssuerReference{Name: "shared-ca", Kind: "ClusterIssuer"},
						OperatorClientIssuerRef: &lll.IssuerReference{Name: "shared-ca", Kind: "ClusterIssuer"},
					}},
					Peer: &lll.PeerTLS{CertManager: &lll.PeerCertManagerTLS{
						IssuerRef: lll.IssuerReference{Name: "shared-peer-ca", Kind: "ClusterIssuer"},
					}},
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validCluster("happy-" + strings.ReplaceAll(strings.ToLower(tc.name), " ", "-"))
			tc.mut(c)
			if err := k8s.Create(ctx, c); err != nil {
				t.Fatalf("apiserver rejected valid spec: %v", err)
			}
			t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
		})
	}
}

// TestSchema_OptionsValidation exercises the typed spec.options schema
// against a real apiserver: the autoCompactionMode enum, the
// autoCompactionRetention pattern, and the numeric bounds. Not CEL —
// plain OpenAPI validation — but apiserver-enforced all the same, so
// envtest is the seam that actually tests the contract.
func TestSchema_OptionsValidation(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	quota := int64(10200547328)
	snapCount := int64(10000)

	// The exact tuning Cozystack's legacy spec.options carried must be
	// accepted in its typed form.
	ok := validCluster("opts-cozystack-shape")
	ok.Spec.Options = &lll.EtcdOptions{
		QuotaBackendBytes:       &quota,
		AutoCompactionMode:      lll.AutoCompactionModePeriodic,
		AutoCompactionRetention: "5m",
		SnapshotCount:           &snapCount,
	}
	if err := k8s.Create(ctx, ok); err != nil {
		t.Fatalf("apiserver rejected valid options: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, ok) })

	// Bare-integer retention (etcd: hours in periodic mode, revisions in
	// revision mode) is also valid.
	okInt := validCluster("opts-int-retention")
	okInt.Spec.Options = &lll.EtcdOptions{
		AutoCompactionMode:      lll.AutoCompactionModeRevision,
		AutoCompactionRetention: "1000",
	}
	if err := k8s.Create(ctx, okInt); err != nil {
		t.Fatalf("apiserver rejected bare-integer retention: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, okInt) })

	rejects := []struct {
		name string
		mut  func(*lll.EtcdCluster)
	}{
		{
			name: "bad compaction mode",
			mut: func(c *lll.EtcdCluster) {
				c.Spec.Options = &lll.EtcdOptions{AutoCompactionMode: "hourly"}
			},
		},
		{
			name: "garbage retention",
			mut: func(c *lll.EtcdCluster) {
				c.Spec.Options = &lll.EtcdOptions{AutoCompactionRetention: "five-minutes"}
			},
		},
		{
			name: "negative quota",
			mut: func(c *lll.EtcdCluster) {
				q := int64(-1)
				c.Spec.Options = &lll.EtcdOptions{QuotaBackendBytes: &q}
			},
		},
		{
			name: "zero snapshot count",
			mut: func(c *lll.EtcdCluster) {
				s := int64(0)
				c.Spec.Options = &lll.EtcdOptions{SnapshotCount: &s}
			},
		},
	}
	for _, tc := range rejects {
		t.Run(tc.name, func(t *testing.T) {
			c := validCluster("opts-" + strings.ReplaceAll(strings.ToLower(tc.name), " ", "-"))
			tc.mut(c)
			if err := k8s.Create(ctx, c); err == nil {
				_ = k8s.Delete(ctx, c)
				t.Fatalf("apiserver accepted invalid options (%s); expected rejection", tc.name)
			}
		})
	}
}

// spec.auth.enabled requires spec.tls.client — auth credentials must
// not cross a plaintext wire. This rule does not reference oldSelf, so it is
// enforced on CREATE.
func TestCEL_EnableAuthWithoutClientTLSRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("auth-no-tls")
	// Credentials ref present so only the tls.client rule can fire.
	c.Spec.Auth = &lll.AuthSpec{
		Enabled:                  true,
		RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "fake-root-creds"},
	}

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted spec.auth.enabled without spec.tls.client; expected rejection")
	}
	if !strings.Contains(err.Error(), lll.MsgAuthRequiresClientTLS) {
		t.Fatalf("error did not mention the tls.client requirement: %v", err)
	}
}

// spec.auth.enabled requires spec.auth.rootCredentialsSecretRef.
func TestCEL_EnableAuthWithoutCredentialsSecretRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("auth-no-creds")
	c.Spec.TLS = &lll.EtcdClusterTLS{
		Client: &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		},
	}
	c.Spec.Auth = &lll.AuthSpec{Enabled: true} // no rootCredentialsSecretRef

	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted spec.auth.enabled without rootCredentialsSecretRef; expected rejection")
	}
	if !strings.Contains(err.Error(), lll.MsgAuthRequiresCredentialsRef) {
		t.Fatalf("error did not mention the credentials-secret requirement: %v", err)
	}
}

// spec.auth.enabled with at least client server-TLS is accepted (server-TLS-only
// satisfies the requirement — full mTLS is not required for the spec to pass).
func TestCEL_EnableAuthWithServerTLSAccepted(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("auth-server-tls")
	c.Spec.TLS = &lll.EtcdClusterTLS{
		Client: &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		},
	}
	c.Spec.Auth = &lll.AuthSpec{
		Enabled:                  true,
		RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "fake-root-creds"},
	}

	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("apiserver rejected spec.auth.enabled with server-TLS: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
}

// spec.auth.enabled is immutable post-create: flipping it on an existing
// cluster is rejected.
func TestCEL_AuthImmutablePostCreate(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("auth-immutable")
	c.Spec.TLS = &lll.EtcdClusterTLS{
		Client: &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		},
	}
	c.Spec.Auth = &lll.AuthSpec{
		Enabled:                  true,
		RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "fake-root-creds"},
	}
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create auth cluster: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.Auth.Enabled = false

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted flipping spec.auth.enabled post-create; expected rejection")
	}
	if !strings.Contains(err.Error(), lll.MsgAuthImmutable) {
		t.Fatalf("error did not mention auth immutability: %v", err)
	}
}

// The auth subtree cannot be added to an existing cluster.
func TestCEL_AuthAddOnExistingClusterRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("auth-add")
	c.Spec.TLS = &lll.EtcdClusterTLS{
		Client: &lll.ClientTLS{
			ServerSecretRef: &corev1.LocalObjectReference{Name: "fake-server-tls"},
		},
	}
	if err := k8s.Create(ctx, c); err != nil {
		t.Fatalf("Create cluster without auth: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

	got := &lll.EtcdCluster{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	got.Spec.Auth = &lll.AuthSpec{
		Enabled:                  true,
		RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "fake-root-creds"},
	}

	err := k8s.Update(ctx, got)
	if err == nil {
		t.Fatalf("apiserver accepted adding spec.auth to existing cluster; expected rejection")
	}
	if !strings.Contains(err.Error(), lll.MsgAuthAddRemove) {
		t.Fatalf("error did not mention add/remove rejection: %v", err)
	}
}
