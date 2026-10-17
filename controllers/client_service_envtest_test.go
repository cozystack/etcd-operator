/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"context"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// The ownership semantics of AnnClientServiceTrafficDistribution live in
// the apiserver's server-side apply, which the fake client doesn't
// implement, so these run against envtest. `make test` sets
// KUBEBUILDER_ASSETS; without it the test skips.
func TestClientServiceTrafficDistribution_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; envtest harness not initialized")
	}
	env := &envtest.Environment{Scheme: testScheme(t)}
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

	// Each subtest gets its own namespace and an in-memory cluster: the
	// Services only need its name, UID and annotations.
	setup := func(t *testing.T, ns string) (*lll.EtcdCluster, *EtcdClusterReconciler, *record.FakeRecorder) {
		t.Helper()
		if err := k8s.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatalf("create namespace: %v", err)
		}
		cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: ns, UID: types.UID(ns + "-uid")}}
		rec := record.NewFakeRecorder(20)
		return cluster, &EtcdClusterReconciler{Client: k8s, Scheme: testScheme(t), Recorder: rec}, rec
	}
	reconcile := func(t *testing.T, r *EtcdClusterReconciler, cluster *lll.EtcdCluster, ann *string) {
		t.Helper()
		if ann == nil {
			delete(cluster.Annotations, AnnClientServiceTrafficDistribution)
		} else {
			cluster.Annotations = map[string]string{AnnClientServiceTrafficDistribution: *ann}
		}
		if err := r.ensureServices(ctx, cluster); err != nil {
			t.Fatalf("ensureServices: %v", err)
		}
	}
	// handSet writes trafficDistribution the way `kubectl edit` would: an
	// update-style patch under a different field manager.
	handSet := func(t *testing.T, ns string, v *string) {
		t.Helper()
		svc := &corev1.Service{}
		if err := k8s.Get(ctx, types.NamespacedName{Namespace: ns, Name: "test-client"}, svc); err != nil {
			t.Fatal(err)
		}
		orig := svc.DeepCopy()
		svc.Spec.TrafficDistribution = v
		if err := k8s.Patch(ctx, svc, client.MergeFrom(orig), client.FieldOwner("kubectl-edit")); err != nil {
			t.Fatalf("hand-set trafficDistribution: %v", err)
		}
	}
	preferClose := strPtr(corev1.ServiceTrafficDistributionPreferClose)

	t.Run("set, then remove clears", func(t *testing.T) {
		cluster, r, _ := setup(t, "td-set-remove")
		reconcile(t, r, cluster, preferClose)
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test-client", preferClose)
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test", nil)

		reconcile(t, r, cluster, nil)
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test-client", nil)
	})

	t.Run("drift is reconciled back", func(t *testing.T) {
		cluster, r, _ := setup(t, "td-drift")
		reconcile(t, r, cluster, preferClose)
		handSet(t, cluster.Namespace, nil)
		reconcile(t, r, cluster, preferClose)
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test-client", preferClose)

		// The hand edit took ownership away; the re-apply took it back,
		// so removing the annotation still clears the field.
		reconcile(t, r, cluster, nil)
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test-client", nil)
	})

	t.Run("hand-set value without annotation is left alone", func(t *testing.T) {
		cluster, r, _ := setup(t, "td-hand-set")
		reconcile(t, r, cluster, nil)
		handSet(t, cluster.Namespace, preferClose)
		reconcile(t, r, cluster, nil)
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test-client", preferClose)
	})

	t.Run("invalid value clears and warns", func(t *testing.T) {
		cluster, r, rec := setup(t, "td-invalid")
		reconcile(t, r, cluster, preferClose)
		reconcile(t, r, cluster, strPtr("Anywhere"))
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test-client", nil)
		requireEvent(t, drainEvents(rec), ReasonTrafficDistributionInvalid)
	})

	// PreferSameZone is served from Kubernetes 1.34 (beta, on by default).
	// The envtest apiserver follows the k8s.io/api minor in go.mod, so this
	// checks whichever outcome that version produces: updated in place, or
	// rejected with an Event and the field cleared.
	t.Run("update to PreferSameZone", func(t *testing.T) {
		cluster, r, rec := setup(t, "td-update")
		reconcile(t, r, cluster, preferClose)
		reconcile(t, r, cluster, strPtr(corev1.ServiceTrafficDistributionPreferSameZone))

		svc := &corev1.Service{}
		if err := k8s.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: "test-client"}, svc); err != nil {
			t.Fatal(err)
		}
		if svc.Spec.TrafficDistribution == nil {
			requireEvent(t, drainEvents(rec), ReasonTrafficDistributionRejected)
			t.Logf("apiserver rejected PreferSameZone; field cleared as documented")
			return
		}
		assertTrafficDistribution(t, k8s, cluster.Namespace, "test-client", strPtr(corev1.ServiceTrafficDistributionPreferSameZone))
	})
}
