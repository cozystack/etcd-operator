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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// clusterWithTrafficDistributionAnnotation returns a cluster carrying the
// annotation with value v; a nil v leaves it off.
func clusterWithTrafficDistributionAnnotation(v *string) *lll.EtcdCluster {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: "test-uid"},
	}
	if v != nil {
		cluster.Annotations = map[string]string{AnnClientServiceTrafficDistribution: *v}
	}
	return cluster
}

func strPtr(s string) *string { return &s }

func assertTrafficDistribution(t *testing.T, c client.Client, ns, name string, want *string) {
	t.Helper()
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, svc); err != nil {
		t.Fatalf("Get Service %s: %v", name, err)
	}
	got := svc.Spec.TrafficDistribution
	switch {
	case want == nil && got != nil:
		t.Fatalf("Service %s trafficDistribution = %q, want unset", name, *got)
	case want != nil && got == nil:
		t.Fatalf("Service %s trafficDistribution unset, want %q", name, *want)
	case want != nil && *got != *want:
		t.Fatalf("Service %s trafficDistribution = %q, want %q", name, *got, *want)
	}
}

// drainEvents returns every event the FakeRecorder has buffered.
func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func requireEvent(t *testing.T, events []string, reason string) {
	t.Helper()
	for _, e := range events {
		if strings.HasPrefix(e, corev1.EventTypeWarning+" "+reason+" ") {
			return
		}
	}
	t.Fatalf("no Warning %s event among %q", reason, events)
}

// applyInterceptingClient is a fake client whose server-side-apply patches
// (unsupported by the fake) go to onApply instead; applies counts them.
func applyInterceptingClient(t *testing.T, applies *int, onApply func(obj client.Object) error, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if patch.Type() == types.ApplyPatchType {
					*applies++
					return onApply(obj)
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
}

func TestDesiredTrafficDistribution(t *testing.T) {
	for _, tc := range []struct {
		name      string
		ann       *string
		wantValue string
		wantValid bool
	}{
		{"absent", nil, "", true},
		{"PreferClose", strPtr("PreferClose"), "PreferClose", true},
		{"PreferSameZone", strPtr("PreferSameZone"), "PreferSameZone", true},
		{"PreferSameNode", strPtr("PreferSameNode"), "PreferSameNode", true},
		{"empty", strPtr(""), "", false},
		{"wrong case", strPtr("preferclose"), "", false},
		{"unknown", strPtr("Anywhere"), "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, ok := desiredTrafficDistribution(clusterWithTrafficDistributionAnnotation(tc.ann))
			if v != tc.wantValue || ok != tc.wantValid {
				t.Fatalf("got (%q, %v), want (%q, %v)", v, ok, tc.wantValue, tc.wantValid)
			}
		})
	}
}

// Without the annotation and without prior ownership, the operator never
// issues an apply: clusters that don't use the annotation see no extra
// writes, and a value someone set by hand stays.
func TestEnsureServices_NoAnnotationNoApply(t *testing.T) {
	cluster := clusterWithTrafficDistributionAnnotation(nil)
	applies := 0
	c := applyInterceptingClient(t, &applies, func(client.Object) error {
		t.Fatal("unexpected apply")
		return nil
	})
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensureServices(context.Background(), cluster); err != nil {
		t.Fatalf("ensureServices (create): %v", err)
	}
	// Hand-set value on the existing client Service.
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "test-client"}, svc); err != nil {
		t.Fatal(err)
	}
	svc.Spec.TrafficDistribution = strPtr(corev1.ServiceTrafficDistributionPreferClose)
	if err := c.Update(context.Background(), svc); err != nil {
		t.Fatal(err)
	}
	if err := r.ensureServices(context.Background(), cluster); err != nil {
		t.Fatalf("ensureServices (existing): %v", err)
	}
	if applies != 0 {
		t.Fatalf("applies = %d, want 0", applies)
	}
	assertTrafficDistribution(t, c, "ns", "test-client", strPtr(corev1.ServiceTrafficDistributionPreferClose))
}

// An annotation value Kubernetes doesn't define is reported and otherwise
// ignored: the reconcile carries on and nothing is applied.
func TestEnsureServices_InvalidAnnotation(t *testing.T) {
	cluster := clusterWithTrafficDistributionAnnotation(strPtr("Anywhere"))
	applies := 0
	c := applyInterceptingClient(t, &applies, func(client.Object) error { return nil })
	rec := record.NewFakeRecorder(10)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), Recorder: rec}

	for range 2 { // create, then reconcile the existing Services
		if err := r.ensureServices(context.Background(), cluster); err != nil {
			t.Fatalf("ensureServices: %v", err)
		}
	}
	if applies != 0 {
		t.Fatalf("applies = %d, want 0", applies)
	}
	requireEvent(t, drainEvents(rec), ReasonTrafficDistributionInvalid)
	assertTrafficDistribution(t, c, "ns", "test-client", nil)
	assertTrafficDistribution(t, c, "ns", "test", nil)
}

// A value the apiserver refuses (PreferSameZone before Kubernetes 1.34) is
// reported as an Event; ensureServices still succeeds so bootstrap, scaling
// and status updates for the cluster are not blocked.
func TestEnsureServices_ApiserverRejectsValue(t *testing.T) {
	cluster := clusterWithTrafficDistributionAnnotation(strPtr(corev1.ServiceTrafficDistributionPreferSameZone))
	applies := 0
	c := applyInterceptingClient(t, &applies, func(obj client.Object) error {
		return apierrors.NewInvalid(schema.GroupKind{Kind: "Service"}, obj.GetName(), field.ErrorList{
			field.NotSupported(field.NewPath("spec", "trafficDistribution"), "PreferSameZone", []string{"PreferClose"}),
		})
	})
	rec := record.NewFakeRecorder(10)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), Recorder: rec}

	for range 2 {
		if err := r.ensureServices(context.Background(), cluster); err != nil {
			t.Fatalf("ensureServices: %v", err)
		}
	}
	if applies == 0 {
		t.Fatal("expected an apply attempt")
	}
	requireEvent(t, drainEvents(rec), ReasonTrafficDistributionRejected)
	assertTrafficDistribution(t, c, "ns", "test-client", nil)
}

// An apiserver that doesn't serve the field accepts the write and drops the
// value; the operator reports that instead of claiming success.
func TestEnsureServices_ApiserverDropsValue(t *testing.T) {
	cluster := clusterWithTrafficDistributionAnnotation(strPtr(corev1.ServiceTrafficDistributionPreferClose))
	applies := 0
	// The "response" is the applied object minus the field.
	c := applyInterceptingClient(t, &applies, func(obj client.Object) error {
		unstructured.RemoveNestedField(obj.(*unstructured.Unstructured).Object, "spec", "trafficDistribution")
		return nil
	})
	rec := record.NewFakeRecorder(10)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), Recorder: rec}

	for range 2 {
		if err := r.ensureServices(context.Background(), cluster); err != nil {
			t.Fatalf("ensureServices: %v", err)
		}
	}
	requireEvent(t, drainEvents(rec), ReasonTrafficDistributionNotPersisted)
}

// Errors that aren't about the value (apiserver unavailable, throttling)
// fail the reconcile so it retries, like every other Service write.
func TestEnsureServices_TransientApplyErrorRetries(t *testing.T) {
	cluster := clusterWithTrafficDistributionAnnotation(strPtr(corev1.ServiceTrafficDistributionPreferClose))
	applies := 0
	c := applyInterceptingClient(t, &applies, func(client.Object) error {
		return apierrors.NewServiceUnavailable("try later")
	})
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensureServices(context.Background(), cluster); !apierrors.IsServiceUnavailable(err) {
		t.Fatalf("ensureServices err = %v, want ServiceUnavailable", err)
	}
}
