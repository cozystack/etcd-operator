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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// tdApiserver models how an apiserver of a given version treats
// spec.trafficDistribution on a Service: rejecting some values (before 1.34
// PreferSameZone / PreferSameNode are Invalid), or not knowing the field at
// all and dropping it from what it stores. It counts the writes that carried
// the field so tests can tell a retry from a quiet reconcile.
type tdApiserver struct {
	reject map[string]bool
	drop   bool

	tdWrites int
}

func (a *tdApiserver) client(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&lll.EtcdCluster{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if svc, ok := obj.(*corev1.Service); ok {
					if err := a.admit(svc); err != nil {
						return err
					}
				}
				return c.Create(ctx, obj, opts...)
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if svc, ok := obj.(*corev1.Service); ok {
					if err := a.admit(svc); err != nil {
						return err
					}
				}
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
}

// admit rejects or strips trafficDistribution ahead of the write. It runs on
// the object the operator is about to send, which for a merge patch is the
// full desired Service, so stripping the field there is what the operator
// gets back from an apiserver that does not know it.
func (a *tdApiserver) admit(svc *corev1.Service) error {
	td := svc.Spec.TrafficDistribution
	if td == nil {
		return nil
	}
	a.tdWrites++
	if a.reject[*td] {
		return apierrors.NewInvalid(corev1.SchemeGroupVersion.WithKind("Service").GroupKind(), svc.Name, field.ErrorList{
			field.NotSupported(field.NewPath("spec", "trafficDistribution"), *td, []string{corev1.ServiceTrafficDistributionPreferClose}),
		})
	}
	if a.drop {
		svc.Spec.TrafficDistribution = nil
	}
	return nil
}

func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case ev := <-rec.Events:
			out = append(out, ev)
		default:
			return out
		}
	}
}

func assertOneEvent(t *testing.T, rec *record.FakeRecorder, reason string) {
	t.Helper()
	evs := drainEvents(rec)
	if len(evs) != 1 || !strings.Contains(evs[0], reason) {
		t.Fatalf("events = %q, want exactly one %s", evs, reason)
	}
}

// fakeClock is a settable clock for the trafficDistribution backoff.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

// A value the apiserver refuses (PreferSameZone before 1.34) must not stop
// the reconcile: ensureServices runs ahead of bootstrap and scaling, so an
// error here would wedge the whole cluster. The client Service is written
// without the field, a Warning Event names the cause, and later reconciles
// don't resend the value until the backoff expires.
func TestEnsureServices_TrafficDistributionRejected(t *testing.T) {
	for name, existing := range map[string][]client.Object{
		"create": nil,
		"patch":  {existingService("test", nil), existingService("test-client", nil)},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := clusterWithTrafficDistribution(strPtr(corev1.ServiceTrafficDistributionPreferSameZone))
			api := &tdApiserver{reject: map[string]bool{corev1.ServiceTrafficDistributionPreferSameZone: true}}
			c := api.client(t, append([]client.Object{cluster}, existing...)...)
			rec := record.NewFakeRecorder(10)
			clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
			r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), Recorder: rec, Now: clock.now}

			if err := r.ensureServices(context.Background(), cluster); err != nil {
				t.Fatalf("ensureServices: %v", err)
			}
			assertTrafficDistribution(t, c, "test-client", nil)
			assertOneEvent(t, rec, EventReasonTrafficDistributionRejected)
			if api.tdWrites != 1 {
				t.Fatalf("writes carrying trafficDistribution = %d, want 1", api.tdWrites)
			}

			if err := r.ensureServices(context.Background(), cluster); err != nil {
				t.Fatalf("second ensureServices: %v", err)
			}
			if api.tdWrites != 1 {
				t.Fatalf("value resent within the backoff: writes = %d, want 1", api.tdWrites)
			}
			if evs := drainEvents(rec); len(evs) != 0 {
				t.Fatalf("events within the backoff = %q, want none", evs)
			}

			// Once the backoff expires the value is tried again, so an
			// upgraded apiserver picks it up without an operator restart.
			clock.t = clock.t.Add(trafficDistributionRetryInterval)
			delete(api.reject, corev1.ServiceTrafficDistributionPreferSameZone)
			if err := r.ensureServices(context.Background(), cluster); err != nil {
				t.Fatalf("ensureServices after backoff: %v", err)
			}
			assertTrafficDistribution(t, c, "test-client", strPtr(corev1.ServiceTrafficDistributionPreferSameZone))
		})
	}
}

// Only a rejection of spec.trafficDistribution alone is worked around; a
// Service invalid for any other reason is still a reconcile error.
func TestEnsureServices_OtherInvalidErrorsPropagate(t *testing.T) {
	cluster := clusterWithTrafficDistribution(strPtr(corev1.ServiceTrafficDistributionPreferClose))
	invalid := apierrors.NewInvalid(corev1.SchemeGroupVersion.WithKind("Service").GroupKind(), "test-client", field.ErrorList{
		field.Invalid(field.NewPath("spec", "ports"), nil, "boom"),
		field.NotSupported(field.NewPath("spec", "trafficDistribution"), "x", []string{}),
	})
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(cluster).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if obj.GetName() == "test-client" {
					return invalid
				}
				return c.Create(ctx, obj, opts...)
			},
		}).
		Build()
	rec := record.NewFakeRecorder(10)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), Recorder: rec}

	if err := r.ensureServices(context.Background(), cluster); !apierrors.IsInvalid(err) {
		t.Fatalf("ensureServices error = %v, want the Invalid error", err)
	}
	if evs := drainEvents(rec); len(evs) != 0 {
		t.Fatalf("events = %q, want none", evs)
	}
}

// An apiserver that does not know spec.trafficDistribution (1.29/1.30)
// accepts the write and silently drops the field. Without a check the
// comparison never converges and the Service is patched on every reconcile;
// instead the operator notices the value did not survive, emits a Warning
// Event and stops resending it for the backoff window.
func TestEnsureServices_TrafficDistributionNotPersisted(t *testing.T) {
	for name, existing := range map[string][]client.Object{
		"create": nil,
		"patch":  {existingService("test", nil), existingService("test-client", nil)},
	} {
		t.Run(name, func(t *testing.T) {
			cluster := clusterWithTrafficDistribution(strPtr(corev1.ServiceTrafficDistributionPreferClose))
			api := &tdApiserver{drop: true}
			c := api.client(t, append([]client.Object{cluster}, existing...)...)
			rec := record.NewFakeRecorder(10)
			clock := &fakeClock{t: time.Unix(1_700_000_000, 0)}
			r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), Recorder: rec, Now: clock.now}

			for i := range 3 {
				if err := r.ensureServices(context.Background(), cluster); err != nil {
					t.Fatalf("ensureServices #%d: %v", i, err)
				}
			}
			assertTrafficDistribution(t, c, "test-client", nil)
			if api.tdWrites != 1 {
				t.Fatalf("writes carrying trafficDistribution = %d, want 1", api.tdWrites)
			}
			assertOneEvent(t, rec, EventReasonTrafficDistributionNotPersisted)

			clock.t = clock.t.Add(trafficDistributionRetryInterval)
			if err := r.ensureServices(context.Background(), cluster); err != nil {
				t.Fatalf("ensureServices after backoff: %v", err)
			}
			if api.tdWrites != 2 {
				t.Fatalf("writes after backoff = %d, want 2", api.tdWrites)
			}
		})
	}
}

// The backoff is per value: changing spec.clientService.trafficDistribution
// to something the apiserver does support applies it straight away.
func TestEnsureServices_TrafficDistributionBackoffClearedBySpecChange(t *testing.T) {
	cluster := clusterWithTrafficDistribution(strPtr(corev1.ServiceTrafficDistributionPreferSameNode))
	api := &tdApiserver{reject: map[string]bool{corev1.ServiceTrafficDistributionPreferSameNode: true}}
	c := api.client(t, cluster, existingService("test", nil), existingService("test-client", nil))
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), Recorder: record.NewFakeRecorder(10)}

	if err := r.ensureServices(context.Background(), cluster); err != nil {
		t.Fatalf("ensureServices: %v", err)
	}
	assertTrafficDistribution(t, c, "test-client", nil)

	cluster.Spec.ClientService.TrafficDistribution = strPtr(corev1.ServiceTrafficDistributionPreferClose)
	if err := r.ensureServices(context.Background(), cluster); err != nil {
		t.Fatalf("ensureServices after spec change: %v", err)
	}
	assertTrafficDistribution(t, c, "test-client", strPtr(corev1.ServiceTrafficDistributionPreferClose))
}
