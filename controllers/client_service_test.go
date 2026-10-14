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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// clusterWithTrafficDistribution returns a cluster whose client Service asks
// for td; a nil td leaves spec.clientService unset.
func clusterWithTrafficDistribution(td *string) *lll.EtcdCluster {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
	}
	if td != nil {
		cluster.Spec.ClientService = &lll.ClientServiceSpec{TrafficDistribution: td}
	}
	return cluster
}

// existingService returns a Service as it would sit on the apiserver before
// the operator's reconcile, carrying td on spec.trafficDistribution.
func existingService(name string, td *string) *corev1.Service {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: corev1.ServiceSpec{
			Selector:            map[string]string{LabelCluster: "test"},
			Ports:               []corev1.ServicePort{{Name: "client", Port: 2379}},
			TrafficDistribution: td,
		},
	}
	if name == "test" {
		svc.Spec.ClusterIP = corev1.ClusterIPNone
		svc.Spec.PublishNotReadyAddresses = true
		svc.Spec.Ports = append(svc.Spec.Ports, corev1.ServicePort{Name: "peer", Port: 2380})
	}
	return svc
}

func strPtr(s string) *string { return &s }

func getTrafficDistribution(t *testing.T, c client.Client, name string) *string {
	t.Helper()
	svc := &corev1.Service{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: name}, svc); err != nil {
		t.Fatalf("Get Service %s: %v", name, err)
	}
	return svc.Spec.TrafficDistribution
}

func assertTrafficDistribution(t *testing.T, c client.Client, name string, want *string) {
	t.Helper()
	got := getTrafficDistribution(t, c, name)
	switch {
	case want == nil && got != nil:
		t.Fatalf("Service %s trafficDistribution = %q, want unset", name, *got)
	case want != nil && got == nil:
		t.Fatalf("Service %s trafficDistribution unset, want %q", name, *want)
	case want != nil && *got != *want:
		t.Fatalf("Service %s trafficDistribution = %q, want %q", name, *got, *want)
	}
}

// On create the client Service carries the requested value; the headless
// Service never does.
func TestEnsureServices_TrafficDistributionOnCreate(t *testing.T) {
	cluster := clusterWithTrafficDistribution(strPtr(corev1.ServiceTrafficDistributionPreferClose))
	c, _ := newTestClient(t, cluster)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensureServices(context.Background(), cluster); err != nil {
		t.Fatalf("ensureServices: %v", err)
	}
	assertTrafficDistribution(t, c, "test-client", strPtr(corev1.ServiceTrafficDistributionPreferClose))
	assertTrafficDistribution(t, c, "test", nil)
}

// A client Service that drifted (value removed or changed by hand) is
// brought back to the spec.
func TestEnsureServices_TrafficDistributionDriftReconciled(t *testing.T) {
	for name, onService := range map[string]*string{
		"unset":     nil,
		"different": strPtr(corev1.ServiceTrafficDistributionPreferSameNode),
	} {
		t.Run(name, func(t *testing.T) {
			cluster := clusterWithTrafficDistribution(strPtr(corev1.ServiceTrafficDistributionPreferClose))
			c, _ := newTestClient(t, cluster, existingService("test", nil), existingService("test-client", onService))
			r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

			if err := r.ensureServices(context.Background(), cluster); err != nil {
				t.Fatalf("ensureServices: %v", err)
			}
			assertTrafficDistribution(t, c, "test-client", strPtr(corev1.ServiceTrafficDistributionPreferClose))
			assertTrafficDistribution(t, c, "test", nil)
		})
	}
}

// With the field unset — no clientService block, or a block without
// trafficDistribution — the operator does not own it: a value patched onto
// the Service by hand survives reconcile, and no value is added.
func TestEnsureServices_TrafficDistributionUnsetLeavesServiceAlone(t *testing.T) {
	for name, cluster := range map[string]*lll.EtcdCluster{
		"no clientService": clusterWithTrafficDistribution(nil),
		"empty clientService": func() *lll.EtcdCluster {
			cl := clusterWithTrafficDistribution(nil)
			cl.Spec.ClientService = &lll.ClientServiceSpec{}
			return cl
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			manual := strPtr(corev1.ServiceTrafficDistributionPreferClose)
			c, _ := newTestClient(t, cluster, existingService("test", nil), existingService("test-client", manual))
			r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

			if err := r.ensureServices(context.Background(), cluster); err != nil {
				t.Fatalf("ensureServices: %v", err)
			}
			assertTrafficDistribution(t, c, "test-client", manual)
			assertTrafficDistribution(t, c, "test", nil)
		})
	}
}

// The headless Service is not the operator's to set traffic distribution on:
// a value someone put there is neither cleared nor overwritten with the
// client Service's value.
func TestEnsureServices_HeadlessTrafficDistributionUntouched(t *testing.T) {
	cluster := clusterWithTrafficDistribution(strPtr(corev1.ServiceTrafficDistributionPreferClose))
	manual := strPtr(corev1.ServiceTrafficDistributionPreferSameNode)
	c, _ := newTestClient(t, cluster, existingService("test", manual), existingService("test-client", nil))
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t)}

	if err := r.ensureServices(context.Background(), cluster); err != nil {
		t.Fatalf("ensureServices: %v", err)
	}
	assertTrafficDistribution(t, c, "test", manual)
	assertTrafficDistribution(t, c, "test-client", strPtr(corev1.ServiceTrafficDistributionPreferClose))
}
