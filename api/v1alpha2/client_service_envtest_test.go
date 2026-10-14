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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// spec.clientService.trafficDistribution admits exactly the values the core
// Service API defines; anything else is rejected at admission rather than
// surfacing later as a failed Service patch inside the reconcile loop.
func TestClientService_TrafficDistributionEnum(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	for _, v := range []string{"PreferClose", "PreferSameZone", "PreferSameNode"} {
		c := validCluster("td-" + strings.ToLower(v))
		c.Spec.ClientService = &lll.ClientServiceSpec{TrafficDistribution: &v}
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("apiserver rejected trafficDistribution=%q: %v", v, err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
	}

	bogus := "Anywhere"
	c := validCluster("td-bogus")
	c.Spec.ClientService = &lll.ClientServiceSpec{TrafficDistribution: &bogus}
	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatalf("apiserver accepted trafficDistribution=%q; expected rejection", bogus)
	}
	if !strings.Contains(err.Error(), "spec.clientService.trafficDistribution") {
		t.Fatalf("error did not name the field: %v", err)
	}
}

// The envtest apiserver (pinned to the k8s.io/api minor in go.mod) must
// persist trafficDistribution on a Service. An apiserver with the field
// disabled drops it silently, which would leave the operator re-patching
// the client Service on every reconcile; this guards the version floor the
// field's docs promise.
func TestClientService_ApiserverPersistsTrafficDistribution(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	td := corev1.ServiceTrafficDistributionPreferClose
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "td-probe", Namespace: "default"},
		Spec: corev1.ServiceSpec{
			Ports:               []corev1.ServicePort{{Name: "client", Port: 2379}},
			TrafficDistribution: &td,
		},
	}
	if err := k8s.Create(ctx, svc); err != nil {
		t.Fatalf("Create Service: %v", err)
	}
	t.Cleanup(func() { _ = k8s.Delete(ctx, svc) })

	got := &corev1.Service{}
	if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(svc), got); err != nil {
		t.Fatalf("Get Service: %v", err)
	}
	if got.Spec.TrafficDistribution == nil || *got.Spec.TrafficDistribution != td {
		t.Fatalf("apiserver did not persist trafficDistribution: %v", got.Spec.TrafficDistribution)
	}
}
