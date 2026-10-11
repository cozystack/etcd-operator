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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

func defrag(name string, rule *lll.DefragRule) *lll.EtcdDefrag {
	return &lll.EtcdDefrag{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: lll.EtcdDefragSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "c1"},
			Rule:       rule,
		},
	}
}

func freeSpaceRule(q string) *lll.DefragRule {
	v := resource.MustParse(q)
	return &lll.DefragRule{FreeSpaceAbove: &v}
}

// clusterRef and rule are the immutable request; ttlSecondsAfterFinished is a
// retention knob that must stay editable after the run finishes.
func TestCEL_EtcdDefragSpecImmutable(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	t.Run("create accepted", func(t *testing.T) {
		d := defrag("df-create", freeSpaceRule("256Mi"))
		if err := k8s.Create(ctx, d); err != nil {
			t.Fatalf("Create rejected unexpectedly: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, d) })
	})

	t.Run("cannot mutate clusterRef", func(t *testing.T) {
		d := defrag("df-clusterref", nil)
		if err := k8s.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, d) })

		got := &lll.EtcdDefrag{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(d), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		got.Spec.ClusterRef.Name = "c2"
		err := k8s.Update(ctx, got)
		if err == nil {
			t.Fatal("apiserver accepted mutating spec.clusterRef; expected rejection")
		}
		if !strings.Contains(err.Error(), "spec.clusterRef is immutable") {
			t.Fatalf("error did not mention the clusterRef rule: %v", err)
		}
	})

	t.Run("cannot mutate rule", func(t *testing.T) {
		d := defrag("df-rule-mutate", freeSpaceRule("256Mi"))
		if err := k8s.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, d) })

		got := &lll.EtcdDefrag{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(d), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		got.Spec.Rule = freeSpaceRule("512Mi")
		err := k8s.Update(ctx, got)
		if err == nil {
			t.Fatal("apiserver accepted mutating spec.rule; expected rejection")
		}
		if !strings.Contains(err.Error(), "spec.rule is immutable") {
			t.Fatalf("error did not mention the rule immutability: %v", err)
		}
	})

	t.Run("cannot add rule", func(t *testing.T) {
		d := defrag("df-rule-add", nil)
		if err := k8s.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, d) })

		got := &lll.EtcdDefrag{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(d), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		got.Spec.Rule = freeSpaceRule("256Mi")
		err := k8s.Update(ctx, got)
		if err == nil {
			t.Fatal("apiserver accepted adding spec.rule; expected rejection")
		}
		if !strings.Contains(err.Error(), "cannot be added to or removed") {
			t.Fatalf("error did not mention the add/remove rule: %v", err)
		}
	})

	t.Run("cannot remove rule", func(t *testing.T) {
		d := defrag("df-rule-remove", freeSpaceRule("256Mi"))
		if err := k8s.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, d) })

		got := &lll.EtcdDefrag{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(d), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		got.Spec.Rule = nil
		err := k8s.Update(ctx, got)
		if err == nil {
			t.Fatal("apiserver accepted removing spec.rule; expected rejection")
		}
		if !strings.Contains(err.Error(), "cannot be added to or removed") {
			t.Fatalf("error did not mention the add/remove rule: %v", err)
		}
	})

	t.Run("ttlSecondsAfterFinished stays mutable", func(t *testing.T) {
		d := defrag("df-ttl", freeSpaceRule("256Mi"))
		if err := k8s.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, d) })

		got := &lll.EtcdDefrag{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(d), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		ttl := int32(3600)
		got.Spec.TTLSecondsAfterFinished = &ttl
		if err := k8s.Update(ctx, got); err != nil {
			t.Fatalf("apiserver rejected setting spec.ttlSecondsAfterFinished; it must stay mutable: %v", err)
		}
	})

	t.Run("status remains mutable", func(t *testing.T) {
		d := defrag("df-status", freeSpaceRule("256Mi"))
		if err := k8s.Create(ctx, d); err != nil {
			t.Fatalf("Create: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, d) })

		got := &lll.EtcdDefrag{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(d), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		got.Status.Phase = lll.EtcdDefragPhasePending
		if err := k8s.Status().Update(ctx, got); err != nil {
			t.Fatalf("apiserver rejected a status update; status must stay mutable: %v", err)
		}
	})
}
