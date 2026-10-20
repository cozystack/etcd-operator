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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// TestDeniedExtraArgsCoverRenderedFlags renders a member Pod through every
// flag-emitting branch of buildPod (plaintext and TLS, mTLS, peer TLS and
// peer auto-TLS, all typed options) and requires each flag the operator
// renders to be on the extraArgs denylist. A flag added to buildPod without
// a denylist entry fails here.
func TestDeniedExtraArgsCoverRenderedFlags(t *testing.T) {
	r := &EtcdMemberReconciler{}
	quota := int64(1 << 30)
	snapCount := int64(10000)
	base := func() lll.EtcdMemberSpec {
		return lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.6.11",
			Storage:        lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: "m=http://m.test.ns.svc:2380", ClusterToken: "tok",
			Options: &lll.EtcdOptions{
				QuotaBackendBytes:       &quota,
				AutoCompactionMode:      lll.AutoCompactionModePeriodic,
				AutoCompactionRetention: "5m",
				SnapshotCount:           &snapCount,
			},
		}
	}
	plain := base()
	tlsFull := base()
	tlsFull.TLS = &lll.EtcdMemberTLS{
		ClientServerSecretRef: &corev1.LocalObjectReference{Name: "srv"},
		ClientMTLS:            true,
		PeerSecretRef:         &corev1.LocalObjectReference{Name: "peer"},
	}
	autoTLS := base()
	autoTLS.TLS = &lll.EtcdMemberTLS{PeerAutoTLS: true}

	rendered := map[string]bool{}
	for _, spec := range []lll.EtcdMemberSpec{plain, tlsFull, autoTLS} {
		pod := r.buildPod(&lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
			Spec:       spec,
		}, false)
		for _, arg := range pod.Spec.Containers[0].Command[1:] {
			name, _, _ := strings.Cut(strings.TrimPrefix(arg, "--"), "=")
			rendered[name] = true
		}
	}
	// Sanity: the branches above really did render the TLS and options flags.
	for _, want := range []string{"peer-auto-tls", "peer-client-cert-auth", "client-cert-auth", "snapshot-count"} {
		if !rendered[want] {
			t.Fatalf("test fixture did not exercise --%s; rendered %v", want, rendered)
		}
	}
	for name := range rendered {
		if _, ok := deniedExtraArgs[name]; !ok {
			t.Errorf("buildPod renders --%s but deniedExtraArgs does not list it; add it there and to the CEL rule on EtcdClusterSpec.ExtraArgs", name)
		}
	}
}

// TestBuildPod_RendersExtraArgs pins the rendering contract: each entry is
// --key=value with the value untouched, entries come after the operator's
// own flags, sorted by key, and the result does not depend on map order.
func TestBuildPod_RendersExtraArgs(t *testing.T) {
	r := &EtcdMemberReconciler{}
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "m", Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.6.11",
			Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			TLS:     &lll.EtcdMemberTLS{PeerSecretRef: &corev1.LocalObjectReference{Name: "peer"}},
			ExtraArgs: map[string]string{
				"wrapper-mode":     "dry-run",
				"log-level":        "debug",
				"experimental-foo": "",
				"log-outputs":      "a=b, c",
			},
		},
	}
	cmd := r.buildPod(member, false).Spec.Containers[0].Command
	want := []string{
		"--experimental-foo=",
		"--log-level=debug",
		"--log-outputs=a=b, c",
		"--wrapper-mode=dry-run",
	}
	if len(cmd) < len(want) {
		t.Fatalf("command too short: %v", cmd)
	}
	if got := cmd[len(cmd)-len(want):]; !reflect.DeepEqual(got, want) {
		t.Fatalf("extraArgs tail = %v; want %v (full command %v)", got, want, cmd)
	}
	for i := 0; i < 20; i++ {
		if again := r.buildPod(member, false).Spec.Containers[0].Command; !reflect.DeepEqual(again, cmd) {
			t.Fatalf("command not deterministic:\n%v\n%v", cmd, again)
		}
	}
}

// TestExtraArgs_DenylistRejectsEveryFlag runs every denylisted key through
// the render-time guard: checkExtraArgs must name the flag, and
// extraArgFlags must never emit it.
func TestExtraArgs_DenylistRejectsEveryFlag(t *testing.T) {
	for flag := range deniedExtraArgs {
		t.Run(flag, func(t *testing.T) {
			args := map[string]string{"log-level": "debug", flag: "x"}
			err := checkExtraArgs(args)
			if err == nil || !strings.Contains(err.Error(), "--"+flag+" ") {
				t.Fatalf("checkExtraArgs(%v) = %v; want an error naming --%s", args, err, flag)
			}
			if got := extraArgFlags(args); !reflect.DeepEqual(got, []string{"--log-level=debug"}) {
				t.Fatalf("extraArgFlags(%v) = %v; want only the allowed flag", args, got)
			}
		})
	}
	if err := checkExtraArgs(map[string]string{"log-level": "debug", "wrapper-mode": "enforce"}); err != nil {
		t.Fatalf("checkExtraArgs rejected allowed flags: %v", err)
	}
	if err := checkExtraArgs(nil); err != nil {
		t.Fatalf("checkExtraArgs(nil) = %v", err)
	}
}

// TestEnsurePod_RefusesDeniedExtraArgs covers the guard's call site: a
// member carrying a denied key (only reachable by writing the EtcdMember
// directly) gets no Pod, and the error says which flag.
func TestEnsurePod_RefusesDeniedExtraArgs(t *testing.T) {
	ctx := context.Background()
	member := &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "test-0", Namespace: "ns", UID: types.UID("uid"), Labels: memberLabels("test", "test-0")},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "test", Version: "3.6.11", Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")},
			InitialCluster: "x", ClusterToken: "test",
			ExtraArgs: map[string]string{"data-dir": "/tmp"},
		},
	}
	c, _ := newTestClient(t, member)
	r := &EtcdMemberReconciler{Client: c, Scheme: testScheme(t)}

	err := r.ensurePod(ctx, member)
	if err == nil || !strings.Contains(err.Error(), "--data-dir") {
		t.Fatalf("ensurePod = %v; want a refusal naming --data-dir", err)
	}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods); err != nil {
		t.Fatal(err)
	}
	if len(pods.Items) != 0 {
		t.Fatalf("ensurePod created %d Pod(s) despite a denied extraArgs key", len(pods.Items))
	}
}

// spec.extraArgs is latched through status.observed like spec.options, so
// an edit must register as "spec ≠ observed".
func TestSpecEqualsObserved_ExtraArgsMatter(t *testing.T) {
	cluster := &lll.EtcdCluster{
		Spec: lll.EtcdClusterSpec{
			Replicas:  ptrInt32(3),
			Version:   "3.6.11",
			Storage:   lll.StorageSpec{Size: quickQty(t, "1Gi")},
			ExtraArgs: map[string]string{"log-level": "debug"},
		},
		Status: lll.EtcdClusterStatus{
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.6.11",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
		},
	}
	if specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return false when extraArgs differ")
	}
	snapshotSpecIntoObserved(cluster)
	if !reflect.DeepEqual(cluster.Status.Observed.ExtraArgs, cluster.Spec.ExtraArgs) {
		t.Fatalf("snapshotSpecIntoObserved did not latch extraArgs: %v", cluster.Status.Observed.ExtraArgs)
	}
	if !specEqualsObserved(cluster) {
		t.Fatalf("specEqualsObserved must return true once extraArgs are latched")
	}
}
