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
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// A cluster mid-handover: spec.tls names cert-manager issuance, while the
// members still carry the BYO Secret refs they were created with.
func handoverCluster() *lll.EtcdCluster {
	return &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd", Namespace: "ns", UID: "cluster-uid"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			TLS: &lll.EtcdClusterTLS{
				Client: &lll.ClientTLS{
					CertManager: &lll.ClientCertManagerTLS{
						ServerIssuerRef: lll.IssuerReference{Name: "etcd-issuer"},
					},
				},
				Peer: &lll.PeerTLS{
					CertManager: &lll.PeerCertManagerTLS{
						IssuerRef: lll.IssuerReference{Name: "etcd-peer-issuer"},
					},
				},
			},
		},
	}
}

// byoMember is a member still pinned to the chart-provided Secrets.
func byoMember(name string) *lll.EtcdMember {
	return &lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Spec: lll.EtcdMemberSpec{
			ClusterName: "etcd",
			Version:     "3.5.17",
			TLS: &lll.EtcdMemberTLS{
				ClientServerSecretRef: &corev1.LocalObjectReference{Name: "legacy-server-tls"},
				PeerSecretRef:         &corev1.LocalObjectReference{Name: "legacy-peer-tls"},
			},
		},
	}
}

func tlsSecret(name string, keys ...string) *corev1.Secret {
	data := map[string][]byte{}
	for _, k := range keys {
		data[k] = []byte("x")
	}
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Data:       data,
	}
}

func handoverCondition(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	got := mustGet(t, c, "etcd", "ns", &lll.EtcdCluster{})
	return findClusterCondition(got, lll.ClusterTLSHandover)
}

// Until every Secret named by the new spec is populated, not a single
// member may be repointed. Repointing first would strand the whole cluster
// on a Secret that does not exist, with the old material already gone from
// its spec.
func TestTLSHandover_WaitsForMaterialBeforeTouchingMembers(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	m1, m2 := byoMember("etcd-aaa"), byoMember("etcd-bbb")
	c, s := newTestClient(t, cluster, m1, m2)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	res, err := r.reconcileTLSHandover(ctx, cluster, []lll.EtcdMember{*m1, *m2}, nil)
	if err != nil {
		t.Fatalf("reconcileTLSHandover: %v", err)
	}
	if res == nil {
		t.Fatalf("expected the handover to take over the reconcile while material is missing")
	}

	for _, name := range []string{"etcd-aaa", "etcd-bbb"} {
		got := mustGet(t, c, name, "ns", &lll.EtcdMember{})
		if got.Spec.TLS.PeerSecretRef.Name != "legacy-peer-tls" {
			t.Fatalf("member %s was repointed before its material existed: %+v", name, got.Spec.TLS)
		}
	}

	cond := handoverCondition(t, c)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != lll.TLSHandoverAwaitingMaterial {
		t.Fatalf("want TLSHandover=True/AwaitingMaterial, got %+v", cond)
	}
	if !strings.Contains(cond.Message, "etcd-peer-tls") {
		t.Fatalf("condition should name the material it is waiting on, got %q", cond.Message)
	}
}

// A Secret that exists but has not been issued into yet is not ready. etcd
// started against a half-written Secret crash-loops.
func TestTLSHandover_SecretPresentButUnpopulatedIsNotReady(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	m := byoMember("etcd-aaa")
	c, s := newTestClient(t, cluster, m,
		tlsSecret("etcd-server-tls", corev1.TLSCertKey, corev1.TLSPrivateKeyKey),
		// peer Secret exists but cert-manager has not written the key yet
		tlsSecret("etcd-peer-tls", corev1.TLSCertKey, caCertKey),
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	res, err := r.reconcileTLSHandover(ctx, cluster, []lll.EtcdMember{*m}, nil)
	if err != nil {
		t.Fatalf("reconcileTLSHandover: %v", err)
	}
	if res == nil {
		t.Fatalf("expected a requeue while the peer Secret is incomplete")
	}
	got := mustGet(t, c, "etcd-aaa", "ns", &lll.EtcdMember{})
	if got.Spec.TLS.PeerSecretRef.Name != "legacy-peer-tls" {
		t.Fatalf("member repointed against an unpopulated Secret: %+v", got.Spec.TLS)
	}
	cond := handoverCondition(t, c)
	if cond == nil || cond.Reason != lll.TLSHandoverAwaitingMaterial {
		t.Fatalf("want AwaitingMaterial, got %+v", cond)
	}
	if !strings.Contains(cond.Message, corev1.TLSPrivateKeyKey) {
		t.Fatalf("condition should name the missing key, got %q", cond.Message)
	}
}

// Once the material is ready, every member is repointed in the same pass.
// A staggered roll would leave old and new members unable to authenticate
// to each other for the whole duration, which is a longer outage, not a
// shorter one.
func TestTLSHandover_RepointsEveryMemberInOnePass(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	m1, m2, m3 := byoMember("etcd-aaa"), byoMember("etcd-bbb"), byoMember("etcd-ccc")
	c, s := newTestClient(t, cluster, m1, m2, m3,
		tlsSecret("etcd-server-tls", corev1.TLSCertKey, corev1.TLSPrivateKeyKey),
		tlsSecret("etcd-peer-tls", corev1.TLSCertKey, corev1.TLSPrivateKeyKey, caCertKey),
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	res, err := r.reconcileTLSHandover(ctx, cluster, []lll.EtcdMember{*m1, *m2, *m3}, nil)
	if err != nil {
		t.Fatalf("reconcileTLSHandover: %v", err)
	}
	if res == nil {
		t.Fatalf("expected the handover to take over the reconcile after repointing")
	}

	for _, name := range []string{"etcd-aaa", "etcd-bbb", "etcd-ccc"} {
		got := mustGet(t, c, name, "ns", &lll.EtcdMember{})
		if got.Spec.TLS.ClientServerSecretRef.Name != "etcd-server-tls" {
			t.Errorf("member %s client ref = %q, want etcd-server-tls", name, got.Spec.TLS.ClientServerSecretRef.Name)
		}
		if got.Spec.TLS.PeerSecretRef.Name != "etcd-peer-tls" {
			t.Errorf("member %s peer ref = %q, want etcd-peer-tls", name, got.Spec.TLS.PeerSecretRef.Name)
		}
	}

	cond := handoverCondition(t, c)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != lll.TLSHandoverRollingMembers {
		t.Fatalf("want TLSHandover=True/RollingMembers, got %+v", cond)
	}
}

// Each member must get its own copy of the derived TLS view; sharing one
// pointer across members would make a later mutation of one silently
// rewrite the others.
func TestTLSHandover_MembersDoNotShareTLSPointer(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	m1, m2 := byoMember("etcd-aaa"), byoMember("etcd-bbb")
	c, s := newTestClient(t, cluster, m1, m2,
		tlsSecret("etcd-server-tls", corev1.TLSCertKey, corev1.TLSPrivateKeyKey),
		tlsSecret("etcd-peer-tls", corev1.TLSCertKey, corev1.TLSPrivateKeyKey, caCertKey),
	)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	members := []lll.EtcdMember{*m1, *m2}
	if _, err := r.reconcileTLSHandover(ctx, cluster, members, nil); err != nil {
		t.Fatalf("reconcileTLSHandover: %v", err)
	}
	if members[0].Spec.TLS == members[1].Spec.TLS {
		t.Fatalf("members share the same *EtcdMemberTLS; each needs its own copy")
	}
}

// A conflict is reported, not acted on, and above all does not stop the
// reconcile: the cluster is still serving on its existing material and its
// health status has to keep flowing.
func TestTLSHandover_ConflictReportedWithoutTouchingMembers(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	m := byoMember("etcd-aaa")
	c, s := newTestClient(t, cluster, m)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	conflict := &tlsMaterialConflictError{kind: "Certificate", name: "etcd-peer"}
	res, err := r.reconcileTLSHandover(ctx, cluster, []lll.EtcdMember{*m}, conflict)
	if err != nil {
		t.Fatalf("a conflict must not fail the reconcile: %v", err)
	}
	if res != nil {
		t.Fatalf("a conflict must not take over the reconcile; the rest of the loop still has work to do")
	}

	got := mustGet(t, c, "etcd-aaa", "ns", &lll.EtcdMember{})
	if got.Spec.TLS.PeerSecretRef.Name != "legacy-peer-tls" {
		t.Fatalf("member repointed despite the conflict: %+v", got.Spec.TLS)
	}
	cond := handoverCondition(t, c)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != lll.TLSHandoverBlocked {
		t.Fatalf("want TLSHandover=False/Blocked, got %+v", cond)
	}
	if !strings.Contains(cond.Message, "etcd-peer") {
		t.Fatalf("condition should name the conflicting object, got %q", cond.Message)
	}
}

// A cluster that was born on cert-manager material never had a handover
// and must not acquire a condition claiming one completed.
func TestTLSHandover_NoConditionWhenNothingEverDrifted(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	aligned := byoMember("etcd-aaa")
	aligned.Spec.TLS = deriveMemberTLS(cluster)
	c, s := newTestClient(t, cluster, aligned)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	res, err := r.reconcileTLSHandover(ctx, cluster, []lll.EtcdMember{*aligned}, nil)
	if err != nil {
		t.Fatalf("reconcileTLSHandover: %v", err)
	}
	if res != nil {
		t.Fatalf("aligned cluster should not take over the reconcile")
	}
	if cond := handoverCondition(t, c); cond != nil {
		t.Fatalf("unexpected TLSHandover condition on a cluster that never drifted: %+v", cond)
	}
}

// Once the roll lands, the in-flight condition resolves to Complete rather
// than being left permanently True.
func TestTLSHandover_SettlesToComplete(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	aligned := byoMember("etcd-aaa")
	aligned.Spec.TLS = deriveMemberTLS(cluster)
	// Simulate the previous pass having reported the roll.
	setClusterCondition(cluster, lll.ClusterTLSHandover, metav1.ConditionTrue,
		lll.TLSHandoverRollingMembers, "rolling")
	c, s := newTestClient(t, cluster, aligned)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	if _, err := r.reconcileTLSHandover(ctx, cluster, []lll.EtcdMember{*aligned}, nil); err != nil {
		t.Fatalf("reconcileTLSHandover: %v", err)
	}
	cond := handoverCondition(t, c)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != lll.TLSHandoverComplete {
		t.Fatalf("want TLSHandover=False/Complete, got %+v", cond)
	}
}

// ensureCertificate must never adopt a Certificate another controller owns
// — that is the chart-collision case, and taking it over would mean two
// controllers reconciling one object forever.
func TestEnsureCertificate_RefusesForeignOwnedCertificate(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()

	foreign := &unstructured.Unstructured{}
	foreign.SetGroupVersionKind(schema.GroupVersionKind{Group: "cert-manager.io", Version: "v1", Kind: "Certificate"})
	foreign.SetName("etcd-peer")
	foreign.SetNamespace("ns")
	foreign.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "helm.toolkit.fluxcd.io/v2",
		Kind:       "HelmRelease",
		Name:       "etcd",
		UID:        "helm-uid",
		Controller: ptrBool(true),
	}})

	c, s := newTestClient(t, cluster)
	if err := c.Create(ctx, foreign); err != nil {
		t.Fatalf("seed foreign Certificate: %v", err)
	}
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	err := r.ensureCertificate(ctx, cluster, certificateSpec{
		name:       "etcd-peer",
		secretName: "etcd-peer-tls",
		commonName: "etcd-peer",
		issuerRef:  lll.IssuerReference{Name: "etcd-peer-issuer"},
	})
	conflict, ok := asTLSMaterialConflict(err)
	if !ok {
		t.Fatalf("want a tlsMaterialConflictError, got %v", err)
	}
	if conflict.name != "etcd-peer" || conflict.kind != "Certificate" {
		t.Fatalf("conflict does not identify the object: %+v", conflict)
	}
}

func TestTLSMountsOutOfDate(t *testing.T) {
	podWith := func(clientSecret, peerSecret string) *corev1.Pod {
		p := &corev1.Pod{}
		if clientSecret != "" {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name:         "tls-client",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: clientSecret}},
			})
		}
		if peerSecret != "" {
			p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
				Name:         "tls-peer",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: peerSecret}},
			})
		}
		return p
	}
	memberWith := func(tls *lll.EtcdMemberTLS) *lll.EtcdMember {
		return &lll.EtcdMember{Spec: lll.EtcdMemberSpec{TLS: tls}}
	}
	refs := func(clientSecret, peerSecret string) *lll.EtcdMemberTLS {
		out := &lll.EtcdMemberTLS{}
		if clientSecret != "" {
			out.ClientServerSecretRef = &corev1.LocalObjectReference{Name: clientSecret}
		}
		if peerSecret != "" {
			out.PeerSecretRef = &corev1.LocalObjectReference{Name: peerSecret}
		}
		return out
	}

	cases := []struct {
		name string
		pod  *corev1.Pod
		mem  *lll.EtcdMember
		want bool
	}{
		{"plaintext cluster is never out of date", podWith("", ""), memberWith(nil), false},
		{"matching refs", podWith("s", "p"), memberWith(refs("s", "p")), false},
		{"peer secret renamed", podWith("s", "old-p"), memberWith(refs("s", "p")), true},
		{"client secret renamed", podWith("old-s", "p"), memberWith(refs("s", "p")), true},
		{"both renamed", podWith("old-s", "old-p"), memberWith(refs("s", "p")), true},
		// --peer-auto-tls mounts nothing for the peer plane; that is not drift.
		{"peer-auto-tls", podWith("s", ""), memberWith(&lll.EtcdMemberTLS{
			ClientServerSecretRef: &corev1.LocalObjectReference{Name: "s"},
			PeerAutoTLS:           true,
		}), false},
		// An adopted member keeps the legacy StatefulSet's volume names and
		// is never restarted by adoption. Its Pod carries none of the
		// operator's TLS volumes even though the member spec names Secrets;
		// calling that drift would delete every adopted Pod on the first
		// reconcile after a migration.
		{"adopted pod with foreign volume names", adoptedPod(), memberWith(refs("s", "p")), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tlsMountsOutOfDate(tc.pod, tc.mem); got != tc.want {
				t.Fatalf("tlsMountsOutOfDate = %v, want %v", got, tc.want)
			}
		})
	}
}

// tlsPod builds a member-owned Pod mounting the named TLS Secrets.
func tlsPod(name, clientSecret, peerSecret string, memberUID types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: "ns",
			OwnerReferences: []metav1.OwnerReference{{Kind: "EtcdMember", Name: name, UID: memberUID}},
		},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{
			{Name: "tls-client", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: clientSecret}}},
			{Name: "tls-peer", VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{SecretName: peerSecret}}},
		}},
	}
}

// operatorManaged is the member-side TLS view after a handover.
func operatorManaged() *lll.EtcdMemberTLS {
	return &lll.EtcdMemberTLS{
		ClientServerSecretRef: &corev1.LocalObjectReference{Name: "etcd-server-tls"},
		PeerSecretRef:         &corev1.LocalObjectReference{Name: "etcd-peer-tls"},
	}
}

// A Pod mounting superseded Secrets is torn down so it can be rebuilt.
// Pod volumes are immutable, so a rebuild is the only route onto new
// material.
func TestEnsurePod_RebuildsPodWhenTLSSecretsChange(t *testing.T) {
	ctx := context.Background()
	member := byoMember("etcd-aaa")
	member.UID = "member-uid"
	member.Spec.TLS = operatorManaged()

	pod := tlsPod("etcd-aaa", "legacy-server-tls", "legacy-peer-tls", "member-uid")
	c, s := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: s}

	if err := r.ensurePod(ctx, member); err != nil {
		t.Fatalf("ensurePod: %v", err)
	}
	err := c.Get(ctx, types.NamespacedName{Name: "etcd-aaa", Namespace: "ns"}, &corev1.Pod{})
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Pod mounting superseded TLS Secrets was not deleted (err=%v)", err)
	}
}

// Once rebuilt on matching Secrets the Pod must be left alone, or the
// member spins in a delete loop and never becomes Ready.
func TestEnsurePod_LeavesPodAloneWhenTLSMatches(t *testing.T) {
	ctx := context.Background()
	member := byoMember("etcd-aaa")
	member.UID = "member-uid"
	member.Spec.TLS = operatorManaged()

	pod := tlsPod("etcd-aaa", "etcd-server-tls", "etcd-peer-tls", "member-uid")
	c, s := newTestClient(t, member, pod)
	r := &EtcdMemberReconciler{Client: c, Scheme: s}

	if err := r.ensurePod(ctx, member); err != nil {
		t.Fatalf("ensurePod: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Name: "etcd-aaa", Namespace: "ns"}, &corev1.Pod{}); err != nil {
		t.Fatalf("Pod on current TLS material was deleted: %v", err)
	}
}

// A conflict is reported even when no member happens to be drifting.
// Reporting Complete while the operator cannot own its own material would
// be a lie of omission.
func TestTLSHandover_ConflictReportedEvenWhenMembersAreAligned(t *testing.T) {
	ctx := context.Background()
	cluster := handoverCluster()
	aligned := byoMember("etcd-aaa")
	aligned.Spec.TLS = deriveMemberTLS(cluster)
	c, s := newTestClient(t, cluster, aligned)
	r := &EtcdClusterReconciler{Client: c, Scheme: s}

	conflict := &tlsMaterialConflictError{kind: "Certificate", name: "etcd-server"}
	if _, err := r.reconcileTLSHandover(ctx, cluster, []lll.EtcdMember{*aligned}, conflict); err != nil {
		t.Fatalf("reconcileTLSHandover: %v", err)
	}
	cond := handoverCondition(t, c)
	if cond == nil || cond.Reason != lll.TLSHandoverBlocked {
		t.Fatalf("want Blocked even with aligned members, got %+v", cond)
	}
}

// adoptedPod mimics a member adopted in place by etcd-migrate: the legacy
// StatefulSet's volume names, which the operator never authored.
func adoptedPod() *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Volumes: []corev1.Volume{
		{Name: "etcd-client-certs", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "legacy-server-tls"}}},
		{Name: "etcd-peer-certs", VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{SecretName: "legacy-peer-tls"}}},
	}}}
}
