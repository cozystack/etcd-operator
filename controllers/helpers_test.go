/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// TestApplyAdditionalMetadata_OperatorKeysWinOnBothMaps pins the precedence
// rule symmetrically: a user-supplied key colliding with an operator-owned
// one must lose, for labels AND annotations alike. The label half protects
// the operator's selectors; the annotation half is preventive (no operator
// annotations exist today) but the asymmetry must not ship.
func TestApplyAdditionalMetadata_OperatorKeysWinOnBothMaps(t *testing.T) {
	md := &lll.AdditionalMetadata{
		Labels: map[string]string{
			"app.kubernetes.io/managed-by": "evil", // collides — must lose
			"cozystack.io/tenant":          "foo",  // fresh — must merge
		},
		Annotations: map[string]string{
			"etcd-operator.cozystack.io/owned": "evil", // collides — must lose
			"example.com/note":                 "bar",  // fresh — must merge
		},
	}
	labels, annotations := applyAdditionalMetadata(
		map[string]string{"app.kubernetes.io/managed-by": "etcd-operator"},
		map[string]string{"etcd-operator.cozystack.io/owned": "operator"},
		md,
	)

	if got := labels["app.kubernetes.io/managed-by"]; got != "etcd-operator" {
		t.Errorf("operator-owned label clobbered: app.kubernetes.io/managed-by = %q, want etcd-operator", got)
	}
	if got := labels["cozystack.io/tenant"]; got != "foo" {
		t.Errorf("additional label not merged: cozystack.io/tenant = %q, want foo", got)
	}
	if got := annotations["etcd-operator.cozystack.io/owned"]; got != "operator" {
		t.Errorf("operator-owned annotation clobbered: etcd-operator.cozystack.io/owned = %q, want operator", got)
	}
	if got := annotations["example.com/note"]; got != "bar" {
		t.Errorf("additional annotation not merged: example.com/note = %q, want bar", got)
	}
}

// TestApplyAdditionalMetadata_NilInputsStayNil guards the no-op path: with
// no additionalMetadata at all, nil inputs must come back nil (not empty
// allocated maps) so created objects keep clean metadata.
func TestApplyAdditionalMetadata_NilInputsStayNil(t *testing.T) {
	labels, annotations := applyAdditionalMetadata(nil, nil, nil)
	if labels != nil || annotations != nil {
		t.Errorf("applyAdditionalMetadata(nil, nil, nil) = %v, %v; want nil, nil", labels, annotations)
	}

	labels, annotations = applyAdditionalMetadata(nil, nil, &lll.AdditionalMetadata{})
	if labels != nil || annotations != nil {
		t.Errorf("applyAdditionalMetadata with empty md = %v, %v; want nil, nil", labels, annotations)
	}
}

// TestApplyAdditionalMetadata_StripsReservedPrefix proves the reserved keys
// are DROPPED, not merely collision-losing: even when the operator hasn't
// pre-populated them (so collision-precedence couldn't protect anything), a
// user-supplied AnnHeadlessServiceName / AnnDataDirSubPath must never reach
// the merged result. Otherwise every operator-created member would inherit
// the migration knobs (breaking the self-wipe) and data-dir-subpath would
// become a user-controllable path into --data-dir.
func TestApplyAdditionalMetadata_StripsReservedPrefix(t *testing.T) {
	md := &lll.AdditionalMetadata{
		Labels: map[string]string{
			AnnHeadlessServiceName: "evil-svc", // reserved → must be stripped
			"cozystack.io/tenant":  "foo",      // fresh → must merge
		},
		Annotations: map[string]string{
			AnnHeadlessServiceName: "evil-svc",  // reserved → must be stripped
			AnnDataDirSubPath:      "../escape", // reserved → must be stripped
			"example.com/note":     "bar",       // fresh → must merge
		},
	}
	labels, annotations := applyAdditionalMetadata(nil, nil, md)

	if _, present := labels[AnnHeadlessServiceName]; present {
		t.Errorf("reserved label %s reached the merged labels", AnnHeadlessServiceName)
	}
	if labels["cozystack.io/tenant"] != "foo" {
		t.Errorf("non-reserved label dropped: %v", labels)
	}
	for _, k := range []string{AnnHeadlessServiceName, AnnDataDirSubPath} {
		if _, present := annotations[k]; present {
			t.Errorf("reserved annotation %s reached the merged annotations", k)
		}
	}
	if annotations["example.com/note"] != "bar" {
		t.Errorf("non-reserved annotation dropped: %v", annotations)
	}
}

// TestMemberEndpoints_PerMemberServiceName pins that during the mixed
// migration window each member's dial endpoint is built under its OWN
// Service name: an adopted member (AnnHeadlessServiceName=legacy) resolves
// under the legacy headless Service, while a native (rolled) member resolves
// under the cluster's own name. A shared cluster-wide name would dial the
// wrong DNS for half the cluster.
func TestMemberEndpoints_PerMemberServiceName(t *testing.T) {
	adopted := lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "etcd-0",
			Annotations: map[string]string{AnnHeadlessServiceName: "etcd-headless"},
		},
		Spec:   lll.EtcdMemberSpec{ClusterName: "etcd"},
		Status: lll.EtcdMemberStatus{IsVoter: true},
	}
	native := lll.EtcdMember{
		ObjectMeta: metav1.ObjectMeta{Name: "etcd-9"},
		Spec:       lll.EtcdMemberSpec{ClusterName: "etcd"},
		Status:     lll.EtcdMemberStatus{IsVoter: true},
	}

	got := memberEndpoints("http", []lll.EtcdMember{adopted, native}, "ns")
	want := []string{
		"http://etcd-0.etcd-headless.ns.svc:2379",
		"http://etcd-9.etcd.ns.svc:2379",
	}
	if len(got) != len(want) {
		t.Fatalf("endpoints = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("endpoint[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestResolveEtcdImage(t *testing.T) {
	member := func(version string, img *lll.EtcdImageSpec) *lll.EtcdMember {
		return &lll.EtcdMember{Spec: lll.EtcdMemberSpec{Version: version, Image: img}}
	}

	cases := []struct {
		name        string
		member      *lll.EtcdMember
		defaultRepo string
		wantImage   string
		wantPolicy  corev1.PullPolicy
	}{
		{
			name:      "no override, no operator default → built-in repo + v<version>",
			member:    member("3.6.11", nil),
			wantImage: EtcdImage + ":v3.6.11",
		},
		{
			name:        "operator default repo, version-derived tag",
			member:      member("3.6.11", nil),
			defaultRepo: "registry.internal/mirror/etcd",
			wantImage:   "registry.internal/mirror/etcd:v3.6.11",
		},
		{
			name:        "per-cluster repository overrides the operator default",
			member:      member("3.6.11", &lll.EtcdImageSpec{Repository: "private.example/etcd"}),
			defaultRepo: "registry.internal/mirror/etcd",
			wantImage:   "private.example/etcd:v3.6.11",
		},
		{
			name:      "explicit tag overrides the version-derived default",
			member:    member("3.6.11", &lll.EtcdImageSpec{Tag: "3.6.11-mirror"}),
			wantImage: EtcdImage + ":3.6.11-mirror",
		},
		{
			name:       "pull policy is propagated",
			member:     member("3.6.11", &lll.EtcdImageSpec{PullPolicy: corev1.PullAlways}),
			wantImage:  EtcdImage + ":v3.6.11",
			wantPolicy: corev1.PullAlways,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			img, policy := resolveEtcdImage(tc.member, tc.defaultRepo)
			if img != tc.wantImage {
				t.Errorf("image = %q, want %q", img, tc.wantImage)
			}
			if policy != tc.wantPolicy {
				t.Errorf("pullPolicy = %q, want %q", policy, tc.wantPolicy)
			}
		})
	}
}
