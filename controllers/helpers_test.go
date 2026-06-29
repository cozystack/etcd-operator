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

// TestResolveEtcdImage pins the operator-wide repository resolution: the
// --etcd-image-repository default (or the EtcdImage built-in when unset),
// always tagged "v"+spec.version.
func TestResolveEtcdImage(t *testing.T) {
	member := func(version string) *lll.EtcdMember {
		return &lll.EtcdMember{Spec: lll.EtcdMemberSpec{Version: version}}
	}

	cases := []struct {
		name        string
		member      *lll.EtcdMember
		defaultRepo string
		wantImage   string
	}{
		{
			name:      "no operator default → built-in repo + v<version>",
			member:    member("3.6.11"),
			wantImage: EtcdImage + ":v3.6.11",
		},
		{
			name:        "operator default repo, version-derived tag",
			member:      member("3.6.11"),
			defaultRepo: "registry.internal/mirror/etcd",
			wantImage:   "registry.internal/mirror/etcd:v3.6.11",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if img := resolveEtcdImage(tc.member, tc.defaultRepo); img != tc.wantImage {
				t.Errorf("image = %q, want %q", img, tc.wantImage)
			}
		})
	}
}
