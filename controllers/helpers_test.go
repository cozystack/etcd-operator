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
