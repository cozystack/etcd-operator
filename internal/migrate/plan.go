/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package migrate translates legacy etcd.aenix.io/v1alpha1 resources into
// their etcd-operator.cozystack.io/v1alpha2 equivalents. The translation
// layer is pure — it never touches a cluster — so the orchestration in
// cmd/etcd-migrate can render the same plan in dry-run and apply modes.
package migrate

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Action says what the tool intends to do for one planned resource.
type Action string

const (
	// ActionAdopt performs an in-place adoption of a live legacy cluster:
	// create the new-API CRs with prefilled status, re-own the existing
	// pods/PVCs/Services, dismantle the legacy CR + StatefulSet with
	// Orphan propagation. The etcd pods are never touched.
	ActionAdopt Action = "Adopt"
	// ActionCreate creates a new-API object (and afterwards deletes the
	// legacy source named by DeleteRef, when set).
	ActionCreate Action = "Create"
	// ActionPrint renders a manifest for the user to review and apply
	// manually (EtcdBackupSchedule → CronJob). Nothing is written and the
	// legacy source is NOT deleted.
	ActionPrint Action = "Print"
	// ActionSkip means the target already exists; the plan is re-runnable.
	ActionSkip Action = "Skip"
	// ActionError means the source cannot be migrated; nothing is created
	// or deleted for it.
	ActionError Action = "Error"
)

// ObjectRef names a legacy object to delete after its replacement exists.
type ObjectRef struct {
	GVR       schema.GroupVersionResource
	Namespace string
	Name      string
}

// ResourcePlan is the per-resource unit the tool renders or applies.
type ResourcePlan struct {
	// SourceKind/SourceName/Namespace identify the legacy object.
	SourceKind string
	SourceName string
	Namespace  string

	Action Action

	// Target is the new-API object to create (EtcdCluster, EtcdSnapshot,
	// generated Secret) or to print (CronJob + RBAC). nil on pure errors.
	Target client.Object
	// Extras are companion objects sharing Target's fate: created right
	// before Target on ActionCreate (e.g. a generated root-credentials
	// Secret), printed alongside it on ActionPrint (e.g. the CronJob's
	// ServiceAccount/Role/RoleBinding).
	Extras []client.Object

	// DeleteRef names the legacy object to delete once Target exists.
	// nil for ActionPrint/ActionError (and for ActionSkip deletes still
	// proceed — the target exists, the source is leftover). For
	// ActionAdopt the delete uses Orphan propagation: the children
	// (StatefulSet, Services) must survive the legacy CR.
	DeleteRef *ObjectRef

	// Adoption carries the in-place payload for ActionAdopt cluster plans:
	// member CRs, status prefills and the legacy objects to re-own or
	// dismantle. nil for every other kind/action.
	Adoption *AdoptionPlan

	// Warnings list legacy settings that do not carry over (dropped fields,
	// manual follow-ups like merging CA bundles into secrets).
	Warnings []string
	// Errors explain why Action == ActionError.
	Errors []string
	// Notes are informational (endpoint compatibility, auth follow-ups).
	Notes []string
}
