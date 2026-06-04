/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynfake "k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/controllers"
	"github.com/cozystack/etcd-operator/internal/migrate"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

func ptrInt32(v int32) *int32 { return &v }

func deployment(ns, name string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrInt32(replicas),
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
		},
	}
}

func controllerPod(ns, app string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: app + "-pod", Labels: map[string]string{"app": app}},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

// TestCheckControllersDown covers the safety gate's verdicts.
func TestCheckControllersDown(t *testing.T) {
	ctx := context.Background()
	ref := deployRef{Namespace: "sys", Name: "mgr"}

	t.Run("absent deployment is down", func(t *testing.T) {
		kube := k8sfake.NewClientset()
		if err := checkControllersDown(ctx, kube, []deployRef{ref}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("scaled to zero with no pods is down", func(t *testing.T) {
		kube := k8sfake.NewClientset(deployment("sys", "mgr", 0))
		if err := checkControllersDown(ctx, kube, []deployRef{ref}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("replicas above zero aborts", func(t *testing.T) {
		kube := k8sfake.NewClientset(deployment("sys", "mgr", 1))
		err := checkControllersDown(ctx, kube, []deployRef{ref})
		if err == nil || !strings.Contains(err.Error(), "not scaled down") {
			t.Fatalf("err = %v, want not-scaled-down", err)
		}
	})

	t.Run("lingering pod aborts even at replicas zero", func(t *testing.T) {
		kube := k8sfake.NewClientset(deployment("sys", "mgr", 0), controllerPod("sys", "mgr", corev1.PodRunning))
		err := checkControllersDown(ctx, kube, []deployRef{ref})
		if err == nil || !strings.Contains(err.Error(), "still has pod") {
			t.Fatalf("err = %v, want still-has-pod", err)
		}
	})

	t.Run("terminated pods are tolerated", func(t *testing.T) {
		kube := k8sfake.NewClientset(deployment("sys", "mgr", 0), controllerPod("sys", "mgr", corev1.PodSucceeded))
		if err := checkControllersDown(ctx, kube, []deployRef{ref}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("identical coordinates checked once", func(t *testing.T) {
		kube := k8sfake.NewClientset(deployment("sys", "mgr", 0))
		if err := checkControllersDown(ctx, kube, []deployRef{ref, ref}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		gets := 0
		for _, a := range kube.Actions() {
			if a.GetVerb() == "get" && a.GetResource().Resource == "deployments" {
				gets++
			}
		}
		if gets != 1 {
			t.Errorf("deployment fetched %d times, want 1 (dedup)", gets)
		}
	})
}

// legacyUnstructured builds a legacy CR as the dynamic client would return it.
func legacyUnstructured(kind, ns, name string, spec map[string]any) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "etcd.aenix.io/v1alpha1",
		"kind":       kind,
		"metadata":   map[string]any{"namespace": ns, "name": name, "uid": "uid-" + name},
		"spec":       spec,
	}}
}

func newDynFake(objs ...runtime.Object) *dynfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	return dynfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		migrate.ClusterGVR:  "EtcdClusterList",
		migrate.BackupGVR:   "EtcdBackupList",
		migrate.ScheduleGVR: "EtcdBackupScheduleList",
	}, objs...)
}

var clusterSpec = map[string]any{
	"replicas": int64(3),
	"storage": map[string]any{
		"volumeClaimTemplate": map[string]any{
			"spec": map[string]any{
				"resources": map[string]any{
					"requests": map[string]any{"storage": "1Gi"},
				},
			},
		},
	},
}

// TestDiscover covers the dynamic listing + trimmed-struct decode, including
// the uninstalled-CRD tolerance.
func TestDiscover(t *testing.T) {
	ctx := context.Background()

	t.Run("decodes all three kinds", func(t *testing.T) {
		dyn := newDynFake(
			legacyUnstructured("EtcdCluster", "ns1", "c1", clusterSpec),
			legacyUnstructured("EtcdBackup", "ns1", "b1", map[string]any{
				"clusterRef":  map[string]any{"name": "c1"},
				"destination": map[string]any{"pvc": map[string]any{"claimName": "claim"}},
			}),
			legacyUnstructured("EtcdBackupSchedule", "ns2", "s1", map[string]any{
				"clusterRef":  map[string]any{"name": "c2"},
				"schedule":    "@hourly",
				"destination": map[string]any{"pvc": map[string]any{"claimName": "claim"}},
			}),
		)
		d, err := discover(ctx, dyn, "")
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		if len(d.Clusters) != 1 || len(d.Backups) != 1 || len(d.Schedules) != 1 {
			t.Fatalf("discovered %d/%d/%d, want 1/1/1", len(d.Clusters), len(d.Backups), len(d.Schedules))
		}
		c := d.Clusters[0]
		if c.Name != "c1" || c.Namespace != "ns1" || c.UID != "uid-c1" {
			t.Errorf("cluster identity = %+v", c)
		}
		if c.Spec.Replicas == nil || *c.Spec.Replicas != 3 {
			t.Errorf("decoded replicas = %v", c.Spec.Replicas)
		}
		if got := c.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "1Gi" {
			t.Errorf("decoded storage request = %s", got.String())
		}
		if d.Backups[0].Spec.Destination.PVC == nil || d.Backups[0].Spec.Destination.PVC.ClaimName != "claim" {
			t.Errorf("decoded backup destination = %+v", d.Backups[0].Spec.Destination)
		}
		if d.Schedules[0].Spec.Schedule != "@hourly" {
			t.Errorf("decoded schedule = %q", d.Schedules[0].Spec.Schedule)
		}
	})

	t.Run("namespace filter applies", func(t *testing.T) {
		dyn := newDynFake(
			legacyUnstructured("EtcdCluster", "ns1", "c1", clusterSpec),
			legacyUnstructured("EtcdCluster", "ns2", "c2", clusterSpec),
		)
		d, err := discover(ctx, dyn, "ns2")
		if err != nil {
			t.Fatalf("discover: %v", err)
		}
		if len(d.Clusters) != 1 || d.Clusters[0].Name != "c2" {
			t.Fatalf("clusters = %+v, want only ns2/c2", d.Clusters)
		}
	})
}

func newCtrlFake(t *testing.T, objs ...runtime.Object) *ctrlfake.ClientBuilder {
	t.Helper()
	scheme, err := newScheme()
	if err != nil {
		t.Fatalf("newScheme: %v", err)
	}
	return ctrlfake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&lll.EtcdCluster{}, &lll.EtcdMember{}).
		WithRuntimeObjects(objs...)
}

// factsFixture is what inspectCluster would report for a healthy 3-member
// legacy cluster named c1 in ns.
func factsFixture() migrate.ClusterFacts {
	f := migrate.ClusterFacts{ClusterIDHex: "00000000deadbeef"}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("c1-%d", i)
		f.Members = append(f.Members, migrate.MemberFact{
			Name:    name,
			IDHex:   fmt.Sprintf("%016x", 0xa00+i),
			PeerURL: fmt.Sprintf("https://%s.c1-headless.ns.svc:2380", name),
			PodUID:  "uid-" + name,
		})
	}
	return f
}

// dataPlaneFixture is the legacy data plane the adoption re-owns: STS, pods,
// PVCs, Services, state ConfigMap.
func dataPlaneFixture() []runtime.Object {
	objs := []runtime.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c1"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c1-cluster-state"}},
		// The legacy headless Service starts controller-owned by the legacy
		// EtcdCluster — the migration must STRIP this stale ref when it
		// re-points ownership at the adopted members.
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c1-headless",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "etcd.aenix.io/v1alpha1", Kind: "EtcdCluster", Name: "c1",
				UID: types.UID("legacy-uid"), Controller: ptrTo(true), BlockOwnerDeletion: ptrTo(true),
			}}}},
		// The legacy client Service is a real ClusterIP (the collision case);
		// the cutover deletes it and recreates "c1" as a headless Service.
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c1"},
			Spec: corev1.ServiceSpec{ClusterIP: "10.0.0.10"}},
	}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("c1-%d", i)
		objs = append(objs,
			&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name, UID: types.UID("uid-" + name),
					Labels: map[string]string{"app.kubernetes.io/name": "etcd", "app.kubernetes.io/instance": "c1"}},
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			},
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "data-" + name}},
		)
	}
	return objs
}

// TestMarkExisting: a pre-existing adoption target keeps Action=Adopt (every
// adoption step is idempotent and a partial run must be completed, not
// skipped) with an explanatory note; a pre-existing Create target (backup)
// still downgrades to Skip.
func TestMarkExisting(t *testing.T) {
	ctx := context.Background()
	existingCluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "c1"}}
	existingSnap := &lll.EtcdSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "b1"}}
	c := newCtrlFake(t, existingCluster, existingSnap).Build()

	d := discovered{
		Clusters: []legacyCluster{{Name: "c1", Namespace: "ns", Spec: legacySpecFixture()}},
		Backups: []legacyBackup{{Name: "b1", Namespace: "ns", Spec: legacy.EtcdBackupSpec{
			ClusterRef:  corev1.LocalObjectReference{Name: "c1"},
			Destination: legacy.BackupDestination{PVC: &legacy.PVCBackupDestination{ClaimName: "claim"}},
		}}},
	}
	facts := map[string]migrate.ClusterFacts{"ns/c1": factsFixture()}
	plans := buildPlans(d, facts, nil, migrate.TranslateOptions{})

	if err := markExisting(ctx, c, plans); err != nil {
		t.Fatalf("markExisting: %v", err)
	}
	if plans[0].Action != migrate.ActionAdopt {
		t.Errorf("adoption target exists: Action = %s, want Adopt (idempotent re-run)", plans[0].Action)
	}
	noteFound := false
	for _, n := range plans[0].Notes {
		if strings.Contains(n, "re-run idempotently") {
			noteFound = true
		}
	}
	if !noteFound {
		t.Errorf("expected idempotent-re-run note, got %v", plans[0].Notes)
	}
	if plans[1].Action != migrate.ActionSkip {
		t.Errorf("existing snapshot target: Action = %s, want Skip", plans[1].Action)
	}
}

// TestBuildPlans_InspectFailure: a cluster whose live inspection failed gets
// an error plan and is never adopted.
func TestBuildPlans_InspectFailure(t *testing.T) {
	d := discovered{Clusters: []legacyCluster{{Name: "c1", Namespace: "ns", Spec: legacySpecFixture()}}}
	plans := buildPlans(d, nil, map[string]error{"ns/c1": fmt.Errorf("no Running etcd pod")}, migrate.TranslateOptions{})
	if len(plans) != 1 || plans[0].Action != migrate.ActionError {
		t.Fatalf("plans = %+v, want one error plan", plans)
	}
	if !strings.Contains(plans[0].Errors[0], "no Running etcd pod") {
		t.Errorf("error should carry the inspection failure: %v", plans[0].Errors)
	}
}

// TestApplyAdoption walks the full in-place adoption against fake clients:
// legacy control plane dismantled (CR, STS, ConfigMap — pods survive), new
// CRs created with prefilled status, pods/PVCs re-owned and labeled,
// Services re-owned. Then runs the apply a second time to pin idempotency.
func TestApplyAdoption(t *testing.T) {
	ctx := context.Background()
	dyn := newDynFake(legacyUnstructured("EtcdCluster", "ns", "c1", clusterSpec))
	c := newCtrlFake(t, dataPlaneFixture()...).Build()

	plan := migrate.BuildAdoption("c1", "ns", legacySpecFixture(), factsFixture(), migrate.TranslateOptions{})
	if plan.Action != migrate.ActionAdopt {
		t.Fatalf("Action = %s (errors %v)", plan.Action, plan.Errors)
	}
	plans := []migrate.ResourcePlan{plan,
		{ // an errored plan must be inert
			SourceKind: "EtcdCluster", SourceName: "broken", Namespace: "ns",
			Action: migrate.ActionError, Errors: []string{"x"},
		},
	}

	stats, err := applyPlans(ctx, c, dyn, plans, io.Discard)
	if err != nil {
		t.Fatalf("applyPlans: %v", err)
	}
	if stats.Adopted != 1 || stats.Errored != 1 {
		t.Fatalf("stats = %+v", stats)
	}

	// Legacy CR gone, STS gone, state ConfigMap gone — but every pod alive.
	if _, err := dyn.Resource(migrate.ClusterGVR).Namespace("ns").Get(ctx, "c1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("legacy CR still present (err=%v)", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "c1"}, &appsv1.StatefulSet{}); !apierrors.IsNotFound(err) {
		t.Errorf("legacy StatefulSet still present (err=%v)", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "c1-cluster-state"}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Errorf("legacy ConfigMap still present (err=%v)", err)
	}

	// New cluster: prefilled status (bootstrap gate). No headless override on
	// the spec anymore — it lives as an annotation on the adopted members.
	cluster := &lll.EtcdCluster{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "c1"}, cluster); err != nil {
		t.Fatalf("new EtcdCluster missing: %v", err)
	}
	if cluster.Status.ClusterID != "00000000deadbeef" || cluster.Status.ClusterToken != "c1-ns" || cluster.Status.Observed == nil {
		t.Errorf("cluster status not prefilled: %+v", cluster.Status)
	}
	if cluster.Status.Observed != nil && cluster.Status.Observed.Replicas != 3 {
		t.Errorf("observed.replicas = %d, want live member count 3", cluster.Status.Observed.Replicas)
	}

	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("c1-%d", i)

		member := &lll.EtcdMember{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: name}, member); err != nil {
			t.Fatalf("EtcdMember %s missing: %v", name, err)
		}
		if member.Annotations[controllers.AnnDataDirSubPath] != migrate.LegacyDataDirSubPath {
			t.Errorf("%s data-dir-subpath annotation = %q, want %q", name, member.Annotations[controllers.AnnDataDirSubPath], migrate.LegacyDataDirSubPath)
		}
		if member.Annotations[controllers.AnnHeadlessServiceName] != "c1-headless" {
			t.Errorf("%s headless-service-name annotation = %q", name, member.Annotations[controllers.AnnHeadlessServiceName])
		}
		if !strings.Contains(member.Spec.InitialCluster, name+"=https://"+name+".c1-headless.ns.svc:2380") {
			t.Errorf("%s initialCluster = %q, want the persisted peer URLs", name, member.Spec.InitialCluster)
		}
		if member.Status.MemberID == "" || !member.Status.IsVoter || member.Status.PodUID != "uid-"+name {
			t.Errorf("%s status not prefilled: %+v", name, member.Status)
		}

		pod := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: name}, pod); err != nil {
			t.Fatalf("pod %s gone — adoption must never delete pods: %v", name, err)
		}
		assertControllerOwner(t, pod.OwnerReferences, "EtcdMember", name)
		if pod.Labels["etcd-operator.cozystack.io/cluster"] != "c1" || pod.Labels["etcd-operator.cozystack.io/role"] != "voter" {
			t.Errorf("pod %s labels not stamped: %v", name, pod.Labels)
		}
		if pod.Labels["app.kubernetes.io/instance"] != "c1" {
			t.Errorf("pod %s legacy labels must survive (old Services select them): %v", name, pod.Labels)
		}

		pvc := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "data-" + name}, pvc); err != nil {
			t.Fatalf("PVC data-%s gone: %v", name, err)
		}
		assertControllerOwner(t, pvc.OwnerReferences, "EtcdMember", name)
	}

	// Legacy headless Service is owner-referenced to the 3 adopted members
	// (non-controller refs) so it self-GCs as they roll — NOT controller-owned
	// by the cluster.
	legacyHeadless := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "c1-headless"}, legacyHeadless); err != nil {
		t.Fatalf("legacy headless Service gone: %v", err)
	}
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("c1-%d", i)
		assertMemberOwnerRef(t, legacyHeadless.OwnerReferences, name)
	}
	for _, o := range legacyHeadless.OwnerReferences {
		if o.Controller != nil && *o.Controller {
			t.Errorf("legacy headless Service must carry no controller owner; got %+v", o)
		}
		if o.Kind != "EtcdMember" {
			t.Errorf("legacy headless Service owner refs must all be EtcdMember; got %q", o.Kind)
		}
	}

	// The legacy client Service "c1" has been replaced in place by the
	// operator's native headless Service of the same name (clusterIP None),
	// controller-owned by the new EtcdCluster.
	nativeHeadless := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "c1"}, nativeHeadless); err != nil {
		t.Fatalf("native headless Service c1 gone: %v", err)
	}
	if nativeHeadless.Spec.ClusterIP != corev1.ClusterIPNone {
		t.Errorf("Service c1 must be headless after cutover; ClusterIP=%q", nativeHeadless.Spec.ClusterIP)
	}
	assertControllerOwner(t, nativeHeadless.OwnerReferences, "EtcdCluster", "c1")

	// Second pass: every step must tolerate the already-adopted state.
	plans2 := []migrate.ResourcePlan{migrate.BuildAdoption("c1", "ns", legacySpecFixture(), factsFixture(), migrate.TranslateOptions{})}
	if _, err := applyPlans(ctx, c, dyn, plans2, io.Discard); err != nil {
		t.Fatalf("second applyPlans must be idempotent: %v", err)
	}
}

// TestRunMutationPhases_AuthDisableBeforeBackup pins the inter-phase contract
// that the snapshot Job depends on: auth-disable MUST run before the backup,
// because the Job dials etcd anonymously and etcd gates the Maintenance
// Snapshot RPC behind auth. The historical bug ran backup first, which made
// the safety backup fail for exactly the auth-enabled clusters the tool
// targets. This test fails if the order regresses.
func TestRunMutationPhases_AuthDisableBeforeBackup(t *testing.T) {
	var order []string
	authDisable := func() error { order = append(order, "auth"); return nil }
	backup := func() error { order = append(order, "backup"); return nil }
	apply := func() (applyStats, error) { order = append(order, "apply"); return applyStats{Adopted: 1}, nil }

	stats, err := runMutationPhases(authDisable, backup, apply)
	if err != nil {
		t.Fatalf("runMutationPhases: %v", err)
	}
	if stats.Adopted != 1 {
		t.Errorf("stats not propagated from apply: %+v", stats)
	}
	want := []string{"auth", "backup", "apply"}
	if len(order) != len(want) {
		t.Fatalf("phase order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("phase order = %v, want %v (auth-disable must precede backup)", order, want)
		}
	}
}

// TestRunMutationPhases_AuthFailureSkipsBackupAndApply: when auth-disable
// errors, neither backup nor apply runs and the error propagates — an
// unprotected, still-auth'd cluster must never reach the mutation phases.
func TestRunMutationPhases_AuthFailureSkipsBackupAndApply(t *testing.T) {
	wantErr := fmt.Errorf("auth boom")
	var ran []string
	_, err := runMutationPhases(
		func() error { ran = append(ran, "auth"); return wantErr },
		func() error { ran = append(ran, "backup"); return nil },
		func() (applyStats, error) { ran = append(ran, "apply"); return applyStats{}, nil },
	)
	if err == nil {
		t.Fatal("expected auth-disable error to propagate")
	}
	if len(ran) != 1 || ran[0] != "auth" {
		t.Errorf("after auth-disable failure, ran = %v; want only [auth]", ran)
	}
}

// TestRunMutationPhases_NilBackupSkips: --skip-backup (nil backup fn) still
// runs auth-disable then apply, in that order.
func TestRunMutationPhases_NilBackupSkips(t *testing.T) {
	var order []string
	_, err := runMutationPhases(
		func() error { order = append(order, "auth"); return nil },
		nil,
		func() (applyStats, error) { order = append(order, "apply"); return applyStats{}, nil },
	)
	if err != nil {
		t.Fatalf("runMutationPhases: %v", err)
	}
	want := []string{"auth", "apply"}
	if len(order) != len(want) || order[0] != want[0] || order[1] != want[1] {
		t.Errorf("phase order with nil backup = %v, want %v", order, want)
	}
}

func assertControllerOwner(t *testing.T, refs []metav1.OwnerReference, kind, name string) {
	t.Helper()
	for _, o := range refs {
		if o.Kind == kind && o.Name == name && o.Controller != nil && *o.Controller {
			return
		}
	}
	t.Errorf("no controller ownerRef %s/%s in %+v", kind, name, refs)
}

// assertMemberOwnerRef checks for a non-controller, non-blocking EtcdMember
// owner reference — the shape the migration tool stamps on the legacy
// headless Service so it self-GCs once the last adopted member rolls away.
func assertMemberOwnerRef(t *testing.T, refs []metav1.OwnerReference, name string) {
	t.Helper()
	for _, o := range refs {
		if o.Kind == "EtcdMember" && o.Name == name {
			if o.Controller != nil && *o.Controller {
				t.Errorf("EtcdMember owner ref %q must not be a controller ref", name)
			}
			if o.BlockOwnerDeletion != nil && *o.BlockOwnerDeletion {
				t.Errorf("EtcdMember owner ref %q must not block owner deletion", name)
			}
			return
		}
	}
	t.Errorf("no EtcdMember ownerRef %q in %+v", name, refs)
}

// TestApplyPlans_BackupCreate: the EtcdBackup → EtcdSnapshot path still
// creates the new CR and deletes the legacy source.
func TestApplyPlans_BackupCreate(t *testing.T) {
	ctx := context.Background()
	dyn := newDynFake(legacyUnstructured("EtcdBackup", "ns", "b1", map[string]any{
		"clusterRef":  map[string]any{"name": "c1"},
		"destination": map[string]any{"pvc": map[string]any{"claimName": "claim"}},
	}))
	c := newCtrlFake(t).Build()

	plans := []migrate.ResourcePlan{migrate.TranslateBackup("b1", "ns", legacy.EtcdBackupSpec{
		ClusterRef:  corev1.LocalObjectReference{Name: "c1"},
		Destination: legacy.BackupDestination{PVC: &legacy.PVCBackupDestination{ClaimName: "claim"}},
	})}
	stats, err := applyPlans(ctx, c, dyn, plans, io.Discard)
	if err != nil {
		t.Fatalf("applyPlans: %v", err)
	}
	if stats.Created != 1 || stats.Deleted != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "b1"}, &lll.EtcdSnapshot{}); err != nil {
		t.Errorf("new EtcdSnapshot missing: %v", err)
	}
	if _, err := dyn.Resource(migrate.BackupGVR).Namespace("ns").Get(ctx, "b1", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Errorf("legacy EtcdBackup still present (err=%v)", err)
	}
}

// TestApplyPlans_SkipStillCleansUp: a Skip (target pre-existing) still
// deletes the leftover legacy CR.
func TestApplyPlans_SkipStillCleansUp(t *testing.T) {
	ctx := context.Background()
	dyn := newDynFake(legacyUnstructured("EtcdBackup", "ns", "b1", map[string]any{
		"clusterRef":  map[string]any{"name": "c1"},
		"destination": map[string]any{"pvc": map[string]any{"claimName": "claim"}},
	}))
	existing := &lll.EtcdSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "b1"}}
	c := newCtrlFake(t, existing).Build()

	plans := []migrate.ResourcePlan{migrate.TranslateBackup("b1", "ns", legacy.EtcdBackupSpec{
		ClusterRef:  corev1.LocalObjectReference{Name: "c1"},
		Destination: legacy.BackupDestination{PVC: &legacy.PVCBackupDestination{ClaimName: "claim"}},
	})}
	if err := markExisting(ctx, c, plans); err != nil {
		t.Fatalf("markExisting: %v", err)
	}
	stats, err := applyPlans(ctx, c, dyn, plans, io.Discard)
	if err != nil {
		t.Fatalf("applyPlans: %v", err)
	}
	if stats.Skipped != 1 || stats.Deleted != 1 {
		t.Fatalf("stats = %+v, want skipped=1 deleted=1", stats)
	}
	_, err = dyn.Resource(migrate.BackupGVR).Namespace("ns").Get(ctx, "b1", metav1.GetOptions{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("legacy CR still present (err=%v)", err)
	}
}

// legacySpecFixture is a minimal migratable legacy cluster spec.
func legacySpecFixture() legacy.EtcdClusterSpec {
	return legacy.EtcdClusterSpec{
		Replicas: ptrInt32(3),
		Storage: legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			}},
		}},
	}
}
