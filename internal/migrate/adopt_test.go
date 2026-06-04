/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package migrate

import (
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/controllers"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

func adoptSpecFixture(t *testing.T) legacy.EtcdClusterSpec {
	t.Helper()
	three := int32(3)
	return legacy.EtcdClusterSpec{
		Replicas: &three,
		Storage: legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty(t, "10Gi")}}},
		}},
	}
}

func adoptFactsFixture(n int) ClusterFacts {
	f := ClusterFacts{ClusterIDHex: "00000000deadbeef"}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("etcd-%d", i)
		f.Members = append(f.Members, MemberFact{
			Name:    name,
			IDHex:   fmt.Sprintf("%016x", 0xa00+i),
			PeerURL: fmt.Sprintf("https://%s.etcd-headless.ns.svc:2380", name),
			PodUID:  "uid-" + name,
		})
	}
	return f
}

// TestBuildAdoption_HappyPath pins the adoption contract end to end: the
// headless override matching the pods' immutable DNS, the member CRs
// mirroring the live pods (data-dir subPath, persisted-URL initialCluster,
// legacy token), and the status prefills that keep bootstrap from firing.
func TestBuildAdoption_HappyPath(t *testing.T) {
	plan := BuildAdoption("etcd", "ns", adoptSpecFixture(t), adoptFactsFixture(3), TranslateOptions{})
	if plan.Action != ActionAdopt {
		t.Fatalf("Action = %s (errors %v)", plan.Action, plan.Errors)
	}
	cluster := plan.Target.(*lll.EtcdCluster)
	if cluster.Spec.Replicas == nil || *cluster.Spec.Replicas != 3 {
		t.Errorf("replicas = %v, want live count 3", cluster.Spec.Replicas)
	}

	a := plan.Adoption
	if a == nil {
		t.Fatal("Adoption payload missing")
	}
	if a.ClusterStatus.ClusterID != "00000000deadbeef" || a.ClusterStatus.ClusterToken != "etcd-ns" {
		t.Errorf("cluster status prefill = %+v", a.ClusterStatus)
	}
	if a.ClusterStatus.Observed == nil || a.ClusterStatus.Observed.Replicas != 3 {
		t.Errorf("observed prefill = %+v", a.ClusterStatus.Observed)
	}
	if a.StatefulSetName != "etcd" || a.ConfigMapName != "etcd-cluster-state" || a.PDBName != "etcd" {
		t.Errorf("legacy object names = %q/%q/%q", a.StatefulSetName, a.ConfigMapName, a.PDBName)
	}
	if a.HeadlessServiceName != "etcd-headless" || a.ClientServiceName != "etcd" {
		t.Errorf("service names = %q/%q", a.HeadlessServiceName, a.ClientServiceName)
	}

	if len(a.Members) != 3 {
		t.Fatalf("members = %d, want 3", len(a.Members))
	}
	wantInitial := "etcd-0=https://etcd-0.etcd-headless.ns.svc:2380," +
		"etcd-1=https://etcd-1.etcd-headless.ns.svc:2380," +
		"etcd-2=https://etcd-2.etcd-headless.ns.svc:2380"
	for i, ma := range a.Members {
		name := fmt.Sprintf("etcd-%d", i)
		if ma.Member.Name != name {
			t.Errorf("member[%d] name = %q (must equal the pod name)", i, ma.Member.Name)
		}
		if ma.Member.Annotations[controllers.AnnDataDirSubPath] != LegacyDataDirSubPath {
			t.Errorf("%s data-dir-subpath annotation = %q, want %q", name, ma.Member.Annotations[controllers.AnnDataDirSubPath], LegacyDataDirSubPath)
		}
		if ma.Member.Annotations[controllers.AnnHeadlessServiceName] != "etcd-headless" {
			t.Errorf("%s headless-service-name annotation = %q, want legacy convention", name, ma.Member.Annotations[controllers.AnnHeadlessServiceName])
		}
		if ma.Member.Spec.InitialCluster != wantInitial {
			t.Errorf("%s initialCluster = %q\nwant %q", name, ma.Member.Spec.InitialCluster, wantInitial)
		}
		if ma.Member.Spec.ClusterToken != "etcd-ns" {
			t.Errorf("%s clusterToken = %q, want the legacy token", name, ma.Member.Spec.ClusterToken)
		}
		if ma.Member.Spec.Bootstrap {
			t.Errorf("%s must not be a bootstrap seed", name)
		}
		if !ma.Status.IsVoter || ma.Status.MemberID == "" || ma.Status.PodUID != "uid-"+name {
			t.Errorf("%s status prefill = %+v", name, ma.Status)
		}
		if ma.PVCName != "data-"+name {
			t.Errorf("%s pvc = %q", name, ma.PVCName)
		}
	}
}

// TestBuildAdoption_MirrorsTLSOntoMembers: the member-side TLS view must
// match what the controller's own deriveMemberTLS would produce, or the
// first replacement pod comes up plaintext against a TLS cluster.
func TestBuildAdoption_MirrorsTLSOntoMembers(t *testing.T) {
	spec := adoptSpecFixture(t)
	spec.Security = &legacy.SecuritySpec{TLS: legacy.TLSSpec{
		ServerSecret: "srv", ClientSecret: "op-client", PeerSecret: "peer",
	}}
	plan := BuildAdoption("etcd", "ns", spec, adoptFactsFixture(1), TranslateOptions{})
	if plan.Action != ActionAdopt {
		t.Fatalf("Action = %s (errors %v)", plan.Action, plan.Errors)
	}
	mtls := plan.Adoption.Members[0].Member.Spec.TLS
	if mtls == nil || mtls.ClientServerSecretRef == nil || mtls.ClientServerSecretRef.Name != "srv" {
		t.Fatalf("member TLS = %+v, want server secret mirrored", mtls)
	}
	if !mtls.ClientMTLS {
		t.Error("operator client secret set ⇒ member must demand client certs (ClientMTLS)")
	}
	if mtls.PeerSecretRef == nil || mtls.PeerSecretRef.Name != "peer" {
		t.Errorf("peer secret not mirrored: %+v", mtls.PeerSecretRef)
	}
}

// TestBuildAdoption_Refusals pins the states the tool must not touch.
func TestBuildAdoption_Refusals(t *testing.T) {
	t.Run("learner member", func(t *testing.T) {
		facts := adoptFactsFixture(2)
		facts.Members[1].IsLearner = true
		plan := BuildAdoption("etcd", "ns", adoptSpecFixture(t), facts, TranslateOptions{})
		if plan.Action != ActionError {
			t.Fatalf("Action = %s, want Error for a learner", plan.Action)
		}
	})

	t.Run("member without running pod", func(t *testing.T) {
		facts := adoptFactsFixture(2)
		facts.Members[0].PodUID = ""
		plan := BuildAdoption("etcd", "ns", adoptSpecFixture(t), facts, TranslateOptions{})
		if plan.Action != ActionError {
			t.Fatalf("Action = %s, want Error for a podless member", plan.Action)
		}
	})

	t.Run("emptyDir storage", func(t *testing.T) {
		spec := adoptSpecFixture(t)
		spec.Storage = legacy.StorageSpec{EmptyDir: &corev1.EmptyDirVolumeSource{}}
		plan := BuildAdoption("etcd", "ns", spec, adoptFactsFixture(1), TranslateOptions{})
		if plan.Action != ActionError {
			t.Fatalf("Action = %s, want Error for emptyDir", plan.Action)
		}
	})
}

// TestBuildAdoption_ReplicasFollowLiveState: a legacy spec disagreeing with
// the live member count is adopted at the LIVE count, with a warning.
func TestBuildAdoption_ReplicasFollowLiveState(t *testing.T) {
	spec := adoptSpecFixture(t) // says replicas=3
	plan := BuildAdoption("etcd", "ns", spec, adoptFactsFixture(2), TranslateOptions{})
	if plan.Action != ActionAdopt {
		t.Fatalf("Action = %s (errors %v)", plan.Action, plan.Errors)
	}
	cluster := plan.Target.(*lll.EtcdCluster)
	if cluster.Spec.Replicas == nil || *cluster.Spec.Replicas != 2 {
		t.Errorf("replicas = %v, want live count 2", cluster.Spec.Replicas)
	}
	found := false
	for _, w := range plan.Warnings {
		if strings.Contains(w, "disagrees with the live member count") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected replicas-mismatch warning, got %v", plan.Warnings)
	}
}

// TestBuildAdoption_HeadlessServiceTemplateOverride: a legacy cluster that
// renamed its headless Service via the template keeps that exact name.
func TestBuildAdoption_HeadlessServiceTemplateOverride(t *testing.T) {
	spec := adoptSpecFixture(t)
	spec.HeadlessServiceTemplate = &legacy.EmbeddedMetadataResource{
		EmbeddedObjectMetadata: legacy.EmbeddedObjectMetadata{Name: "custom-peers"},
	}
	plan := BuildAdoption("etcd", "ns", spec, adoptFactsFixture(1), TranslateOptions{})
	if plan.Action != ActionAdopt {
		t.Fatalf("Action = %s (errors %v)", plan.Action, plan.Errors)
	}
	if got := plan.Adoption.Members[0].Member.Annotations[controllers.AnnHeadlessServiceName]; got != "custom-peers" {
		t.Errorf("member headless-service-name annotation = %q, want the template override", got)
	}
	if plan.Adoption.HeadlessServiceName != "custom-peers" {
		t.Errorf("legacy headless Service to GC = %q, want the template override", plan.Adoption.HeadlessServiceName)
	}
}
