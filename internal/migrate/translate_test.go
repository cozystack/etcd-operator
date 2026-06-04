/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package migrate

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

func qty(t *testing.T, s string) resource.Quantity {
	t.Helper()
	q, err := resource.ParseQuantity(s)
	if err != nil {
		t.Fatalf("ParseQuantity(%q): %v", s, err)
	}
	return q
}

func ptrInt32(v int32) *int32 { return &v }

// hasWarning reports whether any warning contains the substring.
func hasWarning(warnings []string, substr string) bool {
	for _, w := range warnings {
		if strings.Contains(w, substr) {
			return true
		}
	}
	return false
}

func clusterTarget(t *testing.T, plan ResourcePlan) *lll.EtcdCluster {
	t.Helper()
	if plan.Action != ActionCreate {
		t.Fatalf("Action = %s (errors: %v), want Create", plan.Action, plan.Errors)
	}
	out, ok := plan.Target.(*lll.EtcdCluster)
	if !ok {
		t.Fatalf("Target is %T, want *EtcdCluster", plan.Target)
	}
	return out
}

// TestTranslateCluster_KitchenSink runs a fully-loaded legacy spec through
// the translator and pins every mapped field plus the exact set of dropped-
// field warnings.
func TestTranslateCluster_KitchenSink(t *testing.T) {
	sc := "fast-ssd"
	aff := &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{TopologyKey: "kubernetes.io/hostname"}},
	}}
	tsc := []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: "zone", WhenUnsatisfiable: corev1.DoNotSchedule}}
	res := corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: qty(t, "500m")}}

	spec := legacy.EtcdClusterSpec{
		Replicas: ptrInt32(5),
		Options: map[string]string{
			// The four cozystack keys map onto the typed spec.options…
			"quota-backend-bytes":       "10200547328",
			"auto-compaction-mode":      "periodic",
			"auto-compaction-retention": "5m",
			"snapshot-count":            "10000",
			// …anything else is dropped with a warning.
			"enable-v2": "false",
		},
		PodTemplate: legacy.PodTemplate{
			EmbeddedObjectMetadata: legacy.EmbeddedObjectMetadata{
				Labels:      map[string]string{"team": "infra"},
				Annotations: map[string]string{"note": "x"},
			},
			Spec: corev1.PodSpec{
				Affinity:                  aff,
				TopologySpreadConstraints: tsc,
				NodeSelector:              map[string]string{"disk": "ssd"},
				Containers: []corev1.Container{
					{Name: "etcd", Image: "quay.io/coreos/etcd:v3.5.21", Resources: res,
						Env: []corev1.EnvVar{{Name: "X", Value: "y"}}},
					{Name: "exporter", Image: "metrics:1"},
				},
			},
		},
		ServiceTemplate:             &legacy.EmbeddedService{},
		HeadlessServiceTemplate:     &legacy.EmbeddedMetadataResource{},
		PodDisruptionBudgetTemplate: &legacy.EmbeddedPodDisruptionBudget{},
		Storage: legacy.StorageSpec{
			VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: &sc,
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceStorage: qty(t, "10Gi")},
					},
				},
			},
		},
		Security: &legacy.SecuritySpec{
			EnableAuth: true,
			TLS: legacy.TLSSpec{
				ServerSecret:          "srv",
				ServerTrustedCASecret: "srv-ca",
				ClientSecret:          "op-client",
				ClientTrustedCASecret: "client-ca",
				PeerSecret:            "peer",
				PeerTrustedCASecret:   "peer-ca",
			},
		},
	}

	plan := TranslateCluster("my-etcd", "ns", spec, TranslateOptions{})
	out := clusterTarget(t, plan)

	if out.Spec.Replicas == nil || *out.Spec.Replicas != 5 {
		t.Errorf("replicas = %v, want 5", out.Spec.Replicas)
	}
	if out.Spec.Version != "3.5.21" {
		t.Errorf("version = %q, want 3.5.21", out.Spec.Version)
	}
	if out.Spec.Storage.Size.Cmp(qty(t, "10Gi")) != 0 || out.Spec.Storage.Medium != lll.StorageMediumDefault {
		t.Errorf("storage = %+v, want 10Gi PVC", out.Spec.Storage)
	}
	if out.Spec.Storage.StorageClassName == nil || *out.Spec.Storage.StorageClassName != sc {
		t.Errorf("storageClassName = %v, want %q", out.Spec.Storage.StorageClassName, sc)
	}
	if out.Spec.AdditionalMetadata == nil ||
		out.Spec.AdditionalMetadata.Labels["team"] != "infra" ||
		out.Spec.AdditionalMetadata.Annotations["note"] != "x" {
		t.Errorf("additionalMetadata = %+v", out.Spec.AdditionalMetadata)
	}
	if !equality.Semantic.DeepEqual(out.Spec.Affinity, aff) {
		t.Errorf("affinity not mapped: %+v", out.Spec.Affinity)
	}
	if !equality.Semantic.DeepEqual(out.Spec.TopologySpreadConstraints, tsc) {
		t.Errorf("topologySpreadConstraints not mapped: %+v", out.Spec.TopologySpreadConstraints)
	}
	if !equality.Semantic.DeepEqual(out.Spec.Resources, res) {
		t.Errorf("resources not mapped: %+v", out.Spec.Resources)
	}

	// TLS mapping.
	if out.Spec.TLS == nil || out.Spec.TLS.Client == nil ||
		out.Spec.TLS.Client.ServerSecretRef == nil || out.Spec.TLS.Client.ServerSecretRef.Name != "srv" {
		t.Fatalf("tls.client.serverSecretRef not mapped: %+v", out.Spec.TLS)
	}
	if out.Spec.TLS.Client.OperatorClientSecretRef == nil || out.Spec.TLS.Client.OperatorClientSecretRef.Name != "op-client" {
		t.Errorf("tls.client.operatorClientSecretRef = %+v, want op-client", out.Spec.TLS.Client.OperatorClientSecretRef)
	}
	if out.Spec.TLS.Peer == nil || out.Spec.TLS.Peer.SecretRef == nil || out.Spec.TLS.Peer.SecretRef.Name != "peer" {
		t.Errorf("tls.peer.secretRef = %+v, want peer", out.Spec.TLS.Peer)
	}

	// Auth: generated Secret referenced + emitted as an extra.
	if out.Spec.Auth == nil || !out.Spec.Auth.Enabled ||
		out.Spec.Auth.RootCredentialsSecretRef == nil ||
		out.Spec.Auth.RootCredentialsSecretRef.Name != "my-etcd-root-credentials" {
		t.Fatalf("auth = %+v", out.Spec.Auth)
	}
	if len(plan.Extras) != 1 {
		t.Fatalf("extras = %d, want 1 generated Secret", len(plan.Extras))
	}
	sec, ok := plan.Extras[0].(*corev1.Secret)
	if !ok || sec.Type != corev1.SecretTypeBasicAuth ||
		sec.StringData[corev1.BasicAuthUsernameKey] != "root" ||
		len(sec.StringData[corev1.BasicAuthPasswordKey]) < 16 {
		t.Fatalf("generated Secret malformed: %+v", plan.Extras[0])
	}

	// Typed options: the four cozystack keys map 1:1.
	if out.Spec.Options == nil ||
		out.Spec.Options.QuotaBackendBytes == nil || *out.Spec.Options.QuotaBackendBytes != 10200547328 ||
		out.Spec.Options.AutoCompactionMode != lll.AutoCompactionModePeriodic ||
		out.Spec.Options.AutoCompactionRetention != "5m" ||
		out.Spec.Options.SnapshotCount == nil || *out.Spec.Options.SnapshotCount != 10000 {
		t.Errorf("options not mapped: %+v", out.Spec.Options)
	}

	// Exact warning set: every dropped legacy knob accounted for.
	for _, want := range []string{
		`spec.options keys with no typed v1alpha2 equivalent; dropped etcd args: enable-v2="false"`,
		"spec.serviceTemplate",
		"spec.headlessServiceTemplate",
		"spec.podDisruptionBudgetTemplate",
		"containers[etcd].env",
		"containers[exporter] (sidecar)",
		"nodeSelector",
		`merge ca.crt from secret "srv-ca" into secret "srv"`,
		`secret "client-ca" (clientTrustedCASecret) is dropped`,
		`merge ca.crt from secret "peer-ca" into secret "peer"`,
	} {
		if !hasWarning(plan.Warnings, want) {
			t.Errorf("missing warning containing %q; got %v", want, plan.Warnings)
		}
	}
	if plan.DeleteRef == nil || plan.DeleteRef.GVR != ClusterGVR || plan.DeleteRef.Name != "my-etcd" {
		t.Errorf("DeleteRef = %+v", plan.DeleteRef)
	}
}

// TestTranslateCluster_VersionExtraction pins the image-tag → spec.version
// rules across default, override, and unparsable images.
func TestTranslateCluster_VersionExtraction(t *testing.T) {
	base := legacy.EtcdClusterSpec{
		Storage: legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty(t, "1Gi")}}},
		}},
	}

	t.Run("default image", func(t *testing.T) {
		plan := TranslateCluster("c", "ns", base, TranslateOptions{})
		out := clusterTarget(t, plan)
		if out.Spec.Version != "3.5.12" {
			t.Errorf("version = %q, want 3.5.12 (legacy default image)", out.Spec.Version)
		}
		if !hasWarning(plan.Warnings, "assuming the legacy default") {
			t.Errorf("expected default-image warning, got %v", plan.Warnings)
		}
	})

	t.Run("override wins", func(t *testing.T) {
		spec := base
		spec.PodTemplate.Spec.Containers = []corev1.Container{{Name: "etcd", Image: "etcd:v3.4.1"}}
		plan := TranslateCluster("c", "ns", spec, TranslateOptions{VersionOverride: "3.6.11"})
		if out := clusterTarget(t, plan); out.Spec.Version != "3.6.11" {
			t.Errorf("version = %q, want override 3.6.11", out.Spec.Version)
		}
	})

	t.Run("unparsable tag errors", func(t *testing.T) {
		spec := base
		spec.PodTemplate.Spec.Containers = []corev1.Container{{Name: "etcd", Image: "registry/etcd:latest"}}
		plan := TranslateCluster("c", "ns", spec, TranslateOptions{})
		if plan.Action != ActionError {
			t.Fatalf("Action = %s, want Error for unparsable tag", plan.Action)
		}
		if plan.DeleteRef != nil || plan.Target != nil {
			t.Errorf("errored plan must not delete/create anything: %+v", plan)
		}
	})

	t.Run("bad override errors", func(t *testing.T) {
		plan := TranslateCluster("c", "ns", base, TranslateOptions{VersionOverride: "v3.6.11"})
		if plan.Action != ActionError {
			t.Fatalf("Action = %s, want Error for malformed --version", plan.Action)
		}
	})
}

// TestTranslateStorage pins the storage union mapping.
func TestTranslateStorage(t *testing.T) {
	// In-place adoption hands the new operator the existing PVCs; emptyDir
	// clusters have none, so EVERY emptyDir variant must refuse loudly
	// rather than translate into something the adoption cannot back.
	t.Run("any emptyDir errors (nothing to adopt)", func(t *testing.T) {
		size := qty(t, "256Mi")
		for name, ed := range map[string]*corev1.EmptyDirVolumeSource{
			"memory with sizeLimit": {Medium: corev1.StorageMediumMemory, SizeLimit: &size},
			"memory bare":           {Medium: corev1.StorageMediumMemory},
			"disk with sizeLimit":   {SizeLimit: &size},
			"disk bare":             {},
		} {
			if _, _, err := translateStorage(legacy.StorageSpec{EmptyDir: ed}); err == nil {
				t.Errorf("%s: expected error — emptyDir has no PVC for in-place adoption", name)
			}
		}
	})

	t.Run("no storage at all errors (legacy defaulted to disk emptyDir)", func(t *testing.T) {
		_, _, err := translateStorage(legacy.StorageSpec{})
		if err == nil {
			t.Fatal("expected error for the implicit legacy emptyDir default")
		}
	})

	t.Run("vct without size defaults 1Gi with warning", func(t *testing.T) {
		sc := "std"
		got, warns, err := translateStorage(legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{StorageClassName: &sc},
		}})
		if err != nil {
			t.Fatalf("err=%v", err)
		}
		if got.Size.Cmp(qty(t, "1Gi")) != 0 || !hasWarning(warns, "defaulting spec.storage.size to 1Gi") {
			t.Errorf("got %+v warns=%v", got, warns)
		}
	})
}

// TestTranslateCluster_AuthRequiresClientTLS mirrors the v1alpha2 CEL rule:
// enableAuth without server TLS cannot be expressed in the new API.
func TestTranslateCluster_AuthRequiresClientTLS(t *testing.T) {
	spec := legacy.EtcdClusterSpec{
		Storage: legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty(t, "1Gi")}}},
		}},
		Security: &legacy.SecuritySpec{EnableAuth: true},
	}
	plan := TranslateCluster("c", "ns", spec, TranslateOptions{})
	if plan.Action != ActionError {
		t.Fatalf("Action = %s, want Error (auth requires client TLS)", plan.Action)
	}
}

// TestTranslateCluster_AuthSecretFlagSkipsGeneration: an explicit
// --auth-secret is referenced as-is, with no generated Secret extra.
func TestTranslateCluster_AuthSecretFlagSkipsGeneration(t *testing.T) {
	spec := legacy.EtcdClusterSpec{
		Storage: legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty(t, "1Gi")}}},
		}},
		Security: &legacy.SecuritySpec{
			EnableAuth: true,
			TLS:        legacy.TLSSpec{ServerSecret: "srv"},
		},
	}
	plan := TranslateCluster("c", "ns", spec, TranslateOptions{AuthSecretName: "my-creds"})
	out := clusterTarget(t, plan)
	if out.Spec.Auth.RootCredentialsSecretRef.Name != "my-creds" {
		t.Errorf("rootCredentialsSecretRef = %+v, want my-creds", out.Spec.Auth.RootCredentialsSecretRef)
	}
	if len(plan.Extras) != 0 {
		t.Errorf("no Secret should be generated when --auth-secret is given; extras=%v", plan.Extras)
	}
}

// TestTranslateCluster_AuthNotes pins the auth-disable note and the
// auth-off-window warning every auth-enabled adoption carries.
func TestTranslateCluster_AuthNotes(t *testing.T) {
	spec := legacy.EtcdClusterSpec{
		Storage: legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty(t, "1Gi")}}},
		}},
		Security: &legacy.SecuritySpec{
			EnableAuth: true,
			TLS:        legacy.TLSSpec{ServerSecret: "srv", ClientSecret: "op"},
		},
	}
	plan := TranslateCluster("c", "ns", spec, TranslateOptions{})
	clusterTarget(t, plan)
	if !hasWarning(plan.Warnings, "auth is OFF") {
		t.Errorf("expected auth-off-window warning, got %v", plan.Warnings)
	}
	found := false
	for _, n := range plan.Notes {
		if strings.Contains(n, "disables auth on the legacy etcd") {
			found = true
		}
	}
	if !found {
		t.Errorf("expected auth-disable note, got %v", plan.Notes)
	}
}

// TestTranslateCluster_RestoreDropped: an adopted cluster never bootstraps
// (status.clusterID is prefilled), so the legacy restore-at-creation config
// is dropped with a warning instead of carried into spec.bootstrap.
func TestTranslateCluster_RestoreDropped(t *testing.T) {
	spec := legacy.EtcdClusterSpec{
		Storage: legacy.StorageSpec{VolumeClaimTemplate: legacy.EmbeddedPersistentVolumeClaim{
			Spec: corev1.PersistentVolumeClaimSpec{Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: qty(t, "1Gi")}}},
		}},
		Bootstrap: &legacy.BootstrapSpec{Restore: &legacy.RestoreSpec{Source: legacy.BackupDestination{
			S3: &legacy.S3BackupDestination{Endpoint: "https://s3", Bucket: "b", Key: "snap/x.db",
				CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"}},
		}}},
	}
	plan := TranslateCluster("c", "ns", spec, TranslateOptions{})
	out := clusterTarget(t, plan)
	if out.Spec.Bootstrap != nil {
		t.Errorf("spec.bootstrap must not carry over to an adopted cluster: %+v", out.Spec.Bootstrap)
	}
	if !hasWarning(plan.Warnings, "spec.bootstrap.restore is dropped") {
		t.Errorf("expected restore-dropped warning, got %v", plan.Warnings)
	}
}

// TestTranslateBackup pins the field-for-field EtcdBackup → EtcdSnapshot map.
func TestTranslateBackup(t *testing.T) {
	plan := TranslateBackup("bk", "ns", legacy.EtcdBackupSpec{
		ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
		Destination: legacy.BackupDestination{S3: &legacy.S3BackupDestination{
			Endpoint: "https://minio", Bucket: "etcd", Key: "prefix",
			CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3"},
			Region:               "us-east-1", ForcePathStyle: true,
		}},
	})
	if plan.Action != ActionCreate {
		t.Fatalf("Action = %s (errors %v)", plan.Action, plan.Errors)
	}
	snap, ok := plan.Target.(*lll.EtcdSnapshot)
	if !ok {
		t.Fatalf("Target is %T", plan.Target)
	}
	if snap.Spec.ClusterRef.Name != "my-etcd" {
		t.Errorf("clusterRef = %+v", snap.Spec.ClusterRef)
	}
	s3 := snap.Spec.Destination.S3
	if s3 == nil || s3.Endpoint != "https://minio" || s3.Bucket != "etcd" || s3.Key != "prefix" ||
		s3.CredentialsSecretRef.Name != "s3" || s3.Region != "us-east-1" || !s3.ForcePathStyle {
		t.Errorf("destination not mapped: %+v", s3)
	}
	if plan.DeleteRef == nil || plan.DeleteRef.GVR != BackupGVR {
		t.Errorf("DeleteRef = %+v", plan.DeleteRef)
	}

	t.Run("malformed destination errors", func(t *testing.T) {
		p := TranslateBackup("bk", "ns", legacy.EtcdBackupSpec{ClusterRef: corev1.LocalObjectReference{Name: "c"}})
		if p.Action != ActionError {
			t.Fatalf("Action = %s, want Error for empty destination", p.Action)
		}
	})
}
