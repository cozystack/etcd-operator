/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package legacy holds trimmed copies of the legacy etcd.aenix.io/v1alpha1
// spec types, as defined on this repository's `main` branch. The migration
// tool decodes legacy CRs (fetched as unstructured) into these structs with
// runtime.DefaultUnstructuredConverter — there is no scheme registration and
// no deepcopy generation, because the legacy API is consumed read-only and
// never written back. Status types are intentionally omitted: the legacy
// status carries only conditions, none of which inform the translation.
//
// Keep the json tags byte-for-byte identical to the originals
// (main:api/v1alpha1/*_types.go); the converter matches on them.
package legacy

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// GroupVersion identifiers for the legacy API, used by the discovery layer.
const (
	Group   = "etcd.aenix.io"
	Version = "v1alpha1"
)

// DefaultEtcdImage is the image the legacy operator ran when podTemplate did
// not override it; its tag is the version-extraction fallback.
const DefaultEtcdImage = "quay.io/coreos/etcd:v3.5.12"

// EtcdClusterSpec mirrors the legacy EtcdCluster spec.
type EtcdClusterSpec struct {
	Replicas                    *int32                       `json:"replicas,omitempty"`
	Options                     map[string]string            `json:"options,omitempty"`
	PodTemplate                 PodTemplate                  `json:"podTemplate,omitempty"`
	ServiceTemplate             *EmbeddedService             `json:"serviceTemplate,omitempty"`
	HeadlessServiceTemplate     *EmbeddedMetadataResource    `json:"headlessServiceTemplate,omitempty"`
	PodDisruptionBudgetTemplate *EmbeddedPodDisruptionBudget `json:"podDisruptionBudgetTemplate,omitempty"`
	Storage                     StorageSpec                  `json:"storage"`
	Security                    *SecuritySpec                `json:"security,omitempty"`
	Bootstrap                   *BootstrapSpec               `json:"bootstrap,omitempty"`
}

// BootstrapSpec mirrors the legacy restore-at-creation config.
type BootstrapSpec struct {
	Restore *RestoreSpec `json:"restore,omitempty"`
}

// RestoreSpec mirrors the legacy restore source.
type RestoreSpec struct {
	Source BackupDestination `json:"source"`
}

// EmbeddedObjectMetadata mirrors the legacy embedded metadata subset.
type EmbeddedObjectMetadata struct {
	Name        string            `json:"name,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// PodTemplate mirrors the legacy pod-template override.
type PodTemplate struct {
	EmbeddedObjectMetadata `json:"metadata,omitempty"`
	Spec                   corev1.PodSpec `json:"spec,omitempty"`
}

// StorageSpec mirrors the legacy storage config: emptyDir takes precedence
// over volumeClaimTemplate when both are set.
type StorageSpec struct {
	EmptyDir            *corev1.EmptyDirVolumeSource  `json:"emptyDir,omitempty"`
	VolumeClaimTemplate EmbeddedPersistentVolumeClaim `json:"volumeClaimTemplate,omitempty"`
}

// SecuritySpec mirrors the legacy security config.
type SecuritySpec struct {
	TLS        TLSSpec `json:"tls,omitempty"`
	EnableAuth bool    `json:"enableAuth,omitempty"`
}

// TLSSpec mirrors the legacy six-secret TLS layout. All fields are secret
// names in the cluster's namespace.
type TLSSpec struct {
	PeerTrustedCASecret   string `json:"peerTrustedCASecret,omitempty"`
	PeerSecret            string `json:"peerSecret,omitempty"`
	ServerTrustedCASecret string `json:"serverTrustedCASecret,omitempty"`
	ServerSecret          string `json:"serverSecret,omitempty"`
	ClientTrustedCASecret string `json:"clientTrustedCASecret,omitempty"`
	ClientSecret          string `json:"clientSecret,omitempty"`
}

// EmbeddedPersistentVolumeClaim mirrors the legacy embedded PVC template.
// (Status is dropped: read-only and irrelevant to translation.)
type EmbeddedPersistentVolumeClaim struct {
	metav1.TypeMeta        `json:",inline"`
	EmbeddedObjectMetadata `json:"metadata,omitempty"`
	Spec                   corev1.PersistentVolumeClaimSpec `json:"spec,omitempty"`
}

// EmbeddedPodDisruptionBudget mirrors the legacy PDB template. The inner
// spec is irrelevant to translation (the new operator owns the PDB), so it
// is kept opaque — its mere presence triggers a warning.
type EmbeddedPodDisruptionBudget struct {
	EmbeddedObjectMetadata `json:"metadata,omitempty"`
	Spec                   PodDisruptionBudgetSpec `json:"spec"`
}

// PodDisruptionBudgetSpec mirrors the legacy PDB knobs. Translation only
// reports the template's presence, never these values.
type PodDisruptionBudgetSpec struct {
	MinAvailable   *intstr.IntOrString `json:"minAvailable,omitempty"`
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

// EmbeddedService mirrors the legacy client-service template.
type EmbeddedService struct {
	EmbeddedObjectMetadata `json:"metadata,omitempty"`
	Spec                   corev1.ServiceSpec `json:"spec,omitempty"`
}

// EmbeddedMetadataResource mirrors the legacy headless-service template.
type EmbeddedMetadataResource struct {
	EmbeddedObjectMetadata `json:"metadata,omitempty"`
}

// EtcdBackupSpec mirrors the legacy one-shot backup spec.
type EtcdBackupSpec struct {
	ClusterRef  corev1.LocalObjectReference `json:"clusterRef"`
	Destination BackupDestination           `json:"destination"`
}

// BackupDestination mirrors the legacy S3-or-PVC destination union.
type BackupDestination struct {
	S3  *S3BackupDestination  `json:"s3,omitempty"`
	PVC *PVCBackupDestination `json:"pvc,omitempty"`
}

// S3BackupDestination mirrors the legacy S3 destination.
type S3BackupDestination struct {
	Endpoint             string                      `json:"endpoint"`
	Bucket               string                      `json:"bucket"`
	Key                  string                      `json:"key,omitempty"`
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
	Region               string                      `json:"region,omitempty"`
	ForcePathStyle       bool                        `json:"forcePathStyle,omitempty"`
}

// PVCBackupDestination mirrors the legacy PVC destination.
type PVCBackupDestination struct {
	ClaimName string `json:"claimName"`
	SubPath   string `json:"subPath,omitempty"`
}

// EtcdBackupScheduleSpec mirrors the legacy recurring-backup spec.
type EtcdBackupScheduleSpec struct {
	ClusterRef                 corev1.LocalObjectReference `json:"clusterRef"`
	Schedule                   string                      `json:"schedule"`
	Destination                BackupDestination           `json:"destination"`
	SuccessfulJobsHistoryLimit *int32                      `json:"successfulJobsHistoryLimit,omitempty"`
	FailedJobsHistoryLimit     *int32                      `json:"failedJobsHistoryLimit,omitempty"`
}
