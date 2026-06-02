/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha2

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupDestination selects where an etcd snapshot is stored (for EtcdBackup)
// or read from (for a restore source). Exactly one of S3/PVC must be set —
// enforced by CEL.
//
// +kubebuilder:validation:XValidation:rule="has(self.s3) != has(self.pvc)",message="exactly one of destination.s3 or destination.pvc must be set"
type BackupDestination struct {
	// S3 stores the snapshot in an S3-compatible object store.
	// +optional
	S3 *S3BackupDestination `json:"s3,omitempty"`

	// PVC stores the snapshot on a PersistentVolumeClaim.
	// +optional
	PVC *PVCBackupDestination `json:"pvc,omitempty"`
}

// S3BackupDestination describes an S3-compatible object-store location.
type S3BackupDestination struct {
	// Endpoint is the S3 endpoint URL (e.g. "https://s3.amazonaws.com" or a
	// MinIO/Ceph endpoint).
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`

	// Bucket is the destination bucket.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Key is an optional object-key prefix within the bucket. The operator
	// appends "<backup-name>.db".
	// +optional
	Key string `json:"key,omitempty"`

	// CredentialsSecretRef references a Secret in the cluster's namespace
	// holding AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY keys.
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`

	// Region is the S3 region. Optional for endpoints that ignore it.
	// +optional
	Region string `json:"region,omitempty"`

	// ForcePathStyle selects path-style addressing (bucket in the path
	// rather than the host). MinIO/Ceph typically require true.
	// +optional
	ForcePathStyle bool `json:"forcePathStyle,omitempty"`
}

// PVCBackupDestination describes a PersistentVolumeClaim location.
type PVCBackupDestination struct {
	// ClaimName is the name of a PVC in the cluster's namespace.
	// +kubebuilder:validation:MinLength=1
	ClaimName string `json:"claimName"`

	// SubPath is an optional subdirectory within the volume.
	// +optional
	SubPath string `json:"subPath,omitempty"`
}

// EtcdBackupStatusPhase is the lifecycle phase of an EtcdBackup.
type EtcdBackupStatusPhase string

const (
	// EtcdBackupStatusPhasePending is the initial phase before the Job is created.
	EtcdBackupStatusPhasePending EtcdBackupStatusPhase = "Pending"
	// EtcdBackupStatusPhaseStarted means the backup Job is running.
	EtcdBackupStatusPhaseStarted EtcdBackupStatusPhase = "Started"
	// EtcdBackupStatusPhaseComplete means the snapshot was captured and stored.
	EtcdBackupStatusPhaseComplete EtcdBackupStatusPhase = "Complete"
	// EtcdBackupStatusPhaseFailed means the backup failed.
	EtcdBackupStatusPhaseFailed EtcdBackupStatusPhase = "Failed"
)

// BackupReady is the single lifecycle condition on an EtcdBackup. Its Status is
// True only in the terminal Complete phase; Pending/Started/Failed report False
// with the Reason carrying the specific state (e.g. JobRunning, JobFailed,
// ClusterNotFound). One condition — rather than a distinct type per phase —
// means a terminal backup can't report a stale "Started=True" alongside its
// "Failed"/"Complete" outcome (setCondition only ever updates the matching
// Type, so sibling types would otherwise never be flipped back to False).
const BackupReady = "Ready"

// EtcdBackupSpec defines a one-shot etcd snapshot of a cluster to a
// destination. Backups are immutable: change the destination by creating a
// new EtcdBackup.
type EtcdBackupSpec struct {
	// ClusterRef names the EtcdCluster (same namespace) to snapshot.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// Destination selects where the snapshot is stored (S3 or PVC).
	Destination BackupDestination `json:"destination"`
}

// BackupSnapshot records the stored snapshot's coordinates.
type BackupSnapshot struct {
	// URI is the full snapshot location, e.g. "s3://bucket/key.db" or
	// "file:///backup/data/name.db".
	URI string `json:"uri"`

	// SizeBytes is the snapshot size in bytes.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// Checksum is "<algo>:<hex>", e.g. "sha256:abc123...".
	// +optional
	Checksum string `json:"checksum,omitempty"`
}

// EtcdBackupStatus is the observed state of an EtcdBackup.
type EtcdBackupStatus struct {
	// Phase is the high-level lifecycle phase.
	// +optional
	Phase EtcdBackupStatusPhase `json:"phase,omitempty"`

	// Snapshot is populated once the backup completes.
	// +optional
	Snapshot *BackupSnapshot `json:"snapshot,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdBackup is the Schema for the etcdbackups API. It captures a one-shot
// snapshot of an EtcdCluster to a destination (S3 or PVC).
type EtcdBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdBackupSpec   `json:"spec,omitempty"`
	Status EtcdBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdBackupList contains a list of EtcdBackup.
type EtcdBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdBackup{}, &EtcdBackupList{})
}
