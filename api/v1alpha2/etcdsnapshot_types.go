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

// SnapshotLocation selects where an etcd snapshot is stored (for EtcdSnapshot)
// or read from (for a restore source). Exactly one of S3/PVC must be set —
// enforced by CEL.
//
// +kubebuilder:validation:XValidation:rule="has(self.s3) != has(self.pvc)",message="exactly one of destination.s3 or destination.pvc must be set"
type SnapshotLocation struct {
	// S3 stores the snapshot in an S3-compatible object store.
	// +optional
	S3 *S3SnapshotLocation `json:"s3,omitempty"`

	// PVC stores the snapshot on a PersistentVolumeClaim.
	// +optional
	PVC *PVCSnapshotLocation `json:"pvc,omitempty"`
}

// S3SnapshotLocation describes an S3-compatible object-store location.
type S3SnapshotLocation struct {
	// Endpoint is the S3 endpoint URL (e.g. "https://s3.amazonaws.com" or a
	// MinIO/Ceph endpoint).
	// +kubebuilder:validation:MinLength=1
	Endpoint string `json:"endpoint"`

	// Bucket is the destination bucket.
	// +kubebuilder:validation:MinLength=1
	Bucket string `json:"bucket"`

	// Key is an optional object-key prefix within the bucket. The operator
	// appends "<snapshot-name>.db".
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

// PVCSnapshotLocation describes a PersistentVolumeClaim location.
type PVCSnapshotLocation struct {
	// ClaimName is the name of a PVC in the cluster's namespace.
	// +kubebuilder:validation:MinLength=1
	ClaimName string `json:"claimName"`

	// SubPath is an optional subdirectory within the volume.
	// +optional
	SubPath string `json:"subPath,omitempty"`
}

// EtcdSnapshotStatusPhase is the lifecycle phase of an EtcdSnapshot.
type EtcdSnapshotStatusPhase string

const (
	// EtcdSnapshotStatusPhasePending is the initial phase before the Job is created.
	EtcdSnapshotStatusPhasePending EtcdSnapshotStatusPhase = "Pending"
	// EtcdSnapshotStatusPhaseStarted means the snapshot Job is running.
	EtcdSnapshotStatusPhaseStarted EtcdSnapshotStatusPhase = "Started"
	// EtcdSnapshotStatusPhaseComplete means the snapshot was captured and stored.
	EtcdSnapshotStatusPhaseComplete EtcdSnapshotStatusPhase = "Complete"
	// EtcdSnapshotStatusPhaseFailed means the snapshot failed.
	EtcdSnapshotStatusPhaseFailed EtcdSnapshotStatusPhase = "Failed"
)

// SnapshotReady is the single lifecycle condition on an EtcdSnapshot. Its Status is
// True only in the terminal Complete phase; Pending/Started/Failed report False
// with the Reason carrying the specific state (e.g. JobRunning, JobFailed,
// ClusterNotFound). One condition — rather than a distinct type per phase —
// means a terminal snapshot can't report a stale "Started=True" alongside its
// "Failed"/"Complete" outcome (setCondition only ever updates the matching
// Type, so sibling types would otherwise never be flipped back to False).
const SnapshotReady = "Ready"

// EtcdSnapshotSpec defines a one-shot etcd snapshot of a cluster to a
// destination. Snapshots are immutable: change the destination by creating a
// new EtcdSnapshot.
type EtcdSnapshotSpec struct {
	// ClusterRef names the EtcdCluster (same namespace) to snapshot.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// Destination selects where the snapshot is stored (S3 or PVC).
	Destination SnapshotLocation `json:"destination"`
}

// SnapshotArtifact records the stored snapshot's coordinates.
type SnapshotArtifact struct {
	// URI is the full snapshot location, e.g. "s3://bucket/key.db" or
	// "file:///snapshot/data/name.db".
	URI string `json:"uri"`

	// SizeBytes is the snapshot size in bytes.
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty"`

	// Checksum is "<algo>:<hex>", e.g. "sha256:abc123...".
	// +optional
	Checksum string `json:"checksum,omitempty"`
}

// EtcdSnapshotStatus is the observed state of an EtcdSnapshot.
type EtcdSnapshotStatus struct {
	// Phase is the high-level lifecycle phase.
	// +optional
	Phase EtcdSnapshotStatusPhase `json:"phase,omitempty"`

	// Artifact is populated once the snapshot completes.
	// +optional
	Artifact *SnapshotArtifact `json:"artifact,omitempty"`

	// Conditions represent the latest available observations.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdSnapshot is the Schema for the etcdsnapshots API. It captures a one-shot
// snapshot of an EtcdCluster to a destination (S3 or PVC).
type EtcdSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdSnapshotSpec   `json:"spec,omitempty"`
	Status EtcdSnapshotStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdSnapshotList contains a list of EtcdSnapshot.
type EtcdSnapshotList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdSnapshot `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdSnapshot{}, &EtcdSnapshotList{})
}
