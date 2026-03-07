/*
Copyright 2024 The etcd-operator Authors.

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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	EtcdBackupConditionStarted  = "Started"
	EtcdBackupConditionComplete = "Complete"
	EtcdBackupConditionFailed   = "Failed"
)

// EtcdBackupSpec defines the desired state of EtcdBackup
type EtcdBackupSpec struct {
	// ClusterRef references the EtcdCluster to back up.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`
	// Destination defines where the backup will be stored.
	Destination BackupDestination `json:"destination"`
}

// BackupDestination defines the target location for the backup. Exactly one must be specified.
type BackupDestination struct {
	// S3 defines S3-compatible storage as the backup destination.
	// +optional
	S3 *S3BackupDestination `json:"s3,omitempty"`
	// PVC defines a PersistentVolumeClaim as the backup destination.
	// +optional
	PVC *PVCBackupDestination `json:"pvc,omitempty"`
}

// S3BackupDestination defines S3-compatible storage parameters.
type S3BackupDestination struct {
	// Endpoint is the S3-compatible endpoint URL (e.g., "https://s3.amazonaws.com").
	Endpoint string `json:"endpoint"`
	// Bucket is the name of the S3 bucket.
	Bucket string `json:"bucket"`
	// Key is the object key (path) within the bucket.
	Key string `json:"key"`
	// CredentialsSecretRef references a Secret containing AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY keys.
	CredentialsSecretRef corev1.LocalObjectReference `json:"credentialsSecretRef"`
	// Region is the AWS region for the S3 bucket.
	// +optional
	Region string `json:"region,omitempty"`
}

// PVCBackupDestination defines a PersistentVolumeClaim as the backup target.
type PVCBackupDestination struct {
	// ClaimName is the name of the PersistentVolumeClaim to use.
	ClaimName string `json:"claimName"`
	// SubPath is an optional sub-path within the PVC volume.
	// +optional
	SubPath string `json:"subPath,omitempty"`
}

// EtcdBackupStatus defines the observed state of EtcdBackup
type EtcdBackupStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Status",type=string,JSONPath=`.status.conditions[?(@.type=="Complete")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdBackup is the Schema for the etcdbackups API
type EtcdBackup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdBackupSpec   `json:"spec,omitempty"`
	Status EtcdBackupStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdBackupList contains a list of EtcdBackup
type EtcdBackupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdBackup `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdBackup{}, &EtcdBackupList{})
}
