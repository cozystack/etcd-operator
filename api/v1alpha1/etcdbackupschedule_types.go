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
	EtcdBackupScheduleConditionReady  = "Ready"
	EtcdBackupScheduleConditionFailed = "Failed"
)

// EtcdBackupScheduleSpec defines the desired state of EtcdBackupSchedule
type EtcdBackupScheduleSpec struct {
	// ClusterRef references the EtcdCluster to back up.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`
	// Schedule is a cron expression defining when backups should be taken.
	Schedule string `json:"schedule"`
	// Destination defines where the backup will be stored.
	Destination BackupDestination `json:"destination"`
	// SuccessfulJobsHistoryLimit is the number of successful finished CronJob children to retain.
	// +kubebuilder:validation:Minimum=0
	// +optional
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty"`
	// FailedJobsHistoryLimit is the number of failed finished CronJob children to retain.
	// +kubebuilder:validation:Minimum=0
	// +optional
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty"`
	// ActiveBackupJobDeadline is the duration in seconds that each scheduled
	// backup Job may be active before the system tries to terminate it.
	// +kubebuilder:default=900
	// +kubebuilder:validation:Minimum=0
	// +optional
	ActiveBackupJobDeadline int64 `json:"activeBackupJobDeadline,omitempty"`
	// FinishedBackupJobsTTL is the TTL in seconds for cleaning up finished
	// backup Jobs spawned by the CronJob.
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=0
	// +optional
	FinishedBackupJobsTTL int32 `json:"finishedBackupJobsTTL,omitempty"`
}

// EtcdBackupScheduleStatus defines the observed state of EtcdBackupSchedule
type EtcdBackupScheduleStatus struct {
	Conditions               []metav1.Condition `json:"conditions,omitempty"`
	LastScheduleTime         *metav1.Time       `json:"lastScheduleTime,omitempty"`
	LastSuccessfulBackupTime *metav1.Time       `json:"lastSuccessfulBackupTime,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Last Backup",type=date,JSONPath=`.status.lastSuccessfulBackupTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdBackupSchedule is the Schema for the etcdbackupschedules API
type EtcdBackupSchedule struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdBackupScheduleSpec   `json:"spec,omitempty"`
	Status EtcdBackupScheduleStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdBackupScheduleList contains a list of EtcdBackupSchedule
type EtcdBackupScheduleList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdBackupSchedule `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdBackupSchedule{}, &EtcdBackupScheduleList{})
}
