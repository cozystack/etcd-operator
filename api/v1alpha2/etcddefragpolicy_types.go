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

// ConcurrencyPolicy decides what a due tick does when a run stamped by the same
// policy is still in flight.
// +kubebuilder:validation:Enum=Allow;Forbid
type ConcurrencyPolicy string

const (
	// AllowConcurrent stamps the due run even while a previous one is still
	// active. EtcdDefrag serializes per cluster on its own, so the new run
	// simply queues behind the active one.
	AllowConcurrent ConcurrencyPolicy = "Allow"
	// ForbidConcurrent skips the due tick while a run stamped by this policy is
	// still active, rather than letting runs pile up. The default.
	ForbidConcurrent ConcurrencyPolicy = "Forbid"
)

// EtcdDefragPolicySpec is the desired state of an EtcdDefragPolicy: a recurring
// schedule that stamps out EtcdDefrag runs against one EtcdCluster, so the
// operator absorbs the cadence instead of relying on an external CronJob.
type EtcdDefragPolicySpec struct {
	// ClusterRef names the EtcdCluster (same namespace) each stamped EtcdDefrag
	// targets.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// Schedule is a standard five-field cron expression, interpreted in UTC,
	// naming when a run is stamped (e.g. "0 3 * * *" for 03:00 daily).
	// +kubebuilder:validation:MinLength=1
	Schedule string `json:"schedule"`

	// Suspend pauses stamping. Runs already in flight are left alone; clearing
	// it resumes at the next scheduled tick (missed ticks are not backfilled).
	// +optional
	Suspend *bool `json:"suspend,omitempty"`

	// ConcurrencyPolicy decides what a due tick does when a previous stamped run
	// is still active. Defaults to Forbid.
	// +optional
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrencyPolicy,omitempty"`

	// StartingDeadlineSeconds bounds how late a missed tick may still be started.
	// If the operator was down (or the tick forbidden) and more than this many
	// seconds have passed since the scheduled time, that tick is skipped rather
	// than started late. Absent means no deadline.
	// +kubebuilder:validation:Minimum=0
	// +optional
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty"`

	// HistoryLimit caps how many finished (Complete/Failed) EtcdDefrags stamped
	// by this policy are retained; the oldest beyond the limit are deleted.
	// Absent leaves cleanup to each run's ttlSecondsAfterFinished.
	// +kubebuilder:validation:Minimum=0
	// +optional
	HistoryLimit *int32 `json:"historyLimit,omitempty"`

	// Rule is stamped verbatim into each EtcdDefrag; it decides which members a
	// run touches. Absent stamps runs with no rule (the default gate).
	// +optional
	Rule *DefragRule `json:"rule,omitempty"`

	// TTLSecondsAfterFinished is stamped into each EtcdDefrag so a stamped run
	// garbage-collects itself once finished. Complements HistoryLimit.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// EtcdDefragPolicyStatus is the observed state of an EtcdDefragPolicy.
type EtcdDefragPolicyStatus struct {
	// LastScheduleTime is the scheduled time of the most recent tick the policy
	// acted on (stamped or deliberately skipped). It anchors the next tick, so a
	// tick is never acted on twice.
	// +optional
	LastScheduleTime *metav1.Time `json:"lastScheduleTime,omitempty"`

	// LastSuccessfulTime is when a stamped run most recently reached Complete.
	// +optional
	LastSuccessfulTime *metav1.Time `json:"lastSuccessfulTime,omitempty"`

	// Active references the stamped EtcdDefrags that have not yet finished.
	// +optional
	// +listType=atomic
	Active []corev1.LocalObjectReference `json:"active,omitempty"`

	// Conditions represent the latest observations — notably why stamping is
	// paused (Suspended) or not happening (InvalidSchedule).
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Schedule",type=string,JSONPath=`.spec.schedule`
// +kubebuilder:printcolumn:name="Suspend",type=boolean,JSONPath=`.spec.suspend`
// +kubebuilder:printcolumn:name="Last Schedule",type=date,JSONPath=`.status.lastScheduleTime`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdDefragPolicy is the Schema for the etcddefragpolicies API. It stamps out
// EtcdDefrag runs on a cron schedule so the operator drives recurring
// defragmentation itself. Each run is a discrete, auditable EtcdDefrag owned by
// the policy.
type EtcdDefragPolicy struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdDefragPolicySpec   `json:"spec,omitempty"`
	Status EtcdDefragPolicyStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdDefragPolicyList contains a list of EtcdDefragPolicy.
type EtcdDefragPolicyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdDefragPolicy `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdDefragPolicy{}, &EtcdDefragPolicyList{})
}
