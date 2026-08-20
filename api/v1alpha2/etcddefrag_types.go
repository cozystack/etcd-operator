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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EtcdDefragSpec is the desired state of an EtcdDefrag: a one-shot request to
// defragment an EtcdCluster's members.
//
// +kubebuilder:validation:XValidation:rule="size(self.clusterRef.name) != 0",message="spec.clusterRef.name is required"
type EtcdDefragSpec struct {
	// ClusterRef names the EtcdCluster (same namespace) to defragment.
	ClusterRef corev1.LocalObjectReference `json:"clusterRef"`

	// Rule decides which members this run touches. Absent is equivalent to an
	// empty rule: the default gate (freeSpaceAbove 200Mi). To defragment every
	// member unconditionally, set rule.all: true.
	// +optional
	Rule *DefragRule `json:"rule,omitempty"`

	// TTLSecondsAfterFinished records how long after a terminal phase this
	// object should be garbage-collected — meaningful for objects a scheduler
	// stamps out. Acted on by the reconciling controller; the API server does not
	// garbage-collect custom resources on its own. Absent means the record is kept.
	// +kubebuilder:validation:Minimum=0
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// DefragRule decides whether a member is worth defragmenting. A defrag can only
// reclaim DbSize-DbSizeInUse, so that reclaimable amount is the always-applied
// floor — what stops a full-but-unfragmented backend (DbSize ≈ DbSizeInUse near
// the quota) from being defragmented forever for nothing.
//
// +kubebuilder:validation:XValidation:rule="!(has(self.all) && self.all) || (!has(self.freeSpaceAbove) && !has(self.quotaUsageAbove) && !has(self.minReclaim))",message="rule.all cannot be combined with freeSpaceAbove/quotaUsageAbove/minReclaim"
// +kubebuilder:validation:XValidation:rule="!has(self.freeSpaceAbove) || quantity(string(self.freeSpaceAbove)).isGreaterThan(quantity('0'))",message="freeSpaceAbove must be greater than 0"
// +kubebuilder:validation:XValidation:rule="!has(self.minReclaim) || quantity(string(self.minReclaim)).isGreaterThan(quantity('0'))",message="minReclaim must be greater than 0"
// +kubebuilder:validation:XValidation:rule="!has(self.minReclaim) || has(self.quotaUsageAbove)",message="minReclaim is only meaningful with quotaUsageAbove"
// +kubebuilder:validation:XValidation:rule="!(has(self.minReclaim) && has(self.freeSpaceAbove)) || quantity(string(self.minReclaim)).compareTo(quantity(string(self.freeSpaceAbove))) <= 0",message="minReclaim must not exceed freeSpaceAbove"
type DefragRule struct {
	// All defragments every member unconditionally, regardless of size — the
	// explicit "do it now". Mutually exclusive with the threshold fields below.
	// +optional
	All bool `json:"all,omitempty"`

	// FreeSpaceAbove defragments a member whose reclaimable space
	// (DbSize-DbSizeInUse) exceeds this. The primary, always-applied gate.
	// Absent means the built-in default (200Mi).
	// +optional
	FreeSpaceAbove *resource.Quantity `json:"freeSpaceAbove,omitempty"`

	// QuotaUsageAbove: when DbSize exceeds this fraction of the backend quota
	// (approaching NOSPACE), lower the reclaimable floor to MinReclaim so small
	// wins are taken under pressure. A member is never defragmented when its
	// reclaimable space is below MinReclaim. Integer percent 1..99 with a "%"
	// suffix, e.g. "80%"; 100% is rejected because a backend never exceeds its
	// quota (etcd raises NOSPACE first), so the arm could never fire.
	// +kubebuilder:validation:Pattern=`^[1-9][0-9]?%$`
	// +optional
	QuotaUsageAbove string `json:"quotaUsageAbove,omitempty"`

	// MinReclaim floors the quota arm: even under quota pressure, skip a member
	// that would reclaim less than this. Only meaningful with QuotaUsageAbove,
	// and must not exceed FreeSpaceAbove. Absent means the built-in default
	// (32Mi).
	// +optional
	MinReclaim *resource.Quantity `json:"minReclaim,omitempty"`
}

// EtcdDefragPhase is the lifecycle phase of an EtcdDefrag.
type EtcdDefragPhase string

const (
	// EtcdDefragPhasePending is the initial phase: the request is queued.
	// Defragmentations are serialized per cluster, so a request waits here while
	// another runs against the same EtcdCluster, or while the cluster is not yet
	// healthy enough to defragment safely (surfaced as a condition).
	EtcdDefragPhasePending EtcdDefragPhase = "Pending"
	// EtcdDefragPhaseRunning means the member sweep is in progress.
	EtcdDefragPhaseRunning EtcdDefragPhase = "Running"
	// EtcdDefragPhaseComplete means the sweep finished; see status.members for
	// per-member outcomes.
	EtcdDefragPhaseComplete EtcdDefragPhase = "Complete"
	// EtcdDefragPhaseFailed means the sweep could not complete.
	EtcdDefragPhaseFailed EtcdDefragPhase = "Failed"
)

// DefragOutcome is the result of processing a single member.
type DefragOutcome string

const (
	// DefragOutcomePending: not yet processed.
	DefragOutcomePending DefragOutcome = "Pending"
	// DefragOutcomeSkipped: below the rule's threshold, nothing worth reclaiming.
	DefragOutcomeSkipped DefragOutcome = "Skipped"
	// DefragOutcomeDefragmented: successfully defragmented.
	DefragOutcomeDefragmented DefragOutcome = "Defragmented"
	// DefragOutcomeFailed: the Defragment RPC failed or timed out.
	DefragOutcomeFailed DefragOutcome = "Failed"
)

// MemberRole is a member's raft role at the time it was processed.
type MemberRole string

const (
	MemberRoleLeader   MemberRole = "leader"
	MemberRoleFollower MemberRole = "follower"
)

// EtcdDefragStatus is the observed state of an EtcdDefrag.
type EtcdDefragStatus struct {
	// Phase is the high-level lifecycle phase.
	// +optional
	Phase EtcdDefragPhase `json:"phase,omitempty"`

	// StartedAt is when the sweep began.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the sweep reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Defragmented counts members actually defragmented this run.
	// +optional
	Defragmented int32 `json:"defragmented,omitempty"`

	// Members holds the per-member outcome of the sweep, keyed by member name.
	// +optional
	// +listType=map
	// +listMapKey=name
	Members []MemberDefragStatus `json:"members,omitempty"`

	// Conditions represent the latest available observations — including why a
	// Pending run is being deferred (e.g. the cluster is not fully healthy).
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// MemberDefragStatus is the outcome of defragmenting a single member.
type MemberDefragStatus struct {
	// Name is the EtcdMember this row describes.
	Name string `json:"name"`

	// Role is the member's raft role at the time it was processed.
	// +optional
	Role MemberRole `json:"role,omitempty"`

	// Outcome is the result of processing this member.
	// +optional
	Outcome DefragOutcome `json:"outcome,omitempty"`

	// Reason qualifies the outcome (e.g. BelowThreshold, ClusterNotHealthy,
	// RPCError), in condition-reason style.
	// +optional
	Reason string `json:"reason,omitempty"`

	// DBSizeBefore is the member's physical backend size before defragmenting.
	// +optional
	DBSizeBefore int64 `json:"dbSizeBefore,omitempty"`

	// DBSizeAfter is the physical backend size after defragmenting.
	// +optional
	DBSizeAfter int64 `json:"dbSizeAfter,omitempty"`

	// ReclaimedBytes is DBSizeBefore-DBSizeAfter for a completed defrag.
	// +optional
	ReclaimedBytes int64 `json:"reclaimedBytes,omitempty"`

	// FinishedAt is when this member was processed.
	// +optional
	FinishedAt *metav1.Time `json:"finishedAt,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterRef.name`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Defragmented",type=integer,JSONPath=`.status.defragmented`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdDefrag is the Schema for the etcddefrags API. It requests a one-shot,
// run-to-completion defragmentation of an EtcdCluster's members. Like
// EtcdSnapshot it is a record: the operator drives it through status.phase and
// it never re-runs.
type EtcdDefrag struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdDefragSpec   `json:"spec,omitempty"`
	Status EtcdDefragStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdDefragList contains a list of EtcdDefrag.
type EtcdDefragList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdDefrag `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdDefrag{}, &EtcdDefragList{})
}
