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

// EtcdMemberTLS is the per-member view of the parent cluster's
// EtcdClusterTLS. The cluster controller copies the *secret references*
// here at member creation; operator-side material
// (EtcdClusterTLS.Client.OperatorClientSecretRef) is NOT mirrored because
// it never gets mounted into the etcd Pod.
type EtcdMemberTLS struct {
	// ClientServerSecretRef mirrors EtcdClusterTLS.Client.ServerSecretRef.
	// When nil, the member runs the client API in plaintext.
	// +optional
	ClientServerSecretRef *corev1.LocalObjectReference `json:"clientServerSecretRef,omitempty"`

	// ClientMTLS mirrors "EtcdClusterTLS.Client.OperatorClientSecretRef
	// is set" — i.e. whether the etcd server should be started with
	// --client-cert-auth=true and --trusted-ca-file. Decoupled from the
	// secret ref because the secret itself is operator-side only.
	// +optional
	ClientMTLS bool `json:"clientMTLS,omitempty"`

	// PeerSecretRef mirrors EtcdClusterTLS.Peer.SecretRef. When nil, the
	// member runs the peer API in plaintext. When set, peer is always
	// mTLS (--peer-client-cert-auth=true).
	// +optional
	PeerSecretRef *corev1.LocalObjectReference `json:"peerSecretRef,omitempty"`
}

// Condition types for EtcdMember.
const (
	// MemberJoined indicates the member has been added to the etcd cluster.
	MemberJoined = "Joined"
	// MemberReady indicates the member is healthy and serving requests.
	MemberReady = "Ready"
)

// EtcdMemberSpec defines the desired state of a single etcd member.
// Created and managed by the EtcdCluster controller.
type EtcdMemberSpec struct {
	// ClusterName is the name of the owning EtcdCluster.
	// +kubebuilder:validation:MinLength=1
	ClusterName string `json:"clusterName"`

	// Version is the etcd version for this member.
	// +kubebuilder:validation:Pattern=`^\d+\.\d+\.\d+$`
	Version string `json:"version"`

	// Storage mirrors EtcdCluster.spec.storage at the time this member
	// was created. The cluster controller copies size and medium onto
	// each member at creation; the member controller treats it as
	// immutable per-member spec.
	Storage StorageSpec `json:"storage"`

	// Resources mirrors EtcdCluster.spec.resources at the time this
	// member was created. The cluster controller copies it onto each
	// member at creation. The member controller passes the value
	// straight to the etcd container's resources field at Pod-build
	// time; existing members are not re-templated when the cluster
	// spec changes.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// AdditionalMetadata mirrors EtcdCluster.spec.additionalMetadata at the
	// time this member was created. The member controller merges it onto the
	// member's Pod (operator-owned label keys win on collision).
	// +optional
	AdditionalMetadata *AdditionalMetadata `json:"additionalMetadata,omitempty"`

	// Affinity mirrors EtcdCluster.spec.affinity at the time this member was
	// created. The member controller passes it straight to the Pod's
	// spec.affinity at build time.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// TopologySpreadConstraints mirrors EtcdCluster.spec.topologySpreadConstraints
	// at the time this member was created. Passed straight to the Pod's
	// spec.topologySpreadConstraints at build time.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// Bootstrap indicates this member is part of the initial cluster formation.
	// When true the member starts with --initial-cluster-state=new.
	// +optional
	Bootstrap bool `json:"bootstrap,omitempty"`

	// InitialCluster is the value passed to etcd's --initial-cluster flag.
	// Set by the cluster controller at creation time.
	InitialCluster string `json:"initialCluster"`

	// ClusterToken is the value passed to etcd's --initial-cluster-token.
	// Copied from EtcdCluster.status.clusterToken so all members of a cluster
	// agree, and so changes to the cluster's token derivation rule don't
	// affect already-running members.
	ClusterToken string `json:"clusterToken"`

	// Replicas exists only because the PodDisruptionBudget controller
	// traverses Pods' controllerRef looking for /scale on the parent and
	// fails closed ("does not implement the scale subresource") if it
	// isn't there. Each EtcdMember represents exactly one Pod; this field
	// is locked to 1 by validation and cannot be tuned.
	// +kubebuilder:default=1
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Dormant marks the member as paused. While dormant, the member
	// controller deletes the member's Pod but leaves the PVC in place
	// (the PVC stays owned by this EtcdMember). The cluster controller
	// flips Dormant=true on the surviving member when the user sets
	// EtcdCluster.spec.replicas=0 on a 1-member cluster, and flips it
	// back to false when the user scales up. Re-creating the Pod against
	// the existing PVC lets etcd resume from the existing data dir with
	// the same ClusterID and member ID. While dormant, the member does
	// not count toward the EtcdCluster's `current` replica accounting.
	// +optional
	Dormant bool `json:"dormant,omitempty"`

	// TLS mirrors the parent cluster's TLS configuration at the time
	// this member was created. Carries only what the etcd Pod needs to
	// see (secret references and the mTLS flag); operator-side material
	// stays on the parent cluster spec.
	// +optional
	TLS *EtcdMemberTLS `json:"tls,omitempty"`

	// Restore is set only on the bootstrap seed when the parent cluster's
	// spec.bootstrap.restore is configured. It causes the member controller
	// to run a restore initContainer that populates the data dir from the
	// snapshot before etcd starts. Inert once the data dir is initialized.
	// +optional
	Restore *RestoreSpec `json:"restore,omitempty"`
}

// EtcdMemberStatus defines the observed state of a single etcd member.
type EtcdMemberStatus struct {
	// MemberID is the etcd-assigned member ID in hex (e.g. "ae36f238164a08ad"),
	// set once the member joins the cluster. Stored as a string because uint64
	// values can exceed JSON's safe integer range.
	// +optional
	MemberID string `json:"memberID,omitempty"`

	// PodName is the name of the Pod running this member.
	// +optional
	PodName string `json:"podName,omitempty"`

	// PodUID is the UID of the Pod most recently observed for this member.
	// Set when the Pod is created or found; cleared when the Pod is gone
	// and the member controller intentionally removed it (e.g. dormant).
	// For memory-backed members the operator compares the live Pod's UID
	// against this value to detect Pod loss: a stored UID with no live
	// matching Pod means the tmpfs is gone and the member must be replaced.
	// +optional
	PodUID string `json:"podUID,omitempty"`

	// IsVoter is true when etcd's MemberList reports this member with
	// IsLearner=false — i.e. it counts toward quorum. Written by the
	// cluster controller during its MemberList processing and pre-stamped
	// true at seed creation (the seed is never a learner). Read by the
	// member controller to apply the role=voter Pod label that the
	// per-cluster PodDisruptionBudget selects on. Default value false is
	// the safe-but-temporary state for a freshly-added learner before
	// MemberPromote runs.
	// +optional
	IsVoter bool `json:"isVoter,omitempty"`

	// Replicas exposes via /scale "this EtcdMember owns 1 Pod if it has
	// a PodName, 0 otherwise". Required by the PodDisruptionBudget
	// controller to derive expectedPods for the cluster's PDB — without
	// /scale on the Pod controller-ref it sets the PDB to SyncFailed.
	// +optional
	Replicas int32 `json:"replicas,omitempty"`

	// Selector exposes the label-selector that matches this member's Pod
	// via /scale (consumed by the PDB controller; not user-facing).
	// +optional
	Selector string `json:"selector,omitempty"`

	// PVCName is the name of the PersistentVolumeClaim for this member's data.
	// +optional
	PVCName string `json:"pvcName,omitempty"`

	// Conditions represent the latest available observations of the member's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.replicas,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Cluster",type=string,JSONPath=`.spec.clusterName`
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdMember represents a single member of an etcd cluster.
// EtcdMember resources are created and deleted by the EtcdCluster controller.
// Users should not create these directly.
type EtcdMember struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdMemberSpec   `json:"spec,omitempty"`
	Status EtcdMemberStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdMemberList contains a list of EtcdMember.
type EtcdMemberList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdMember `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdMember{}, &EtcdMemberList{})
}
