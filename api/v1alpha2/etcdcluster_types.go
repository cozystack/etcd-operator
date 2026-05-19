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

// EtcdClusterTLS configures transport-layer security for the cluster's two
// etcd surfaces: the client API (port 2379) and the peer API (port 2380).
// Each subtree is independently optional. Subtree fields are immutable
// post-create — flipping TLS on or off on an existing cluster is a
// non-trivial rolling change (the operator's own etcd client must switch
// protocols in lockstep with the members), so v1 punts that to delete-and-
// recreate.
type EtcdClusterTLS struct {
	// Client configures TLS for the etcd client API (port 2379). Absent
	// means plaintext. See ClientTLS for the mTLS-toggle semantics.
	// +optional
	Client *ClientTLS `json:"client,omitempty"`

	// Peer configures TLS for the etcd peer API (port 2380). Absent
	// means plaintext. When set, peer is always mTLS — etcd's peer mesh
	// is symmetric and there is no useful encrypt-only-no-identity mode
	// for it.
	// +optional
	Peer *PeerTLS `json:"peer,omitempty"`
}

// ClientTLS configures TLS for the etcd client API.
//
// Modes (derived from which fields are populated):
//
//   - ServerSecretRef set, OperatorClientSecretRef absent → server-TLS only
//     (encryption, no client identity). etcd serves https://. The operator
//     dials with RootCAs from ServerSecretRef's ca.crt but presents no
//     client cert. ServerSecretRef.ca.crt is REQUIRED in this mode (the
//     operator needs to verify the server).
//
//   - ServerSecretRef set, OperatorClientSecretRef set → full mTLS. All of
//     the above, plus etcd is started with --client-cert-auth=true and
//     --trusted-ca-file pointing at ServerSecretRef.ca.crt. The operator
//     presents OperatorClientSecretRef's tls.crt/tls.key on every dial.
//
//     ServerSecretRef.ca.crt MUST include both (a) the CA that issued the
//     server cert (because etcd self-dials its own grpc-gateway loopback
//     and the server cert is presented as the client cert on that path —
//     so --trusted-ca-file has to verify it) and (b) the CA that signed
//     OperatorClientSecretRef.tls.crt. In the common one-CA topology these
//     are the same content; with two CAs on the client plane the user
//     bundles both PEM blocks into a single ca.crt.
//
//     ServerSecretRef.tls.crt MUST carry clientAuth in its EKU alongside
//     serverAuth — same loopback reason. The operator does not parse the
//     cert to validate this; misconfiguration surfaces as etcd startup
//     failure in the Pod logs.
type ClientTLS struct {
	// ServerSecretRef points at a Secret in the cluster's namespace
	// holding the etcd server cert in the standard kubernetes.io/tls
	// shape: tls.crt, tls.key, and ca.crt. ca.crt is always required
	// (the operator's own etcd client needs it to verify the server,
	// and when mTLS is on it doubles as --trusted-ca-file).
	ServerSecretRef corev1.LocalObjectReference `json:"serverSecretRef"`

	// OperatorClientSecretRef points at a Secret in the cluster's
	// namespace holding the operator's etcd-client identity (tls.crt,
	// tls.key). Setting this enables mTLS — etcd is started with
	// --client-cert-auth=true and the operator presents this cert when
	// dialing. Leaving it unset selects server-TLS-only mode.
	// +optional
	OperatorClientSecretRef *corev1.LocalObjectReference `json:"operatorClientSecretRef,omitempty"`
}

// PeerTLS configures TLS for the etcd peer API. When PeerTLS is set, peer
// is always mTLS.
type PeerTLS struct {
	// SecretRef points at a Secret in the cluster's namespace holding
	// the peer cert+key in the standard kubernetes.io/tls shape:
	// tls.crt, tls.key, ca.crt. ca.crt is required (peer is symmetric
	// — same cert is used to serve inbound and dial outbound peer
	// connections — and --peer-trusted-ca-file is always populated).
	// The peer cert MUST carry both serverAuth and clientAuth in EKU.
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// Condition types for EtcdCluster.
const (
	// ClusterAvailable indicates the cluster has a healthy quorum.
	ClusterAvailable = "Available"
	// ClusterProgressing indicates a scaling or version change is in progress.
	ClusterProgressing = "Progressing"
	// ClusterDegraded indicates some members are unhealthy but quorum holds.
	ClusterDegraded = "Degraded"
)

// StorageMedium selects the volume backend for each member's etcd data
// directory. The values mirror corev1.StorageMedium semantics: the empty
// string is the default (a PVC backed by the namespace's default
// StorageClass) and "Memory" is a tmpfs emptyDir whose lifetime is bound
// to the Pod.
//
// Memory-backed clusters trade durability for speed: a Pod that loses its
// tmpfs (eviction, node failure, deletion) loses its data and the member
// must be replaced. The operator detects this and removes the member via
// the existing finalizer flow; the cluster controller's scale-up gap-fill
// then creates a replacement member. This works only when quorum holds
// across the loss, so a single-replica memory cluster cannot survive a
// Pod eviction.
//
// Production memory clusters also want hard pod-anti-affinity and a
// container memory limit covering the tmpfs size plus etcd's own
// headroom. Those two are not auto-emitted by the operator yet — see
// https://github.com/lllamnyp/etcd-operator/issues/16. The
// PodDisruptionBudget is auto-emitted on every cluster.
//
// +kubebuilder:validation:Enum="";Memory
type StorageMedium string

const (
	// StorageMediumDefault uses a PersistentVolumeClaim per member.
	StorageMediumDefault StorageMedium = ""
	// StorageMediumMemory uses a tmpfs emptyDir per member.
	StorageMediumMemory StorageMedium = "Memory"
)

// StorageSpec configures the per-member data directory.
type StorageSpec struct {
	// Size is the requested capacity per member. For Medium="" (PVC) this
	// is the PVC's requested storage. For Medium="Memory" this is the
	// tmpfs emptyDir's SizeLimit.
	//
	// Shrinking is rejected on UPDATE: PVCs cannot shrink and tmpfs
	// SizeLimit reduction does not free already-allocated memory.
	// +kubebuilder:default="1Gi"
	// +kubebuilder:validation:XValidation:rule="quantity(string(self)).compareTo(quantity(string(oldSelf))) >= 0",message="spec.storage.size cannot be shrunk"
	// +optional
	Size resource.Quantity `json:"size,omitempty"`

	// Medium selects the volume backend: "" (PVC) or "Memory" (tmpfs
	// emptyDir). See the StorageMedium type doc for operational trade-offs.
	//
	// Immutable: changing the medium on an existing cluster would orphan
	// the previous PVC (or tmpfs) and the rolling-migrate path is not
	// implemented. The default ("") is set explicitly so the apiserver
	// always stores the field on Create — without it, a first-time set
	// from absent → Memory would slip past the transition rule.
	// +kubebuilder:default=""
	// +kubebuilder:validation:XValidation:rule="self == oldSelf",message="spec.storage.medium is immutable; delete and recreate the cluster to change the storage backend"
	// +optional
	Medium StorageMedium `json:"medium,omitempty"`

	// StorageClassName selects the StorageClass for the per-member PVC.
	// Mirrors corev1.PersistentVolumeClaimSpec.StorageClassName semantics:
	// nil (the default) uses the namespace's default StorageClass; the
	// empty string explicitly disables dynamic provisioning (a
	// pre-provisioned PV must already match the PVC selector); any other
	// value names a specific StorageClass.
	//
	// Ignored when Medium=Memory (no PVC is created).
	//
	// Immutable post-create — PersistentVolumeClaim.spec.storageClassName
	// is itself immutable after PVC creation, so honouring a mid-life
	// change would require a rolling PVC-recreation flow that this
	// operator does not perform. The immutability rules live at the
	// EtcdClusterSpec level (alongside the other pointer-field rules)
	// because *string transition CEL on the inner field cannot fire when
	// the field is being added from nil.
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`
}

// EtcdClusterSpec defines the desired state of an etcd cluster.
//
// CEL validation rules (k8s 1.29+ apiserver-enforced; both
// CustomResourceValidationExpressions and the quantity() extension
// are GA in 1.29):
//
//   - storage.medium=Memory + replicas=0 wedges the cluster on resume
//     (the dormant flip deletes the Pod and the tmpfs goes with it but
//     the resume path treats the member as if its data were preserved).
//     Reject the combination outright; recreate is the only safe path.
//
//   - storage.medium=Memory requires storage.size > 0. Without a SizeLimit
//     the tmpfs is unbounded against node memory, which defeats the whole
//     point of opting into memory backing.
//
// +kubebuilder:validation:XValidation:rule="!(has(self.replicas) && self.replicas == 0 && has(self.storage) && has(self.storage.medium) && self.storage.medium == 'Memory')",message="spec.replicas=0 with spec.storage.medium=Memory is unsupported: pausing a memory-backed cluster wedges on resume. Delete and recreate the cluster instead."
// +kubebuilder:validation:XValidation:rule="!(has(self.storage) && has(self.storage.medium) && self.storage.medium == 'Memory') || quantity(string(self.storage.size)).isGreaterThan(quantity('0'))",message="spec.storage.size must be > 0 when spec.storage.medium=Memory (the tmpfs sizeLimit cannot be zero)."
// +kubebuilder:validation:XValidation:rule="has(self.tls) == has(oldSelf.tls)",message="spec.tls cannot be added to or removed from an existing cluster; delete and recreate"
// +kubebuilder:validation:XValidation:rule="!has(self.tls) || !has(oldSelf.tls) || self.tls == oldSelf.tls",message="spec.tls is immutable post-create; delete and recreate the cluster to change TLS configuration"
// +kubebuilder:validation:XValidation:rule="has(self.storage.storageClassName) == has(oldSelf.storage.storageClassName)",message="spec.storage.storageClassName cannot be added to or removed from an existing cluster; delete and recreate"
// +kubebuilder:validation:XValidation:rule="!has(self.storage.storageClassName) || !has(oldSelf.storage.storageClassName) || self.storage.storageClassName == oldSelf.storage.storageClassName",message="spec.storage.storageClassName is immutable post-create (a PVC's storageClassName itself is immutable, and the operator does not roll PVCs); delete and recreate the cluster to change the StorageClass"
type EtcdClusterSpec struct {
	// Replicas is the desired number of cluster members. Should be odd.
	// A value of 0 parks the cluster ("scale to zero"): the operator
	// flips spec.dormant=true on the surviving EtcdMember, which causes
	// the member controller to delete that member's Pod. The EtcdMember
	// CR and its PVC are preserved (the PVC stays owned by the same
	// EtcdMember across the pause). Scaling back up to >=1 flips
	// spec.dormant=false on the same member; etcd resumes from its
	// existing data dir with the same ClusterID and member ID.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`

	// Version is the desired etcd version (e.g. "3.5.17").
	// +kubebuilder:validation:Pattern=`^\d+\.\d+\.\d+$`
	Version string `json:"version"`

	// Storage configures the per-member data directory: size and medium
	// (PVC or tmpfs). The size shrink-rejection and medium immutability
	// rules live as field-level CEL on the inner fields; the spec-level
	// CEL above couples replicas and storage.medium.
	// +kubebuilder:default={size: "1Gi", medium: ""}
	// +optional
	Storage StorageSpec `json:"storage,omitempty"`

	// ProgressDeadlineSeconds bounds the time the operator spends trying to
	// reach a desired state before abandoning the in-flight target and
	// adopting whatever the user has set as the new spec. Defaults to 600
	// (10 minutes). A patch to status.progressDeadline can shorten this for
	// a stuck reconcile (set it to "now" to abort immediately).
	// +kubebuilder:default=600
	// +kubebuilder:validation:Minimum=1
	// +optional
	ProgressDeadlineSeconds *int32 `json:"progressDeadlineSeconds,omitempty"`

	// TLS configures transport-layer security for the etcd client and
	// peer APIs. Absent means plaintext on both surfaces. The whole
	// subtree is immutable post-create — the immutability rules live at
	// the EtcdClusterSpec level (above) because pointer-field transition
	// rules don't fire when the field is being added (nil → set) and we
	// want to reject that direction too.
	// +optional
	TLS *EtcdClusterTLS `json:"tls,omitempty"`

	// Resources sets the etcd container's resource requests and limits.
	// When omitted, the operator falls back to a conservative default
	// (100m CPU + 128Mi memory requests, no limits) suitable for
	// kicking the tyres but not for production. Memory-backed clusters
	// specifically should set limits.memory covering the tmpfs SizeLimit
	// plus etcd's own headroom.
	//
	// Updates take effect on newly-created members (scale-up,
	// replacement). The operator does not roll existing Pods to apply
	// a resource change in place — wire a VerticalPodAutoscaler at
	// targetRef={kind: EtcdCluster, name: <cluster>} for that, or
	// delete one Pod at a time to recreate them with the new spec.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ObservedClusterSpec is the locked-in target the controller is currently
// reconciling toward. It is updated from Spec only when the previous target
// has been reached (or its deadline has expired). Reconciliation logic uses
// these fields, not Spec — that's how the controller "ignores" further spec
// changes mid-flight.
type ObservedClusterSpec struct {
	// Replicas is the locked target replica count.
	Replicas int32 `json:"replicas"`

	// Version is the locked target etcd version.
	Version string `json:"version"`

	// Storage is the locked target storage configuration. The locking
	// pattern prevents a mid-flight size grow from being honoured until
	// the current target is reached or its deadline expires. (Medium
	// can't change at all post-create — that's enforced by spec-level
	// CEL.)
	Storage StorageSpec `json:"storage"`

	// Resources is the locked target etcd container resources. The
	// locking pattern prevents a mid-flight resource change from being
	// honoured on newly-created members until the current target is
	// reached or its deadline expires.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// EtcdClusterStatus defines the observed state of an etcd cluster.
type EtcdClusterStatus struct {
	// ReadyMembers is the count of members that are healthy and serving.
	// Also exposed as Scale.Status.Replicas via the /scale subresource so
	// kubectl scale and clients like VerticalPodAutoscaler can read
	// "current scale" without a custom status field.
	ReadyMembers int32 `json:"readyMembers,omitempty"`

	// Selector is the label-selector form ("etcd.lllamnyp.su/cluster=<name>")
	// matching every Pod owned by this cluster. Surfaced for the /scale
	// subresource — the VPA admission controller reads it via Scales().Get()
	// to know which Pods to inject recommendations into. Plain users won't
	// see this field directly.
	// +optional
	Selector string `json:"selector,omitempty"`

	// BrokenMembers is the count of members the operator considers broken.
	// While the auto-replacement predicate is a stub it is always 0; surfaced
	// here so the predicate has a tested call site and the field already
	// exists when the policy lands.
	BrokenMembers int32 `json:"brokenMembers,omitempty"`

	// ClusterID is the etcd cluster ID in hex (e.g. "769f1c9e0d723d0b"),
	// set after initial bootstrap. Stored as a string because uint64 values
	// can exceed JSON's safe integer range.
	// +optional
	ClusterID string `json:"clusterID,omitempty"`

	// ClusterToken is the value passed to etcd's --initial-cluster-token,
	// recorded at bootstrap. Reused for all subsequent scale-up operations
	// so existing clusters keep their original token even if the derivation
	// rule changes in a later release.
	// +optional
	ClusterToken string `json:"clusterToken,omitempty"`

	// Observed is the locked-in desired state the operator is currently
	// reconciling toward. The reconciler ignores spec changes while a target
	// is in flight; Observed is only updated from spec when the current
	// target is met or its deadline has expired. nil before the first
	// reconcile.
	// +optional
	Observed *ObservedClusterSpec `json:"observed,omitempty"`

	// ProgressDeadline is the time at which the in-flight reconciliation
	// will be abandoned in favor of the latest spec. Cleared when the
	// cluster reaches Observed. Patch this to a time in the past to abort
	// a stuck reconcile.
	// +optional
	ProgressDeadline *metav1.Time `json:"progressDeadline,omitempty"`

	// Conditions represent the latest available observations of the cluster's state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:subresource:scale:specpath=.spec.replicas,statuspath=.status.readyMembers,selectorpath=.status.selector
// +kubebuilder:printcolumn:name="Version",type=string,JSONPath=`.spec.version`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyMembers`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// EtcdCluster is the Schema for the etcdclusters API.
type EtcdCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdClusterSpec   `json:"spec,omitempty"`
	Status EtcdClusterStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// EtcdClusterList contains a list of EtcdCluster.
type EtcdClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EtcdCluster{}, &EtcdClusterList{})
}
