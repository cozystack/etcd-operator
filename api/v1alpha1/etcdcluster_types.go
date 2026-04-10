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
	"k8s.io/apimachinery/pkg/util/intstr"
)

const DefaultEtcdImage = "quay.io/coreos/etcd:v3.5.12"

// EtcdClusterSpec defines the desired state of EtcdCluster
type EtcdClusterSpec struct {
	// Replicas is the count of etcd instances in cluster.
	// +optional
	// +kubebuilder:default:=3
	// +kubebuilder:validation:Minimum:=0
	Replicas *int32 `json:"replicas,omitempty"`
	// Options are the extra arguments to pass to the etcd container.
	// +optional
	// +kubebuilder:example:={enable-v2: "false", debug: "true"}
	Options map[string]string `json:"options,omitempty"`
	// PodTemplate defines the desired state of PodSpec for etcd members. If not specified, default values will be used.
	PodTemplate PodTemplate `json:"podTemplate,omitempty"`
	// Service defines the desired state of Service for etcd members. If not specified, default values will be used.
	// +optional
	ServiceTemplate *EmbeddedService `json:"serviceTemplate,omitempty"`
	// HeadlessService defines the desired state of HeadlessService for etcd members. If not specified, default values will be used.
	// +optional
	HeadlessServiceTemplate *EmbeddedMetadataResource `json:"headlessServiceTemplate,omitempty"`
	// PodDisruptionBudgetTemplate describes PDB resource to create for etcd cluster members. Nil to disable.
	// +optional
	PodDisruptionBudgetTemplate *EmbeddedPodDisruptionBudget `json:"podDisruptionBudgetTemplate,omitempty"`
	Storage                     StorageSpec                  `json:"storage"`
	// Security describes security settings of etcd (authentication, certificates, rbac)
	// +optional
	Security *SecuritySpec `json:"security,omitempty"`
	// Bootstrap defines initialization from an existing data source (e.g., snapshot restore).
	// This is only used during initial cluster creation and is ignored afterward.
	// +optional
	Bootstrap *BootstrapSpec `json:"bootstrap,omitempty"`
}

// BootstrapSpec defines how to initialize a new EtcdCluster from an existing data source.
type BootstrapSpec struct {
	// Restore configures cluster initialization from an etcd snapshot.
	// +optional
	Restore *RestoreSpec `json:"restore,omitempty"`
}

// RestoreSpec defines how to restore a cluster from a snapshot.
type RestoreSpec struct {
	// Source defines where to get the snapshot for restoration.
	Source BackupDestination `json:"source"`
}

const (
	EtcdConditionInitialized = "Initialized"
	EtcdConditionReady       = "Ready"
	EtcdConditionError       = "Error"
)

type EtcdCondType string
type EtcdCondMessage string

const (
	EtcdCondTypeInitStarted           EtcdCondType = "InitializationStarted"
	EtcdCondTypeInitComplete          EtcdCondType = "InitializationComplete"
	EtcdCondTypeWaitingForFirstQuorum EtcdCondType = "WaitingForFirstQuorum"
	EtcdCondTypeStatefulSetReady      EtcdCondType = "StatefulSetReady"
	EtcdCondTypeStatefulSetNotReady   EtcdCondType = "StatefulSetNotReady"
	EtcdCondTypeSplitbrain            EtcdCondType = "Splitbrain"
)

const (
	EtcdInitCondNegMessage           EtcdCondMessage = "Cluster initialization started"
	EtcdInitCondPosMessage           EtcdCondMessage = "Cluster managed resources created"
	EtcdReadyCondNegMessage          EtcdCondMessage = "Cluster StatefulSet is not Ready"
	EtcdReadyCondPosMessage          EtcdCondMessage = "Cluster StatefulSet is Ready"
	EtcdReadyCondNegWaitingForQuorum EtcdCondMessage = "Waiting for first quorum to be established"
	EtcdErrorCondSplitbrainMessage   EtcdCondMessage = "Etcd endpoints reporting more than one unique cluster ID"
)

// EtcdClusterStatus defines the observed state of EtcdCluster
type EtcdClusterStatus struct {
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// EtcdCluster is the Schema for the etcdclusters API
type EtcdCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   EtcdClusterSpec   `json:"spec,omitempty"`
	Status EtcdClusterStatus `json:"status,omitempty"`
}

// CalculateQuorumSize returns minimum quorum size for current number of replicas
func (r *EtcdCluster) CalculateQuorumSize() int {
	return int(*r.Spec.Replicas)/2 + 1
}

func (c *EtcdCluster) IsClientSecurityEnabled() bool {
	return c.Spec.Security != nil && c.Spec.Security.TLS.ClientSecret != ""
}

func (c *EtcdCluster) IsServerSecurityEnabled() bool {
	return c.Spec.Security != nil && c.Spec.Security.TLS.ServerSecret != ""
}

func (c *EtcdCluster) IsServerTrustedCADefined() bool {
	return c.Spec.Security != nil && c.Spec.Security.TLS.ServerTrustedCASecret != ""
}

// +kubebuilder:object:root=true

// EtcdClusterList contains a list of EtcdCluster
type EtcdClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []EtcdCluster `json:"items"`
}

// EmbeddedObjectMetadata contains a subset of the fields included in k8s.io/apimachinery/pkg/apis/meta/v1.ObjectMeta
// Only fields which are relevant to embedded resources are included.
type EmbeddedObjectMetadata struct {
	// Name must be unique within a namespace. Is required when creating resources, although
	// some resources may allow a client to request the generation of an appropriate name
	// automatically. Name is primarily intended for creation idempotence and configuration
	// definition.
	// Cannot be updated.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	// Labels Map of string keys and values that can be used to organize and categorize
	// (scope and select) objects. May match selectors of replication controllers
	// and services.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/labels
	// +optional
	Labels map[string]string `json:"labels,omitempty" protobuf:"bytes,11,rep,name=labels"`

	// Annotations is an unstructured key value map stored with a resource that may be
	// set by external tools to store and retrieve arbitrary metadata. They are not
	// queryable and should be preserved when modifying objects.
	// More info: https://kubernetes.io/docs/concepts/overview/working-with-objects/annotations
	// +optional
	Annotations map[string]string `json:"annotations,omitempty" protobuf:"bytes,12,rep,name=annotations"`
}

// ToObjectMeta converts EmbeddedObjectMetadata to metav1.ObjectMeta
func (r *EmbeddedObjectMetadata) ToObjectMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:        r.Name,
		Labels:      r.Labels,
		Annotations: r.Annotations,
	}
}

// PodTemplate allows overrides, such as sidecars, init containers, changes to the security context, etc to the pod template generated by the operator.
type PodTemplate struct {
	// EmbeddedObjectMetadata contains metadata relevant to an EmbeddedResource
	// +optional
	EmbeddedObjectMetadata `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec follows the structure of a regular Pod spec. Overrides defined here will be strategically merged with the default pod spec, generated by the operator.
	// +optional
	// +kubebuilder:pruning:PreserveUnknownFields
	Spec corev1.PodSpec `json:"spec,omitempty"`
}

// StorageSpec defines the configured storage for a etcd members.
// If neither `emptyDir` nor `volumeClaimTemplate` is specified, then by default an [EmptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) will be used.
// +k8s:openapi-gen=true
type StorageSpec struct {
	// EmptyDirVolumeSource to be used by the StatefulSets. If specified, used in place of any volumeClaimTemplate. More
	// info: https://kubernetes.io/docs/concepts/storage/volumes/#emptydir
	// +optional
	EmptyDir *corev1.EmptyDirVolumeSource `json:"emptyDir,omitempty"`
	// A PVC spec to be used by the StatefulSets.
	// +optional
	VolumeClaimTemplate EmbeddedPersistentVolumeClaim `json:"volumeClaimTemplate,omitempty"`
}

// SecuritySpec defines security settings for etcd.
// +k8s:openapi-gen=true
type SecuritySpec struct {
	// Section for user-managed tls certificates
	// +optional
	TLS TLSSpec `json:"tls,omitempty"`
	// Section to enable etcd auth
	EnableAuth bool `json:"enableAuth,omitempty"`
}

// TLSSpec defines user-managed certificates names.
type TLSSpec struct {
	// Trusted CA certificate secret to secure peer-to-peer communication between etcd nodes. It is expected to have ca.crt field in the secret.
	// This secret must be created in the namespace with etcdCluster CR.
	// +optional
	PeerTrustedCASecret string `json:"peerTrustedCASecret,omitempty"`
	// Certificate secret to secure peer-to-peer communication between etcd nodes. It is expected to have tls.crt and tls.key fields in the secret.
	// This secret must be created in the namespace with etcdCluster CR.
	// +optional
	PeerSecret string `json:"peerSecret,omitempty"`
	// Trusted CA for etcd server certificates for client-server communication. Is necessary to set trust between operator and etcd.
	// It is expected to have ca.crt field in the secret. If it is not specified, then insecure communication will be used.
	// This secret must be created in the namespace with etcdCluster CR.
	// +optional
	ServerTrustedCASecret string `json:"serverTrustedCASecret,omitempty"`
	// Server certificate secret to secure client-server communication. Is provided to the client who connects to etcd by client port (2379 by default).
	// It is expected to have tls.crt and tls.key fields in the secret.
	// This secret must be created in the namespace with etcdCluster CR.
	// +optional
	ServerSecret string `json:"serverSecret,omitempty"`
	// Trusted CA for client certificates that are provided by client to etcd. It is expected to have ca.crt field in the secret.
	// This secret must be created in the namespace with etcdCluster CR.
	// +optional
	ClientTrustedCASecret string `json:"clientTrustedCASecret,omitempty"`
	// Client certificate for etcd-operator to do maintenance. It is expected to have tls.crt and tls.key fields in the secret.
	// This secret must be created in the namespace with etcdCluster CR.
	// +optional
	ClientSecret string `json:"clientSecret,omitempty"`
}

// EmbeddedPersistentVolumeClaim is an embedded version of k8s.io/api/core/v1.PersistentVolumeClaim.
// It contains TypeMeta and a reduced ObjectMeta.
type EmbeddedPersistentVolumeClaim struct {
	metav1.TypeMeta `json:",inline"`

	// EmbeddedMetadata contains metadata relevant to an EmbeddedResource.
	// +optional
	EmbeddedObjectMetadata `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec defines the desired characteristics of a volume requested by a pod author.
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	// +optional
	Spec corev1.PersistentVolumeClaimSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status represents the current information/status of a persistent volume claim.
	// Read-only.
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	// +optional
	Status corev1.PersistentVolumeClaimStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// EmbeddedPodDisruptionBudget describes PDB resource for etcd cluster members
type EmbeddedPodDisruptionBudget struct {
	// EmbeddedMetadata contains metadata relevant to an EmbeddedResource.
	// +optional
	EmbeddedObjectMetadata `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Spec defines the desired characteristics of a PDB.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/disruptions/#pod-disruption-budgets
	// +optional
	Spec PodDisruptionBudgetSpec `json:"spec"`
}

type PodDisruptionBudgetSpec struct {
	// MinAvailable describes minimum ready replicas. If both are empty, controller will implicitly
	// calculate MaxUnavailable based on number of replicas
	// Mutually exclusive with MaxUnavailable.
	// +optional
	MinAvailable *intstr.IntOrString `json:"minAvailable,omitempty"`
	// MinAvailable describes maximum not ready replicas. If both are empty, controller will implicitly
	// calculate MaxUnavailable based on number of replicas
	// Mutually exclusive with MinAvailable
	// +optional
	MaxUnavailable *intstr.IntOrString `json:"maxUnavailable,omitempty"`
}

type EmbeddedService struct {
	// EmbeddedMetadata contains metadata relevant to an EmbeddedResource.
	// +optional
	EmbeddedObjectMetadata `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// Spec defines the behavior of the service.
	// +optional
	Spec corev1.ServiceSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

type EmbeddedMetadataResource struct {
	// EmbeddedMetadata contains metadata relevant to an EmbeddedResource.
	// +optional
	EmbeddedObjectMetadata `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
}

func init() {
	SchemeBuilder.Register(&EtcdCluster{}, &EtcdClusterList{})
}
