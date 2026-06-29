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

// AuthSpec configures in-etcd authentication.
//
// This version ships a single-user model: enabling auth provisions one etcd
// user, "root", granted etcd's built-in "root" role, and then turns on
// authentication cluster-wide. The root password is bring-your-own — supplied
// via a Secret referenced by RootCredentialsSecretRef (see that field), never
// hardcoded. Multi-user / per-tenant RBAC is out of scope here and would land
// as additional fields on this struct (e.g. a Users list).
type AuthSpec struct {
	// Enabled turns on etcd authentication. The operator provisions the
	// root user + role and runs `auth enable` once the cluster has
	// converged to a healthy quorum (see status.authEnabled). Mirrors
	// `etcdctl auth enable` and the AuthStatusResponse.Enabled field.
	//
	// Requires spec.tls.client to be set: auth credentials must not cross
	// a plaintext wire. Immutable post-create — enabling or disabling auth
	// on a live cluster mutates persisted data-store state in lockstep with
	// the operator's own client, which this version does not roll back, so
	// the field is frozen the same way spec.tls is. Delete and recreate to
	// change it.
	// +optional
	Enabled bool `json:"enabled,omitempty"`

	// RootCredentialsSecretRef references a Secret in the cluster's
	// namespace holding the etcd root user's credentials. Required when
	// Enabled is true (CEL-enforced).
	//
	// The Secret is expected to be of type kubernetes.io/basic-auth: the
	// operator reads the `password` key and provisions the etcd `root` user
	// with it. The `username` key is for consumers (e.g. a Kamaji DataStore
	// pointing at the same Secret) and must be "root" — the etcd user is
	// always root, since etcd requires a user named root to enable auth.
	//
	// Immutable post-create (part of the immutable auth subtree). The
	// operator reads the password on every dial; changing the Secret's
	// contents after auth is enabled would desync the operator from etcd —
	// in-place password rotation is not supported in this version, recreate
	// to change.
	// +optional
	RootCredentialsSecretRef *corev1.LocalObjectReference `json:"rootCredentialsSecretRef,omitempty"`
}

// Messages emitted by the spec.auth CEL XValidation rules on EtcdClusterSpec.
// The kubebuilder markers below embed these strings literally — controller-gen
// cannot reference Go constants — so they are named here as the single source
// tests assert against, instead of re-typing the literals. Keep the marker text
// and these constants in sync.
const (
	MsgAuthRequiresClientTLS      = "spec.auth.enabled requires spec.tls.client (auth credentials must not cross a plaintext connection)"
	MsgAuthRequiresCredentialsRef = "spec.auth.enabled requires spec.auth.rootCredentialsSecretRef"
	MsgAuthAddRemove              = "spec.auth cannot be added to or removed from an existing cluster"
	MsgAuthImmutable              = "spec.auth is immutable post-create"
)

// ClientTLS configures TLS for the etcd client API.
//
// Material can come from either a user-provided Secret (ServerSecretRef +
// OperatorClientSecretRef) or operator-driven cert-manager issuance
// (CertManager). Exactly one source must be set per ClientTLS subtree —
// enforced by CEL.
//
// Modes (BYO):
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
//
// Modes (CertManager): the operator emits cert-manager.io/v1 Certificate
// resources at reconcile time, the Issuer signs them, the resulting
// Secrets have the same kubernetes.io/tls shape as BYO and the rest of
// the wiring is identical. See ClientCertManagerTLS for the details.
//
// +kubebuilder:validation:XValidation:rule="has(self.serverSecretRef) != has(self.certManager)",message="exactly one of spec.tls.client.serverSecretRef or spec.tls.client.certManager must be set"
// +kubebuilder:validation:XValidation:rule="!has(self.certManager) || !has(self.operatorClientSecretRef)",message="spec.tls.client.operatorClientSecretRef cannot be combined with certManager; use certManager.operatorClientIssuerRef"
type ClientTLS struct {
	// ServerSecretRef points at a Secret in the cluster's namespace
	// holding the etcd server cert in the standard kubernetes.io/tls
	// shape: tls.crt, tls.key, and ca.crt. ca.crt is always required
	// (the operator's own etcd client needs it to verify the server,
	// and when mTLS is on it doubles as --trusted-ca-file).
	//
	// Mutually exclusive with CertManager.
	// +optional
	ServerSecretRef *corev1.LocalObjectReference `json:"serverSecretRef,omitempty"`

	// OperatorClientSecretRef points at a Secret in the cluster's
	// namespace holding the operator's etcd-client identity (tls.crt,
	// tls.key). Setting this enables mTLS — etcd is started with
	// --client-cert-auth=true and the operator presents this cert when
	// dialing. Leaving it unset selects server-TLS-only mode.
	//
	// Cannot be combined with CertManager; use
	// CertManager.OperatorClientIssuerRef instead.
	// +optional
	OperatorClientSecretRef *corev1.LocalObjectReference `json:"operatorClientSecretRef,omitempty"`

	// CertManager configures operator-driven TLS material provisioning
	// via cert-manager.io/v1 Certificate resources. Mutually exclusive
	// with ServerSecretRef. The operator owns the emitted Certificates
	// (they GC with the EtcdCluster) and the resulting Secrets are
	// mounted into the etcd Pods the same way BYO Secrets are.
	// +optional
	CertManager *ClientCertManagerTLS `json:"certManager,omitempty"`
}

// ClientCertManagerTLS configures operator-driven TLS for the client API
// via cert-manager.io/v1 Certificate resources.
type ClientCertManagerTLS struct {
	// ServerIssuerRef is the Issuer or ClusterIssuer that will sign the
	// etcd server cert. Resulting Secret name is
	// "<cluster>-server-tls".
	ServerIssuerRef IssuerReference `json:"serverIssuerRef"`

	// OperatorClientIssuerRef is the Issuer or ClusterIssuer that will
	// sign the operator's etcd-client identity. Presence ⇒ client mTLS
	// is on; absence ⇒ server-TLS only. Resulting Secret name is
	// "<cluster>-operator-client-tls".
	//
	// In the happy path the same Issuer signs both server and operator-
	// client certs, so the CA visible in each Secret's ca.crt is the
	// same content, doubling as etcd's --trusted-ca-file. Splitting the
	// two Issuers across separate root CAs requires the user to ensure
	// the trust bundle on the server side covers both — that case is an
	// edge of this v1.
	// +optional
	OperatorClientIssuerRef *IssuerReference `json:"operatorClientIssuerRef,omitempty"`
}

// PeerTLS configures TLS for the etcd peer API. When PeerTLS is set, peer
// is always mTLS.
//
// +kubebuilder:validation:XValidation:rule="has(self.secretRef) != has(self.certManager)",message="exactly one of spec.tls.peer.secretRef or spec.tls.peer.certManager must be set"
type PeerTLS struct {
	// SecretRef points at a Secret in the cluster's namespace holding
	// the peer cert+key in the standard kubernetes.io/tls shape:
	// tls.crt, tls.key, ca.crt. ca.crt is required (peer is symmetric
	// — same cert is used to serve inbound and dial outbound peer
	// connections — and --peer-trusted-ca-file is always populated).
	// The peer cert MUST carry both serverAuth and clientAuth in EKU.
	//
	// Mutually exclusive with CertManager.
	// +optional
	SecretRef *corev1.LocalObjectReference `json:"secretRef,omitempty"`

	// CertManager configures operator-driven TLS material provisioning
	// for the peer plane via cert-manager.io/v1 Certificate resources.
	// Mutually exclusive with SecretRef.
	// +optional
	CertManager *PeerCertManagerTLS `json:"certManager,omitempty"`
}

// PeerCertManagerTLS configures operator-driven TLS for the peer API.
type PeerCertManagerTLS struct {
	// IssuerRef is the Issuer or ClusterIssuer that signs the peer cert.
	// Peer is symmetric (same cert serves and dials), so this single
	// Issuer covers both directions of peer mTLS. Resulting Secret name
	// is "<cluster>-peer-tls".
	IssuerRef IssuerReference `json:"issuerRef"`
}

// IssuerReference points at a cert-manager Issuer or ClusterIssuer in the
// cluster's namespace (Issuer) or cluster-wide (ClusterIssuer).
type IssuerReference struct {
	// Name is the issuer resource's name.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Kind is either "Issuer" or "ClusterIssuer". Defaults to "Issuer"
	// — a namespaced issuer living next to the EtcdCluster.
	// +kubebuilder:validation:Enum=Issuer;ClusterIssuer
	// +kubebuilder:default=Issuer
	// +optional
	Kind string `json:"kind,omitempty"`
}

// AutoCompactionMode selects etcd's auto-compaction interpretation of
// the retention value: "periodic" treats it as a time duration,
// "revision" as a revision-count delta.
//
// +kubebuilder:validation:Enum=periodic;revision
type AutoCompactionMode string

const (
	// AutoCompactionModePeriodic compacts by time window (--auto-compaction-mode=periodic).
	AutoCompactionModePeriodic AutoCompactionMode = "periodic"
	// AutoCompactionModeRevision compacts by revision delta (--auto-compaction-mode=revision).
	AutoCompactionModeRevision AutoCompactionMode = "revision"
)

// EtcdOptions carries the etcd server tuning flags the operator passes to
// each member's `etcd` command line. Deliberately a closed, typed set — the
// legacy aenix operator exposed a free-form `spec.options` map[string]string,
// which let users inject arbitrary (and operator-conflicting) flags; this
// port types exactly the keys Cozystack's etcd package actually used. New
// flags land here as new typed fields, not as an escape hatch.
//
// Like spec.resources, options are latched through status.observed and take
// effect on newly-created members (scale-up, replacement) only; the operator
// does not roll existing Pods to apply a tuning change in place. Delete one
// Pod at a time to re-template members, or recreate the cluster.
type EtcdOptions struct {
	// QuotaBackendBytes sets --quota-backend-bytes: the backend database
	// size limit in bytes before the member raises the cluster-wide
	// NOSPACE alarm. 0 or absent means etcd's built-in default (2GiB).
	// etcd's documented practical maximum is 8GiB.
	// +kubebuilder:validation:Minimum=0
	// +optional
	QuotaBackendBytes *int64 `json:"quotaBackendBytes,omitempty"`

	// AutoCompactionMode sets --auto-compaction-mode: how
	// AutoCompactionRetention is interpreted, "periodic" (time-based)
	// or "revision" (revision-count-based). Absent means etcd's default
	// ("periodic" — though compaction only activates when a retention
	// is set).
	// +optional
	AutoCompactionMode AutoCompactionMode `json:"autoCompactionMode,omitempty"`

	// AutoCompactionRetention sets --auto-compaction-retention. In
	// periodic mode a duration ("5m", "1h"; a bare integer means hours);
	// in revision mode a revision count. Absent or "0" disables
	// auto-compaction. The pattern admits what etcd itself parses: a
	// bare non-negative integer or a Go duration.
	// +kubebuilder:validation:Pattern=`^([0-9]+(\.[0-9]+)?(ms|s|m|h))+$|^[0-9]+$`
	// +optional
	AutoCompactionRetention string `json:"autoCompactionRetention,omitempty"`

	// SnapshotCount sets --snapshot-count: the number of committed
	// raft entries to retain in memory before triggering an internal
	// raft snapshot (this is unrelated to EtcdSnapshot backups). Absent
	// means etcd's built-in default. Lower values trade replay speed on
	// restart for a smaller memory footprint.
	// +kubebuilder:validation:Minimum=1
	// +optional
	SnapshotCount *int64 `json:"snapshotCount,omitempty"`
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

// BootstrapSpec configures one-time cluster initialization. Consulted only
// at first bootstrap; immutable post-create.
type BootstrapSpec struct {
	// Restore initializes the new cluster from an existing etcd snapshot
	// instead of an empty data dir. Absent means a normal empty bootstrap.
	// +optional
	Restore *RestoreSpec `json:"restore,omitempty"`
}

// RestoreSpec points at the snapshot a new cluster is restored from.
//
// Unlike a snapshot destination, a restore SOURCE addresses a single existing
// object: when the source is S3 the key must be the exact object key, not a
// prefix; when the source is a PVC the subPath must be the exact snapshot file
// within the volume. An empty locator would only surface as an opaque failure
// inside the seed init container, so reject it at the apiserver.
//
// +kubebuilder:validation:XValidation:rule="!has(self.source.s3) || (has(self.source.s3.key) && size(self.source.s3.key) > 0)",message="bootstrap.restore.source.s3.key must be the exact (non-empty) object key for a restore source"
// +kubebuilder:validation:XValidation:rule="!has(self.source.pvc) || (has(self.source.pvc.subPath) && size(self.source.pvc.subPath) > 0)",message="bootstrap.restore.source.pvc.subPath must be the exact (non-empty) snapshot file path for a restore source"
type RestoreSpec struct {
	// Source is where the snapshot is read from (S3 or PVC). Same shape as
	// an EtcdSnapshot destination.
	Source SnapshotLocation `json:"source"`
}

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
// https://github.com/cozystack/etcd-operator/issues/16. The
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
// +kubebuilder:validation:XValidation:rule="has(self.auth) == has(oldSelf.auth)",message="spec.auth cannot be added to or removed from an existing cluster; delete and recreate"
// +kubebuilder:validation:XValidation:rule="!has(self.auth) || !has(oldSelf.auth) || self.auth == oldSelf.auth",message="spec.auth is immutable post-create; delete and recreate the cluster to change auth configuration"
// +kubebuilder:validation:XValidation:rule="!(has(self.auth) && self.auth.enabled) || (has(self.tls) && has(self.tls.client))",message="spec.auth.enabled requires spec.tls.client (auth credentials must not cross a plaintext connection)"
// +kubebuilder:validation:XValidation:rule="!(has(self.auth) && self.auth.enabled) || has(self.auth.rootCredentialsSecretRef)",message="spec.auth.enabled requires spec.auth.rootCredentialsSecretRef"
// +kubebuilder:validation:XValidation:rule="has(self.bootstrap) == has(oldSelf.bootstrap)",message="spec.bootstrap cannot be added to or removed from an existing cluster; it is consulted only at first bootstrap"
// +kubebuilder:validation:XValidation:rule="!has(self.bootstrap) || !has(oldSelf.bootstrap) || self.bootstrap == oldSelf.bootstrap",message="spec.bootstrap is immutable post-create; it is consulted only at first bootstrap"
// +kubebuilder:validation:XValidation:rule="!(has(self.bootstrap) && has(self.bootstrap.restore)) || !(has(self.storage) && has(self.storage.medium) && self.storage.medium == 'Memory')",message="spec.bootstrap.restore is unsupported with spec.storage.medium=Memory: the restored data dir is tmpfs, so any seed Pod restart re-restores the snapshot — reverting writes (single member) or breaking the cluster with a fresh ID it can't rejoin (multi-member). Use persistent storage to restore."
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

	// Version is the desired etcd version (e.g. "3.6.11").
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

	// Auth configures in-etcd authentication. Absent means no auth
	// (anonymous access on the client API, subject only to TLS). See
	// AuthSpec for the single-user parity model and its constraints
	// (requires spec.tls.client; immutable post-create).
	// +optional
	Auth *AuthSpec `json:"auth,omitempty"`

	// Bootstrap configures one-time cluster initialization options. It is
	// consulted only at first bootstrap (while status.clusterID is unset)
	// and is immutable post-create. Absent means a normal empty-cluster
	// bootstrap.
	// +optional
	Bootstrap *BootstrapSpec `json:"bootstrap,omitempty"`

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

	// AdditionalMetadata holds extra labels and annotations the operator
	// merges onto every object it creates for this cluster — member Pods,
	// the per-member data PVCs, the client and headless Services, the
	// PodDisruptionBudget, and the EtcdMember CRs. Operator-owned keys
	// (the app.kubernetes.io/* set and the cluster/role labels, and any
	// operator-set annotation) always win on collision, so this cannot be
	// used to shadow the operator's own metadata.
	//
	// Like spec.resources, changes take effect on newly-created objects
	// (scale-up, replacement); the operator does not re-stamp existing
	// Pods in place. The value is latched through status.observed with the
	// rest of the target spec, so a mid-flight edit only applies once the
	// current target is reached.
	// +optional
	AdditionalMetadata *AdditionalMetadata `json:"additionalMetadata,omitempty"`

	// Affinity sets the scheduling affinity/anti-affinity for member Pods.
	// Passed straight through to each member Pod's spec.affinity. A common
	// use is a pod anti-affinity on app.kubernetes.io/instance=<cluster> to
	// keep members off the same node.
	//
	// Updates take effect on newly-created members (scale-up, replacement);
	// the operator does not roll existing Pods to apply an affinity change
	// in place.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// TopologySpreadConstraints controls how member Pods are spread across
	// topology domains (zones, nodes). Passed straight through to each
	// member Pod's spec.topologySpreadConstraints.
	//
	// Updates take effect on newly-created members (scale-up, replacement);
	// the operator does not roll existing Pods to apply a change in place.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// Options carries etcd server tuning flags (backend quota,
	// auto-compaction, raft snapshot count) passed to each member's
	// command line. A closed typed set — see EtcdOptions for why there
	// is deliberately no free-form flag map.
	//
	// Updates take effect on newly-created members (scale-up,
	// replacement); the operator does not roll existing Pods to apply a
	// tuning change in place.
	// +optional
	Options *EtcdOptions `json:"options,omitempty"`

	// ImagePullSecrets is a list of Secret references in the cluster's
	// namespace used to pull the etcd (and restore initContainer) image from
	// a private registry — e.g. an air-gapped mirror behind credentials.
	// Passed straight through to each member Pod's spec.imagePullSecrets.
	//
	// Changes take effect on newly-created members (scale-up, replacement);
	// the operator does not roll existing Pods. Latched through
	// status.observed.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

// AdditionalMetadata is a set of labels and annotations the operator merges
// onto every resource it creates for a cluster. See
// EtcdClusterSpec.AdditionalMetadata for the precedence and timing rules.
type AdditionalMetadata struct {
	// Labels are extra labels merged onto created objects. Operator-owned
	// label keys take precedence on collision.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`

	// Annotations are extra annotations merged onto created objects.
	// +optional
	Annotations map[string]string `json:"annotations,omitempty"`
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

	// Affinity is the locked target scheduling affinity for member Pods.
	// Latched alongside the rest of the target spec so a mid-flight change
	// only applies to members created once the current target is reached.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// TopologySpreadConstraints is the locked target spread configuration
	// for member Pods. Latched with the rest of the target spec.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// AdditionalMetadata is the locked target extra labels/annotations
	// stamped onto objects created for this cluster. Latched with the rest
	// of the target spec so a mid-flight metadata edit only applies to
	// objects created once the current target is reached.
	// +optional
	AdditionalMetadata *AdditionalMetadata `json:"additionalMetadata,omitempty"`

	// Options is the locked target etcd tuning flags for member Pods.
	// Latched with the rest of the target spec so a mid-flight tuning
	// edit only applies to members created once the current target is
	// reached.
	// +optional
	Options *EtcdOptions `json:"options,omitempty"`

	// ImagePullSecrets is the locked target pull-secret list for member
	// Pods. Latched with the rest of the target spec.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`
}

// EtcdClusterStatus defines the observed state of an etcd cluster.
type EtcdClusterStatus struct {
	// ReadyMembers is the count of members that are healthy and serving.
	// Also exposed as Scale.Status.Replicas via the /scale subresource so
	// kubectl scale and clients like VerticalPodAutoscaler can read
	// "current scale" without a custom status field.
	ReadyMembers int32 `json:"readyMembers,omitempty"`

	// Selector is the label-selector form ("etcd-operator.cozystack.io/cluster=<name>")
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

	// AuthEnabled is true once the operator has successfully run
	// `auth enable` against the cluster. It is latched (never cleared —
	// spec.auth.enabled is immutable) and is the single signal every
	// operator etcd dial consults to decide whether to present the root
	// credentials: false ⇒ dial anonymously (auth not yet on, e.g. during
	// the bootstrap window before the cluster has converged), true ⇒ dial
	// as root. Decoupling this from spec.auth.enabled is what makes
	// the bootstrap window correct — clientv3 attempts an Authenticate RPC
	// on connect when a username is set, which fails until auth is enabled.
	// +optional
	AuthEnabled bool `json:"authEnabled,omitempty"`

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
