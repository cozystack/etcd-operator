package controllers

import (
	"fmt"
	"path"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	// LabelCluster is the label key used to associate resources with an EtcdCluster.
	LabelCluster = "etcd-operator.cozystack.io/cluster"

	// LabelRole identifies the etcd-side raft role of a member's Pod. The
	// only value the operator emits today is RoleVoter; learners carry no
	// LabelRole at all so the per-cluster PodDisruptionBudget can select
	// voters exclusively (its selector requires LabelRole=RoleVoter).
	LabelRole = "etcd-operator.cozystack.io/role"
	RoleVoter = "voter"

	// EtcdImage is the container image repository for etcd.
	EtcdImage = "quay.io/coreos/etcd"

	// MemberFinalizer is placed on EtcdMember resources to ensure
	// graceful removal from the etcd cluster before deletion.
	MemberFinalizer = "etcd-operator.cozystack.io/member-cleanup"

	// ReservedAnnotationPrefix namespaces the operator-interpreted
	// annotations below. additionalMetadata must never be able to set a
	// key under this prefix (applyAdditionalMetadata strips it): the
	// reserved annotations drive member DNS identity and the --data-dir
	// path, so a user-settable copy would (a) make every operator-created
	// member inherit a migration knob — breaking the self-wipe — and (b)
	// turn data-dir-subpath into a user-controllable path into --data-dir.
	ReservedAnnotationPrefix = "etcd-operator.cozystack.io/"

	// AnnHeadlessServiceName overrides the headless Service name a member's
	// DNS identity keys off: its Pod subdomain and every peer/client URL
	// the operator constructs for it. Absent ⇒ the cluster's own name
	// (native behaviour). Stamped ONLY by the in-place migration tool on
	// the EtcdMembers it creates for adopted pods (whose immutable
	// spec.subdomain and persisted peer URLs use the legacy Service name);
	// the operator never stamps it, so every rolled/replaced member comes
	// up native and the override self-wipes once the cluster fully rolls.
	AnnHeadlessServiceName = ReservedAnnotationPrefix + "headless-service-name"

	// AnnDataDirSubPath relocates etcd's --data-dir to a subdirectory of
	// the member's data volume (/var/lib/etcd/<subpath>). Absent ⇒ the
	// volume root. Same migration-only contract as AnnHeadlessServiceName:
	// the legacy operator kept etcd data under "default.etcd" inside the
	// PVC, so an adopted member's replacement Pod finds the existing data
	// dir instead of bootstrapping empty. The value is validated in code
	// (validDataDirSubPath) — an annotation has no apiserver schema, so the
	// controller fails closed against a mount-escaping value.
	AnnDataDirSubPath = ReservedAnnotationPrefix + "data-dir-subpath"

	// AnnPeerAutoTLS, set to "true" on an EtcdCluster, runs the peer plane
	// with etcd's --peer-auto-tls: per-member self-signed certs, NO shared
	// CA, so peer traffic is encrypted but NOT authenticated. This is a
	// migration-only knob etcd-migrate stamps when adopting a legacy cluster
	// that ran the previous operator's unconditional --peer-auto-tls default
	// (no CA exists to do real mTLS, so a strict-mTLS replacement could never
	// rejoin the still-auto-tls members). Unlike AnnHeadlessServiceName /
	// AnnDataDirSubPath it is cluster-level and does NOT self-wipe: the
	// controller propagates it to every member it builds so replacement/
	// scaled members keep interoperating. Deliberately NOT a typed spec field
	// — an unauthenticated peer plane must not be a discoverable, CEL-blessed
	// option for new clusters; an undocumented reserved key is the lesser
	// footgun. Superseded by an explicit spec.tls.peer.secretRef/certManager
	// (real mTLS wins; precedence lives in clusterPeerAutoTLS).
	AnnPeerAutoTLS = ReservedAnnotationPrefix + "peer-auto-tls"
)

// etcdDataDirRoot is the mount path of every member's data volume; --data-dir
// is this root or, for adopted members, a validated subdirectory of it.
const etcdDataDirRoot = "/var/lib/etcd"

// dataDirSubPathRe is the same pattern the removed spec.dataDirSubPath field
// carried as an apiserver-enforced kubebuilder marker. Now that the value
// arrives as an unvalidated annotation, the controller enforces it here.
var dataDirSubPathRe = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]*$`)

// validDataDirSubPath fails closed: the value must be a single safe path
// component — no slash (so it cannot name a nested path) and no ".." (so it
// cannot escape the mount) — matching the original field's pattern. Anything
// else is rejected and the caller falls back to the native data-dir root.
func validDataDirSubPath(s string) bool {
	if strings.ContainsRune(s, '/') || strings.Contains(s, "..") {
		return false
	}
	return dataDirSubPathRe.MatchString(s)
}

// memberDataDir resolves a member's etcd --data-dir from its
// AnnDataDirSubPath annotation, falling back to the volume root when the
// annotation is absent or fails validation (fail-closed: an invalid value is
// ignored, never substituted into the path).
func memberDataDir(member *lll.EtcdMember) string {
	sub := member.Annotations[AnnDataDirSubPath]
	if sub == "" || !validDataDirSubPath(sub) {
		return etcdDataDirRoot
	}
	return path.Join(etcdDataDirRoot, sub)
}

// peerURL returns the etcd peer URL for a member, using the headless Service
// DNS. `service` is the headless Service name the member resolves under —
// resolve it per-member via memberServiceName (the cluster's own name by
// default, or the AnnHeadlessServiceName override an adopted member carries),
// never assume cluster.Name for every member: during an in-place migration
// adopted and rolled members live under different Service names at once.
// scheme is "http" or "https" depending on whether peer TLS is enabled.
func peerURL(scheme, member, service, namespace string) string {
	return fmt.Sprintf("%s://%s.%s.%s.svc:2380", scheme, member, service, namespace)
}

// clientURL returns the etcd client URL for a member. Same service-name
// contract as peerURL.
// scheme is "http" or "https" depending on whether client TLS is enabled.
func clientURL(scheme, member, service, namespace string) string {
	return fmt.Sprintf("%s://%s.%s.%s.svc:2379", scheme, member, service, namespace)
}

// memberServiceName resolves the headless Service name a member resolves
// under: the AnnHeadlessServiceName annotation when present (set only by the
// migration tool on adopted members), the owning cluster's own name
// otherwise. Every constructed member URL and the Pod's spec.subdomain key
// off this. The operator never stamps the annotation on members it creates,
// so a rolled/replaced member defaults to the cluster name and the override
// self-wipes as the cluster rolls — there is deliberately no cluster-level
// equivalent (the operator's native headless Service is always cluster.Name).
func memberServiceName(member *lll.EtcdMember) string {
	if v := member.Annotations[AnnHeadlessServiceName]; v != "" {
		return v
	}
	return member.Spec.ClusterName
}

// clusterClientScheme returns "https" when the cluster has client TLS
// configured, "http" otherwise. The operator's etcd client and the
// `--advertise-client-urls` values both key off this.
func clusterClientScheme(cluster *lll.EtcdCluster) string {
	if cluster != nil && cluster.Spec.TLS != nil && cluster.Spec.TLS.Client != nil {
		return "https"
	}
	return "http"
}

// clusterPeerScheme returns "https" when the cluster has peer TLS configured,
// "http" otherwise. The legacy-compat --peer-auto-tls mode (carried on the
// AnnPeerAutoTLS annotation, no typed spec.tls.peer) also serves peer over
// https, so it counts too.
func clusterPeerScheme(cluster *lll.EtcdCluster) string {
	if cluster != nil && cluster.Spec.TLS != nil && cluster.Spec.TLS.Peer != nil {
		return "https"
	}
	if clusterPeerAutoTLS(cluster) {
		return "https"
	}
	return "http"
}

// clusterPeerAutoTLS reports whether the cluster runs the legacy-compat
// --peer-auto-tls peer mode, carried on the reserved AnnPeerAutoTLS annotation
// (see its doc). An explicit typed peer TLS mode (secretRef/certManager) always
// wins, so the annotation is honoured only when spec.tls.peer is unset.
func clusterPeerAutoTLS(cluster *lll.EtcdCluster) bool {
	if cluster == nil {
		return false
	}
	if cluster.Spec.TLS != nil && cluster.Spec.TLS.Peer != nil {
		return false
	}
	return cluster.Annotations[AnnPeerAutoTLS] == "true"
}

// memberClientScheme is the per-member counterpart to clusterClientScheme,
// keyed off the propagated EtcdMemberSpec.TLS.
func memberClientScheme(member *lll.EtcdMember) string {
	if member != nil && member.Spec.TLS != nil && member.Spec.TLS.ClientServerSecretRef != nil {
		return "https"
	}
	return "http"
}

// memberPeerScheme is the per-member counterpart to clusterPeerScheme.
func memberPeerScheme(member *lll.EtcdMember) string {
	if member != nil && member.Spec.TLS != nil &&
		(member.Spec.TLS.PeerSecretRef != nil || member.Spec.TLS.PeerAutoTLS) {
		return "https"
	}
	return "http"
}

// buildInitialCluster builds the --initial-cluster flag value from member names.
func buildInitialCluster(peerScheme string, names []string, service, namespace string) string {
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = name + "=" + peerURL(peerScheme, name, service, namespace)
	}
	return strings.Join(parts, ",")
}

// deriveMemberTLS produces the per-member TLS view from a cluster's TLS
// spec. Mirrors only the secret references plus the mTLS flag; the operator-
// side operator-client secret stays on the parent cluster.
//
// Both BYO (Secret refs) and cert-manager (operator-emitted Certificate)
// modes are flattened here: the member-side fields hold the resolved
// Secret names regardless of source, so buildPod / ensurePod /
// buildOperatorTLSConfig stay source-agnostic.
func deriveMemberTLS(cluster *lll.EtcdCluster) *lll.EtcdMemberTLS {
	if cluster == nil {
		return nil
	}
	out := &lll.EtcdMemberTLS{}
	if cluster.Spec.TLS != nil {
		if name := serverSecretName(cluster); name != "" {
			out.ClientServerSecretRef = &corev1.LocalObjectReference{Name: name}
			out.ClientMTLS = operatorClientSecretName(cluster) != ""
		}
		if name := peerSecretName(cluster); name != "" {
			out.PeerSecretRef = &corev1.LocalObjectReference{Name: name}
		}
	}
	// Carry the legacy-compat --peer-auto-tls posture (a cluster-level
	// reserved annotation, not typed spec) down to the member. clusterPeerAutoTLS
	// already yields false when an explicit peer mode is set, so real mTLS wins.
	if out.PeerSecretRef == nil && clusterPeerAutoTLS(cluster) {
		out.PeerAutoTLS = true
	}
	if out.ClientServerSecretRef == nil && out.PeerSecretRef == nil && !out.PeerAutoTLS {
		return nil
	}
	return out
}

// restoreForSeed returns the RestoreSpec the bootstrap seed should carry, or
// nil. Only the seed restores; subsequent (scale-up) members join the live
// cluster normally.
func restoreForSeed(cluster *lll.EtcdCluster) *lll.RestoreSpec {
	if cluster == nil || cluster.Spec.Bootstrap == nil {
		return nil
	}
	return cluster.Spec.Bootstrap.Restore
}

// serverSecretName resolves the Secret name holding the server cert+key,
// across the BYO and cert-manager-driven sources. Empty when the client
// plane is plaintext.
func serverSecretName(cluster *lll.EtcdCluster) string {
	if cluster == nil || cluster.Spec.TLS == nil || cluster.Spec.TLS.Client == nil {
		return ""
	}
	c := cluster.Spec.TLS.Client
	if c.ServerSecretRef != nil {
		return c.ServerSecretRef.Name
	}
	if c.CertManager != nil {
		return cluster.Name + "-server-tls"
	}
	return ""
}

// operatorClientSecretName resolves the Secret name holding the
// operator's etcd-client identity. Empty when mTLS is off (server-TLS-
// only mode).
func operatorClientSecretName(cluster *lll.EtcdCluster) string {
	if cluster == nil || cluster.Spec.TLS == nil || cluster.Spec.TLS.Client == nil {
		return ""
	}
	c := cluster.Spec.TLS.Client
	if c.OperatorClientSecretRef != nil {
		return c.OperatorClientSecretRef.Name
	}
	if c.CertManager != nil && c.CertManager.OperatorClientIssuerRef != nil {
		return cluster.Name + "-operator-client-tls"
	}
	return ""
}

// peerSecretName resolves the Secret name holding the peer cert+key.
// Empty when the peer plane is plaintext.
func peerSecretName(cluster *lll.EtcdCluster) string {
	if cluster == nil || cluster.Spec.TLS == nil || cluster.Spec.TLS.Peer == nil {
		return ""
	}
	p := cluster.Spec.TLS.Peer
	if p.SecretRef != nil {
		return p.SecretRef.Name
	}
	if p.CertManager != nil {
		return cluster.Name + "-peer-tls"
	}
	return ""
}

// memberEndpoints returns etcd client endpoints for the subset of
// `members` we can safely dial cluster-management RPCs against — i.e.
// members that are not currently learners.
//
// Why filter: etcd refuses several RPCs (MemberList, MemberAdd,
// MemberPromote, ...) on learner endpoints with
// "rpc not supported for learner". clientv3's balancer round-robins
// through whatever endpoints we hand it, so an unfiltered list lets
// reconcile calls land on the learner intermittently and fail. The
// failure is not just noisy — when no voter is reachable in the retry
// budget (5–10s context), the operator wedges: scaleUp's promote step
// can't see the cluster, allMembersReady gate never opens, and the
// pod that needs promoting never gets a memberID populated.
//
// "Ready=True" is the right proxy for "voter": the member-controller
// sets Ready=True only after MemberID is populated via discoverMemberID
// — which itself needs MemberList to succeed against a voter. So
// Ready=True transitively implies the member has been observed as a
// non-learner. Members still in the learner state (no MemberID yet,
// Ready=False) get filtered out.
//
// Falls back to the unfiltered list when no member is yet Ready, which
// covers (a) bootstrap discovery (only the seed exists, its Ready
// status doesn't matter for this dialer), and (b) a single fresh
// learner's own discoverMemberID call where the peer list is just
// "self" — letting the dialer try anyway is no worse than silently
// returning [].
//
// Each endpoint is built with the member's OWN Service name
// (memberServiceName), not a single cluster-wide name: during an in-place
// migration adopted members resolve under the legacy headless Service while
// rolled members resolve under the cluster name, so a shared `service` would
// dial the wrong DNS for half the cluster.
func memberEndpoints(scheme string, members []lll.EtcdMember, namespace string) []string {
	voters := make([]string, 0, len(members))
	for i := range members {
		if members[i].Status.IsVoter {
			voters = append(voters, clientURL(scheme, members[i].Name, memberServiceName(&members[i]), namespace))
		}
	}
	if len(voters) > 0 {
		return voters
	}
	eps := make([]string, len(members))
	for i := range members {
		eps[i] = clientURL(scheme, members[i].Name, memberServiceName(&members[i]), namespace)
	}
	return eps
}

// clusterLabels returns the standard labels for cluster-level resources.
func clusterLabels(cluster string) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "etcd",
		"app.kubernetes.io/instance":   cluster,
		"app.kubernetes.io/managed-by": "etcd-operator",
		LabelCluster:                   cluster,
	}
}

// memberLabels returns the standard labels for member-level resources.
func memberLabels(cluster, member string) map[string]string {
	l := clusterLabels(cluster)
	l["app.kubernetes.io/component"] = member
	return l
}

// MemberLabels exposes the operator's member-level label set to external
// writers that must stamp objects exactly the way the controllers expect —
// the in-place migration tool labels adopted Pods/PVCs with it so the
// headless-Service selector, the PDB selector and the /scale Selector all
// match from the moment the new operator starts.
func MemberLabels(cluster, member string) map[string]string {
	return memberLabels(cluster, member)
}

// ClusterLabels is the cluster-level counterpart of MemberLabels.
func ClusterLabels(cluster string) map[string]string {
	return clusterLabels(cluster)
}

// applyAdditionalMetadata merges the user-supplied labels/annotations from
// spec.additionalMetadata onto a child object's ObjectMeta. Operator-owned
// keys (everything already present in objLabels/objAnnotations) always win
// on collision — symmetrically for both maps — so the additional metadata
// can never shadow the operator's own selectors or annotations. The inputs
// are mutated in place (allocated when nil and there is something to merge)
// and returned, so callers can assign the results straight onto
// ObjectMeta.{Labels,Annotations}.
//
// Keys under ReservedAnnotationPrefix are dropped from BOTH maps before
// merging. This is load-bearing, not hygiene: additionalMetadata is mirrored
// onto every EtcdMember the operator creates, so if a user could set
// AnnHeadlessServiceName / AnnDataDirSubPath through it, every new member
// would inherit the migration knobs (breaking the self-wipe) and
// data-dir-subpath would become a user-controllable path into --data-dir.
func applyAdditionalMetadata(objLabels, objAnnotations map[string]string, md *lll.AdditionalMetadata) (labels, annotations map[string]string) {
	if md == nil {
		return objLabels, objAnnotations
	}
	if objLabels == nil && len(md.Labels) > 0 {
		objLabels = make(map[string]string, len(md.Labels))
	}
	for k, v := range md.Labels {
		if strings.HasPrefix(k, ReservedAnnotationPrefix) {
			continue
		}
		if _, taken := objLabels[k]; !taken {
			objLabels[k] = v
		}
	}
	if objAnnotations == nil && len(md.Annotations) > 0 {
		objAnnotations = make(map[string]string, len(md.Annotations))
	}
	for k, v := range md.Annotations {
		if strings.HasPrefix(k, ReservedAnnotationPrefix) {
			continue
		}
		if _, taken := objAnnotations[k]; !taken {
			objAnnotations[k] = v
		}
	}
	return objLabels, objAnnotations
}

// filterActiveMembers returns members that are not being deleted.
func filterActiveMembers(members []lll.EtcdMember) []lll.EtcdMember {
	var active []lll.EtcdMember
	for i := range members {
		if members[i].DeletionTimestamp.IsZero() {
			active = append(active, members[i])
		}
	}
	return active
}

// filterRunningMembers returns active (non-deleting) members that are not
// dormant. The cluster controller's replica accounting (`current`),
// readiness gating, and most scale decisions operate on this set —
// dormant members have no Pod and contribute no etcd capacity, so
// counting them would mean "1-member cluster paused via spec.replicas=0"
// looks like a 1-member cluster from the operator's perspective and
// scale-back-up could never decide to wake the dormant member.
func filterRunningMembers(members []lll.EtcdMember) []lll.EtcdMember {
	var running []lll.EtcdMember
	for i := range members {
		if !members[i].DeletionTimestamp.IsZero() {
			continue
		}
		if members[i].Spec.Dormant {
			continue
		}
		running = append(running, members[i])
	}
	return running
}

// findDormantMember returns the first non-deleting member with
// Spec.Dormant=true, or nil if none. By construction the operator never
// creates more than one dormant member at a time (only the 1→0 step
// flips dormant, and the 0→1 step flips it back before any further
// scale-up), but the helper just returns the first match.
func findDormantMember(members []lll.EtcdMember) *lll.EtcdMember {
	for i := range members {
		if !members[i].DeletionTimestamp.IsZero() {
			continue
		}
		if members[i].Spec.Dormant {
			return &members[i]
		}
	}
	return nil
}

// memberNameFromPeerURL recovers the EtcdMember name from a peer URL of the
// shape http://<member>.<cluster>.<namespace>.svc:2380. Used during scale-up
// when etcd's MemberList may report a member with Name=="" — the window
// between MemberAddAsLearner and the new pod reporting its identity. Returns
// "" if the URL doesn't match the expected shape.
func memberNameFromPeerURL(u string) string {
	s := strings.TrimPrefix(u, "http://")
	s = strings.TrimPrefix(s, "https://")
	if i := strings.LastIndex(s, ":"); i > 0 {
		s = s[:i]
	}
	if i := strings.Index(s, "."); i > 0 {
		return s[:i]
	}
	return ""
}

func ptrBool(b bool) *bool    { return &b }
func ptrInt64(i int64) *int64 { return &i }

// deriveClusterToken returns the etcd --initial-cluster-token value for a
// cluster. Includes namespace + UID so two same-named clusters in different
// namespaces never share a token (etcd uses the token as a sanity check
// against accidental cross-cluster peer traffic). Recorded in
// EtcdCluster.status.clusterToken at bootstrap so future changes to this
// derivation rule don't break already-running clusters.
func deriveClusterToken(cluster *lll.EtcdCluster) string {
	return fmt.Sprintf("%s-%s-%s", cluster.Namespace, cluster.Name, cluster.UID)
}

// isBroken decides whether a member should be treated as broken. In the
// current implementation the field it drives — EtcdCluster.status.broken
// Members — stays at 0 in practice, because the member controller
// detects memory-backed Pod loss and self-deletes the EtcdMember in the
// same reconcile pass; by the time updateStatus runs over the running
// set the lost member is already Terminating and filtered out.
//
// The predicate is left in place as a hook for future broken-member
// detection policies that don't tear the member down immediately (e.g.
// PVC corruption with a grace period, irrecoverable crashloop with a
// retry budget). The "memory member with PodUID recorded but PodName
// empty" condition would only arise if the member-controller's loss
// path is delayed or fails after a partial Status write — defensive
// rather than expected. For PVC-backed members the predicate stays a
// stub.
func (r *EtcdClusterReconciler) isBroken(m lll.EtcdMember) bool {
	if m.Spec.Storage.Medium == lll.StorageMediumMemory {
		return m.Status.PodUID != "" && m.Status.PodName == ""
	}
	return false
}

// setMemberCondition stamps an EtcdMember condition with the resource's
// current Generation as ObservedGeneration. Returns true when something
// actually changed; callers skip the Status().Update if nothing did, so the
// member controller doesn't write the same status back every 30 seconds
// just because of a periodic reconcile.
func setMemberCondition(member *lll.EtcdMember, condType string, status metav1.ConditionStatus, reason, msg string) bool {
	want := metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: member.Generation,
	}
	for _, existing := range member.Status.Conditions {
		if existing.Type == want.Type {
			if existing.Status == want.Status &&
				existing.Reason == want.Reason &&
				existing.Message == want.Message &&
				existing.ObservedGeneration == want.ObservedGeneration {
				return false
			}
			break
		}
	}
	setCondition(&member.Status.Conditions, want)
	return true
}

// setCondition inserts or updates a condition, preserving LastTransitionTime
// when the status has not changed.
func setCondition(conditions *[]metav1.Condition, c metav1.Condition) {
	now := metav1.Now()
	for i, existing := range *conditions {
		if existing.Type == c.Type {
			if existing.Status == c.Status {
				c.LastTransitionTime = existing.LastTransitionTime
			} else {
				c.LastTransitionTime = now
			}
			(*conditions)[i] = c
			return
		}
	}
	c.LastTransitionTime = now
	*conditions = append(*conditions, c)
}
