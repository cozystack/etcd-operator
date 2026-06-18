/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package migrate

import (
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/controllers"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

// LegacyDataDirSubPath is where the legacy operator kept etcd's data inside
// the member PVC: the volume was mounted at /var/run/etcd and etcd ran with
// --data-dir=/var/run/etcd/default.etcd (main:internal/controller/factory/
// statefulset.go). Recorded on every adopted EtcdMember so a replacement Pod
// finds the existing data dir.
const LegacyDataDirSubPath = "default.etcd"

// Legacy object-naming conventions, mirrored from the legacy operator's
// factory package (main:internal/controller/factory). The adopted cluster
// keeps using the legacy headless Service name — it is baked into the pods'
// immutable spec.subdomain and into the peer URLs persisted inside etcd.

// LegacyHeadlessServiceName mirrors factory.GetHeadlessServiceName.
func LegacyHeadlessServiceName(name string, spec legacy.EtcdClusterSpec) string {
	if spec.HeadlessServiceTemplate != nil && spec.HeadlessServiceTemplate.Name != "" {
		return spec.HeadlessServiceTemplate.Name
	}
	return name + "-headless"
}

// LegacyClientServiceName mirrors factory.GetServiceName.
func LegacyClientServiceName(name string, spec legacy.EtcdClusterSpec) string {
	if spec.ServiceTemplate != nil && spec.ServiceTemplate.Name != "" {
		return spec.ServiceTemplate.Name
	}
	return name
}

// LegacyClusterToken mirrors the legacy cluster-state ConfigMap's
// ETCD_INITIAL_CLUSTER_TOKEN derivation ("<name>-<namespace>"). Recorded in
// status.clusterToken so future scale-ups keep the token the existing
// members were bootstrapped with.
func LegacyClusterToken(name, namespace string) string {
	return name + "-" + namespace
}

// LegacyStateConfigMapName mirrors factory.GetClusterStateConfigMapName.
func LegacyStateConfigMapName(name string) string {
	return name + "-cluster-state"
}

// MemberFact is one etcd member as reported by the live legacy cluster
// (MemberList over a port-forward).
type MemberFact struct {
	// Name is the etcd member name. The legacy operator ran members with
	// --name=$(POD_NAME), so this is also the Pod and EtcdMember name.
	Name string
	// IDHex is the etcd member ID in lowercase 16-digit hex — the format
	// EtcdMemberStatus.MemberID uses.
	IDHex string
	// PeerURL is the member's first persisted peer URL, used verbatim in
	// the adopted members' spec.initialCluster.
	PeerURL string
	// IsLearner blocks adoption: the legacy operator never created
	// learners, so one indicates an intervention the tool cannot reason
	// about.
	IsLearner bool
	// PodUID is the UID of the running Pod backing this member.
	PodUID string
}

// ClusterFacts is everything the inspection phase learned about one live
// legacy cluster. BuildAdoption is pure given these facts, so the dry-run
// renders exactly what --apply executes.
type ClusterFacts struct {
	// ClusterIDHex is the etcd cluster ID in lowercase hex (the format
	// EtcdClusterStatus.ClusterID uses).
	ClusterIDHex string
	// Members is the live member list. Sorted by name in BuildAdoption.
	Members []MemberFact
	// AuthEnabled reports etcd's live auth status; when true the apply
	// phase must run `auth disable` before the new operator starts.
	AuthEnabled bool
}

// MemberAdoption is one existing pod+PVC pair becoming an EtcdMember.
type MemberAdoption struct {
	// Member is the EtcdMember CR to create. Its name equals the existing
	// Pod's name, so the member controller finds the Pod without creating
	// anything.
	Member *lll.EtcdMember
	// Status is the prefilled status written via the status subresource
	// right after Create: MemberID and IsVoter spare the controller a
	// discovery round-trip, and IsVoter=true specifically must be there
	// before the first reconcile (learner-filtering and the PDB's
	// role=voter label both key off it).
	Status lll.EtcdMemberStatus
	// PVCName is the existing PVC ("data-<member>") to label and re-own.
	PVCName string
}

// AdoptionPlan is the in-place adoption payload for one cluster: what to
// create under the new API, what to re-own, and what legacy machinery to
// dismantle (leaving the pods untouched).
type AdoptionPlan struct {
	// ClusterStatus is written to the new EtcdCluster's status subresource
	// right after Create. Prefilling ClusterID + ClusterToken + Observed
	// is what keeps the cluster controller's bootstrap branch (which would
	// create a seed pod with --initial-cluster-state=new) from ever
	// firing.
	ClusterStatus lll.EtcdClusterStatus
	// Members lists the pod adoptions, sorted by member name.
	Members []MemberAdoption
	// StatefulSetName is the legacy StatefulSet to delete with Orphan
	// propagation BEFORE pod owner references are rewritten — while it
	// exists, its controller would fight for the pods.
	StatefulSetName string
	// ConfigMapName is the legacy cluster-state ConfigMap to delete.
	ConfigMapName string
	// PDBName is the legacy PodDisruptionBudget to delete; the new
	// operator emits its own under the same name afterwards.
	PDBName string
	// HeadlessServiceName is the legacy headless Service (e.g.
	// "<cluster>-headless"). The apply phase owner-references it to the
	// adopted EtcdMembers so Kubernetes GC removes it exactly when the last
	// adopted member rolls away — no operator code manages it.
	HeadlessServiceName string
	// ClientServiceName is the legacy client Service (e.g. "<cluster>").
	// Its name collides with the operator's native headless Service, so the
	// apply phase deletes it and immediately recreates a headless Service of
	// the same name (owned by the new EtcdCluster). The DNS name keeps
	// resolving for consumers; see docs/migration.md for the
	// ClusterIP→headless caveats.
	ClientServiceName string
}

// BuildAdoption translates one legacy EtcdCluster into an in-place adoption
// plan: the spec translation of TranslateCluster, plus the new-API member
// CRs mirroring the live pods, the status prefills, and the legacy-object
// bookkeeping. Pure given the facts — no cluster access.
func BuildAdoption(name, namespace string, spec legacy.EtcdClusterSpec, facts ClusterFacts, opts TranslateOptions) ResourcePlan {
	plan := TranslateCluster(name, namespace, spec, opts)
	if plan.Action == ActionError {
		return plan
	}
	plan.Action = ActionAdopt
	cluster := plan.Target.(*lll.EtcdCluster)

	// The legacy headless Service name is the keystone: stamping it as the
	// AnnHeadlessServiceName annotation on each adopted member makes every
	// URL the new operator constructs for that member match the DNS the
	// adopted pod actually has (immutable spec.subdomain) and the peer URL
	// etcd has persisted. The operator never stamps this annotation on
	// members it creates, so replacements come up under the cluster's own
	// (native) headless Service and the override self-wipes as the cluster
	// rolls. It lives on the members, not the cluster spec.
	legacyHeadless := LegacyHeadlessServiceName(name, spec)

	members := append([]MemberFact(nil), facts.Members...)
	sort.Slice(members, func(i, j int) bool { return members[i].Name < members[j].Name })

	for _, m := range members {
		if m.IsLearner {
			plan.Errors = append(plan.Errors, fmt.Sprintf(
				"etcd member %q is a learner; the legacy operator never creates learners, refusing to adopt a cluster in an unrecognized state", m.Name))
		}
		if m.PodUID == "" {
			plan.Errors = append(plan.Errors, fmt.Sprintf(
				"etcd member %q has no running Pod of the same name; every member must be backed by a Ready pod to adopt", m.Name))
		}
	}

	// Detect the legacy operator's default --peer-auto-tls. It enables peer TLS
	// with self-signed, no-shared-CA certs UNCONDITIONALLY unless a BYO
	// peerSecret is set, so a default cluster advertises https:// peer URLs that
	// translateTLS — which sees only the spec — cannot represent (it leaves
	// spec.tls.peer nil). Carry it forward as the reserved cluster annotation
	// AnnPeerAutoTLS (NOT a typed spec field — an unauthenticated peer plane
	// must not be a discoverable option) so the new operator runs replacement/
	// scaled members with --peer-auto-tls too and they interoperate with the
	// still-auto-tls adopted members (no shared CA exists to do real mTLS, and a
	// plaintext-peer replacement could never join). This preserves the legacy
	// peer security posture — encrypted but NOT authenticated — so flag it as a
	// SecurityWarning; moving to real mTLS later is a delete-and-recreate.
	peerTLSDeclared := cluster.Spec.TLS != nil && cluster.Spec.TLS.Peer != nil
	if !peerTLSDeclared {
		for _, m := range members {
			if strings.HasPrefix(m.PeerURL, "https://") {
				if cluster.Annotations == nil {
					cluster.Annotations = map[string]string{}
				}
				cluster.Annotations[controllers.AnnPeerAutoTLS] = "true"
				plan.SecurityWarnings = append(plan.SecurityWarnings, fmt.Sprintf(
					"cluster runs etcd --peer-auto-tls (member %q advertises %s; no peerSecret in the legacy spec): carried forward via the reserved %s annotation so members keep interoperating across replacement/scale. "+
						"The peer plane is encrypted but NOT authenticated (no shared CA) — any TLS-capable workload that reaches :2380 can peer. Move to real mTLS (spec.tls.peer.secretRef or certManager) when you can; that is a delete-and-recreate since spec.tls is immutable.",
					m.Name, m.PeerURL, controllers.AnnPeerAutoTLS))
				break
			}
		}
	}

	// Replicas follow the LIVE member count. A legacy spec disagreeing with
	// reality (mid-scale crash, manual edits) is surfaced, not silently
	// trusted — adopting with spec.replicas != len(members) would make the
	// new operator immediately start scaling a cluster it just took over.
	replicas := int32(len(members))
	if spec.Replicas != nil && *spec.Replicas != replicas {
		plan.Warnings = append(plan.Warnings, fmt.Sprintf(
			"legacy spec.replicas=%d disagrees with the live member count %d; adopting with replicas=%d (the live state)",
			*spec.Replicas, replicas, replicas))
	}
	cluster.Spec.Replicas = &replicas

	if len(plan.Errors) > 0 {
		plan.Action = ActionError
		plan.Target = nil
		plan.Extras = nil
		plan.DeleteRef = nil
		return plan
	}

	plan.Adoption = &AdoptionPlan{}

	// --initial-cluster for adopted members is built from etcd's OWN view
	// (the persisted peer URLs), not reconstructed from conventions. etcd
	// ignores the flag when the data dir exists, but the member controller
	// refuses to start a pod with an empty value — and an empty value also
	// reads as "pending scale-up" to the cluster controller, which would
	// run MemberAddAsLearner against the live cluster.
	parts := make([]string, 0, len(members))
	for _, m := range members {
		parts = append(parts, m.Name+"="+m.PeerURL)
	}
	initialCluster := strings.Join(parts, ",")

	token := LegacyClusterToken(name, namespace)
	memberTLS := deriveAdoptedMemberTLS(cluster)

	for _, m := range members {
		em := &lll.EtcdMember{
			TypeMeta: metav1.TypeMeta{APIVersion: lll.GroupVersion.String(), Kind: "EtcdMember"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      m.Name,
				Namespace: namespace,
				Labels:    controllers.MemberLabels(name, m.Name),
				// Reserved annotations the member controller interprets:
				// the legacy headless Service name drives this member's DNS
				// identity, and the legacy data-dir subpath relocates
				// --data-dir so the replacement Pod finds the existing data.
				// Only adopted members carry these; the operator never
				// stamps them, so the cluster self-wipes back to native as
				// members roll.
				Annotations: map[string]string{
					controllers.AnnHeadlessServiceName: legacyHeadless,
					controllers.AnnDataDirSubPath:      LegacyDataDirSubPath,
				},
			},
			Spec: lll.EtcdMemberSpec{
				ClusterName:               name,
				Version:                   cluster.Spec.Version,
				Storage:                   cluster.Spec.Storage,
				Resources:                 cluster.Spec.Resources,
				AdditionalMetadata:        cluster.Spec.AdditionalMetadata,
				Affinity:                  cluster.Spec.Affinity,
				TopologySpreadConstraints: cluster.Spec.TopologySpreadConstraints,
				Options:                   cluster.Spec.Options,
				Image:                     cluster.Spec.Image,
				ImagePullSecrets:          cluster.Spec.ImagePullSecrets,
				Bootstrap:                 false,
				InitialCluster:            initialCluster,
				ClusterToken:              token,
				Replicas:                  1,
				TLS:                       memberTLS,
			},
		}
		plan.Adoption.Members = append(plan.Adoption.Members, MemberAdoption{
			Member: em,
			Status: lll.EtcdMemberStatus{
				MemberID: m.IDHex,
				PodName:  m.Name,
				PodUID:   m.PodUID,
				PVCName:  "data-" + m.Name,
				IsVoter:  true,
			},
			PVCName: "data-" + m.Name,
		})
	}

	plan.Adoption.ClusterStatus = lll.EtcdClusterStatus{
		ClusterID:    facts.ClusterIDHex,
		ClusterToken: token,
		Observed: &lll.ObservedClusterSpec{
			Replicas:                  replicas,
			Version:                   cluster.Spec.Version,
			Storage:                   cluster.Spec.Storage,
			Resources:                 cluster.Spec.Resources,
			Affinity:                  cluster.Spec.Affinity,
			TopologySpreadConstraints: cluster.Spec.TopologySpreadConstraints,
			AdditionalMetadata:        cluster.Spec.AdditionalMetadata,
			Options:                   cluster.Spec.Options,
			Image:                     cluster.Spec.Image,
			ImagePullSecrets:          cluster.Spec.ImagePullSecrets,
		},
	}
	plan.Adoption.StatefulSetName = name
	plan.Adoption.ConfigMapName = LegacyStateConfigMapName(name)
	plan.Adoption.PDBName = name
	plan.Adoption.HeadlessServiceName = legacyHeadless
	plan.Adoption.ClientServiceName = LegacyClientServiceName(name, spec)

	plan.Notes = append(plan.Notes,
		"in-place adoption: the etcd pods and their PVCs stay exactly as they are; only ownership, labels and member annotations change",
		fmt.Sprintf("adopted members carry annotation %s=%q so the operator's URL convention matches the adopted pods' DNS; it self-wipes as members roll", controllers.AnnHeadlessServiceName, legacyHeadless),
		fmt.Sprintf("the legacy headless Service %q is owner-referenced to the adopted members and is garbage-collected automatically once the last adopted member is replaced", legacyHeadless),
		fmt.Sprintf("the legacy client Service %q is replaced in place by the operator's native headless Service of the same name (consumers using its DNS name keep working; see docs/migration.md for the ClusterIP→headless caveats)", LegacyClientServiceName(name, spec)))

	return plan
}

// deriveAdoptedMemberTLS mirrors the controller's cluster→member TLS
// projection for the modes a legacy translation produces: client server
// secret ref + the "operator presents a client cert" bit, and peer either as
// a secret ref (BYO) or auto-tls (legacy --peer-auto-tls, carried forward via
// the reserved AnnPeerAutoTLS cluster annotation rather than the typed spec).
func deriveAdoptedMemberTLS(cluster *lll.EtcdCluster) *lll.EtcdMemberTLS {
	tls := cluster.Spec.TLS
	peerAutoTLS := cluster.Annotations[controllers.AnnPeerAutoTLS] == "true"
	if (tls == nil || (tls.Client == nil && tls.Peer == nil)) && !peerAutoTLS {
		return nil
	}
	out := &lll.EtcdMemberTLS{}
	if tls != nil && tls.Client != nil && tls.Client.ServerSecretRef != nil {
		out.ClientServerSecretRef = &corev1.LocalObjectReference{Name: tls.Client.ServerSecretRef.Name}
		out.ClientMTLS = tls.Client.OperatorClientSecretRef != nil
	}
	if tls != nil && tls.Peer != nil && tls.Peer.SecretRef != nil {
		out.PeerSecretRef = &corev1.LocalObjectReference{Name: tls.Peer.SecretRef.Name}
	} else if peerAutoTLS {
		out.PeerAutoTLS = true
	}
	return out
}
