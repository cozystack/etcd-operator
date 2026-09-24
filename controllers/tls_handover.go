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

package controllers

import (
	"context"
	goerrors "errors"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// caCertKey is the Secret key cert-manager writes the issuing CA under,
// and the one etcd is pointed at via --trusted-ca-file /
// --peer-trusted-ca-file.
const caCertKey = "ca.crt"

// tlsMaterialConflictError reports that a piece of TLS material the
// operator needs to own already exists under another controller's
// ownership. Carried out of ensureCertificate so Reconcile can report it
// as a condition rather than as an opaque reconcile failure.
type tlsMaterialConflictError struct {
	kind string
	name string
}

func (e *tlsMaterialConflictError) Error() string {
	return fmt.Sprintf("%s %q exists but is not controlled by this EtcdCluster", e.kind, e.name)
}

// asTLSMaterialConflict unwraps err to a *tlsMaterialConflictError.
func asTLSMaterialConflict(err error) (*tlsMaterialConflictError, bool) {
	var conflict *tlsMaterialConflictError
	if goerrors.As(err, &conflict) {
		return conflict, true
	}
	return nil, false
}

// membersNeedingTLSHandover returns the members whose recorded TLS view no
// longer matches what the cluster's spec resolves to.
//
// Every other mirrored field (Storage, Resources, Version) is deliberately
// frozen per member at creation — the cluster controller does not
// re-template existing members when the cluster spec moves. TLS is the one
// exception, because the material a member holds is not a preference but a
// precondition for talking to its peers: leave half the cluster on the old
// CA and there is no cluster, only two halves that cannot authenticate each
// other.
func membersNeedingTLSHandover(cluster *lll.EtcdCluster, members []lll.EtcdMember) []*lll.EtcdMember {
	want := deriveMemberTLS(cluster)
	var out []*lll.EtcdMember
	for i := range members {
		m := &members[i]
		if !equality.Semantic.DeepEqual(m.Spec.TLS, want) {
			out = append(out, m)
		}
	}
	return out
}

// unreadyTLSSecrets returns a human-readable list of the Secrets named by
// the cluster's current TLS spec that are not yet usable — absent, or
// present but missing a key etcd will be started against.
//
// Existence alone is not a sufficient gate. cert-manager creates the Secret
// before it finishes issuing into it, and a Pod mounting a Secret whose
// tls.key has not landed yet starts etcd against an unreadable file and
// crash-loops. Checking the keys turns that into a wait.
func (r *EtcdClusterReconciler) unreadyTLSSecrets(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	want *lll.EtcdMemberTLS,
) ([]string, error) {
	var unready []string

	check := func(name string, keys ...string) error {
		sec := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: name}, sec); err != nil {
			if errors.IsNotFound(err) {
				unready = append(unready, name+" (not created yet)")
				return nil
			}
			return err
		}
		var missing []string
		for _, k := range keys {
			if len(sec.Data[k]) == 0 {
				missing = append(missing, k)
			}
		}
		if len(missing) > 0 {
			unready = append(unready, fmt.Sprintf("%s (missing %s)", name, strings.Join(missing, ", ")))
		}
		return nil
	}

	if want != nil {
		if want.ClientServerSecretRef != nil {
			keys := []string{corev1.TLSCertKey, corev1.TLSPrivateKeyKey}
			// ca.crt only matters when the server verifies client certs;
			// in server-TLS-only mode etcd is never handed a
			// --trusted-ca-file and cert-manager may legitimately omit it.
			if want.ClientMTLS {
				keys = append(keys, caCertKey)
			}
			if err := check(want.ClientServerSecretRef.Name, keys...); err != nil {
				return nil, err
			}
		}
		if want.PeerSecretRef != nil {
			// Peer is always mTLS, so the CA is always load-bearing.
			if err := check(want.PeerSecretRef.Name, corev1.TLSCertKey, corev1.TLSPrivateKeyKey, caCertKey); err != nil {
				return nil, err
			}
		}
	}

	// The operator's own client identity is not part of the member view,
	// but it has to be usable the moment the members come back on the new
	// CA — otherwise the roll completes into a cluster the operator can no
	// longer dial, and it cannot even observe what it just did.
	if name := operatorClientSecretName(cluster); name != "" {
		if err := check(name, corev1.TLSCertKey, corev1.TLSPrivateKeyKey); err != nil {
			return nil, err
		}
	}

	sort.Strings(unready)
	return unready, nil
}

// reconcileTLSHandover moves an already-running cluster from user-provided
// TLS Secrets onto operator-managed cert-manager material.
//
// Returns a non-nil *ctrl.Result when it has taken over the reconcile —
// either because it is waiting for material or because it has just rolled
// the members. A nil result means there was nothing to do and the caller
// should carry on.
//
// Sequencing, and why it is a single simultaneous roll rather than a
// rolling restart: the new material is signed by a different CA than the
// old, so an old member and a new member cannot authenticate each other at
// all. Rolling one member at a time would therefore spend the entire roll
// with a split cluster — every intermediate step is a quorum failure, and
// with 3 members a one-at-a-time roll is strictly *worse* than stopping
// everything, because it drags the outage out across three pod startups
// instead of one. So: repoint every member in one pass, let their Pods come
// back together on consistent material, and keep the window as short as the
// slowest pod start.
//
// The conflict argument is the (optional) outcome of certificate emission.
// It is threaded in rather than re-derived so the blocked state is reported
// on the same condition as the rest of the handover.
func (r *EtcdClusterReconciler) reconcileTLSHandover(
	ctx context.Context,
	cluster *lll.EtcdCluster,
	members []lll.EtcdMember,
	conflict *tlsMaterialConflictError,
) (*ctrl.Result, error) {
	log := log.FromContext(ctx)

	stale := membersNeedingTLSHandover(cluster, members)

	// Conflict first, before the members are even considered. A Certificate
	// this cluster must own but does not is worth surfacing whether or not
	// any member happens to be drifting right now — reporting Complete while
	// the operator cannot own its own material would be a lie of omission.
	if conflict != nil {
		// Blocked, and only a human can unblock it. Deliberately not fatal
		// to the reconcile: the cluster is still serving on the material it
		// already holds, and freezing the rest of the loop here would leave
		// its readyMembers and health conditions stale — a cluster that
		// silently stops reporting is worse than one that reports it cannot
		// converge.
		log.Info("TLS handover is blocked by a conflicting object",
			"kind", conflict.kind, "name", conflict.name)
		if setClusterCondition(cluster, lll.ClusterTLSHandover, metav1.ConditionFalse, lll.TLSHandoverBlocked,
			fmt.Sprintf("cannot take over %s %q: it exists but another controller owns it — most likely the "+
				"chart that installed this cluster. Stop that chart from emitting it and the operator will "+
				"issue its own; cert-manager leaves the existing Secret in place, so the replacement reissues "+
				"into it without a gap.", conflict.kind, conflict.name)) {
			if err := r.statusUpdateTolerateConflict(ctx, cluster); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}

	if len(stale) == 0 {
		// Nothing pending. Only claim completion if we ever said otherwise;
		// clusters that were born on cert-manager material never had a
		// handover and should not carry a condition about one.
		if prev := findClusterCondition(cluster, lll.ClusterTLSHandover); prev != nil {
			if setClusterCondition(cluster, lll.ClusterTLSHandover, metav1.ConditionFalse,
				lll.TLSHandoverComplete, "all members run on the TLS material named by spec.tls") {
				if err := r.statusUpdateTolerateConflict(ctx, cluster); err != nil {
					return nil, err
				}
			}
		}
		return nil, nil
	}

	want := deriveMemberTLS(cluster)

	unready, err := r.unreadyTLSSecrets(ctx, cluster, want)
	if err != nil {
		return nil, err
	}
	if len(unready) > 0 {
		// Not one member is touched until every piece of the new material
		// is on disk. This is the difference between a brief outage and an
		// unrecoverable one: repoint the members first and they all come
		// back mounting a Secret that does not exist, with the old material
		// already out of their spec.
		log.Info("TLS handover waiting on material", "unready", unready)
		if setClusterCondition(cluster, lll.ClusterTLSHandover, metav1.ConditionTrue,
			lll.TLSHandoverAwaitingMaterial,
			"waiting for TLS material to be issued: "+strings.Join(unready, "; ")) {
			if err := r.statusUpdateTolerateConflict(ctx, cluster); err != nil {
				return nil, err
			}
		}
		return &ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	names := make([]string, 0, len(stale))
	for _, m := range stale {
		orig := m.DeepCopy()
		m.Spec.TLS = want.DeepCopy()
		if err := r.Patch(ctx, m, client.MergeFrom(orig)); err != nil {
			// Partial application is safe to retry: the members already
			// patched simply don't come up in the next pass's stale set,
			// and the ones that didn't get patched are picked up again.
			return nil, err
		}
		names = append(names, m.Name)
	}
	sort.Strings(names)

	log.Info("TLS handover: repointed members at operator-managed material; their Pods will be rebuilt",
		"members", names)
	if setClusterCondition(cluster, lll.ClusterTLSHandover, metav1.ConditionTrue, lll.TLSHandoverRollingMembers,
		fmt.Sprintf("rolling %d member(s) onto operator-managed TLS material: %s",
			len(names), strings.Join(names, ", "))) {
		if err := r.statusUpdateTolerateConflict(ctx, cluster); err != nil {
			return nil, err
		}
	}
	return &ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

// statusUpdateTolerateConflict writes the cluster status, treating a
// conflict as success — the next reconcile re-derives the same condition
// from a fresh read. Keeps the handover from turning an optimistic-
// concurrency retry into a reconcile error.
func (r *EtcdClusterReconciler) statusUpdateTolerateConflict(ctx context.Context, cluster *lll.EtcdCluster) error {
	if err := r.Status().Update(ctx, cluster); err != nil && !errors.IsConflict(err) {
		return err
	}
	return nil
}

// findClusterCondition returns the named condition, or nil.
func findClusterCondition(cluster *lll.EtcdCluster, condType string) *metav1.Condition {
	for i := range cluster.Status.Conditions {
		if cluster.Status.Conditions[i].Type == condType {
			return &cluster.Status.Conditions[i]
		}
	}
	return nil
}

// podTLSSecretNames reports the Secret names a Pod actually mounts for the
// client and peer planes. Empty string means the plane is not mounted.
func podTLSSecretNames(pod *corev1.Pod) (clientSecret, peerSecret string) {
	for _, v := range pod.Spec.Volumes {
		if v.Secret == nil {
			continue
		}
		switch v.Name {
		case "tls-client":
			clientSecret = v.Secret.SecretName
		case "tls-peer":
			peerSecret = v.Secret.SecretName
		}
	}
	return clientSecret, peerSecret
}

// tlsMountsOutOfDate reports whether a running Pod mounts different TLS
// Secrets than its member spec now names — the observable trace of a
// handover that has repointed the spec but not yet rebuilt the Pod.
//
// Scoped strictly to Secret *names*. Content changes (a cert-manager
// renewal writing a fresh leaf into the same Secret) are not a reason to
// rebuild anything: the kubelet refreshes the projected volume in place and
// etcd picks the new leaf up on subsequent handshakes.
//
// Only Pods this operator built are candidates. A Pod carrying neither of
// the operator's TLS volumes is either plaintext (nothing to compare) or a
// shape the operator did not author — an adopted member from etcd-migrate
// keeps the legacy StatefulSet's volume names, and adoption deliberately
// never restarts it. Treating that as drift would delete every adopted Pod
// on the first reconcile after a migration, which is precisely the outcome
// in-place adoption exists to avoid. Such members are left alone; a
// handover rolls them only when they are rolled onto the operator's own
// Pod shape.
func tlsMountsOutOfDate(pod *corev1.Pod, member *lll.EtcdMember) bool {
	gotClient, gotPeer := podTLSSecretNames(pod)
	if gotClient == "" && gotPeer == "" {
		return false
	}

	var wantClient, wantPeer string
	if member.Spec.TLS != nil {
		if member.Spec.TLS.ClientServerSecretRef != nil {
			wantClient = member.Spec.TLS.ClientServerSecretRef.Name
		}
		if member.Spec.TLS.PeerSecretRef != nil {
			wantPeer = member.Spec.TLS.PeerSecretRef.Name
		}
	}
	return gotClient != wantClient || gotPeer != wantPeer
}
