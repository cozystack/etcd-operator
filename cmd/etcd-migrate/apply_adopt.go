/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"context"
	"fmt"
	"io"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/controllers"
	"github.com/cozystack/etcd-operator/internal/migrate"
)

// applyAdoption executes one cluster's in-place adoption. The etcd pods are
// never restarted: only object ownership, labels, member annotations and CRs
// change. Every step is idempotent, so an interrupted run is completed by
// re-running the tool.
//
// Ordering is load-bearing in three places:
//
//   - The new-API CRs are created with their status prefilled before the
//     user scales the new operator up (the tool runs with both operators
//     down), so the cluster controller's bootstrap branch never fires.
//   - The legacy headless Service is owner-referenced to the adopted members
//     BEFORE the legacy CRs are deleted — otherwise the Service is briefly
//     sole-owned by a now-missing object and GC could reap it.
//   - The legacy StatefulSet is orphan-deleted (and its deletion awaited)
//     BEFORE pod owner references are rewritten — while it exists, its
//     controller would adopt the pods right back.
func applyAdoption(ctx context.Context, c client.Client, dyn dynamic.Interface, p *migrate.ResourcePlan, out io.Writer) error {
	a := p.Adoption
	cluster := p.Target.(*lll.EtcdCluster)
	ns := p.Namespace

	// 1. Create the new-API cluster (+ companion Secret) with prefilled
	// status. Done first: the prefilled status.clusterID keeps the bootstrap
	// branch from ever firing, and the live cluster UID owns the headless
	// Service recreated in step 6.
	for _, extra := range p.Extras {
		if err := c.Create(ctx, extra); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create %s %s/%s: %w",
				extra.GetObjectKind().GroupVersionKind().Kind, ns, extra.GetName(), err)
		}
	}
	if err := c.Create(ctx, cluster); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create EtcdCluster: %w", err)
	}
	liveCluster := &lll.EtcdCluster{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: cluster.Name}, liveCluster); err != nil {
		return fmt.Errorf("re-read EtcdCluster: %w", err)
	}
	// Fill-if-empty: a re-run must not clobber status once the new operator
	// has taken over (both operators are down during a normal run, but stay
	// safe against misuse).
	if liveCluster.Status.ClusterID == "" {
		liveCluster.Status = a.ClusterStatus
		if err := c.Status().Update(ctx, liveCluster); err != nil {
			return fmt.Errorf("prefill EtcdCluster status: %w", err)
		}
		fmt.Fprintf(out, "  created EtcdCluster %q (clusterID=%s prefilled — bootstrap will not fire)\n",
			cluster.Name, a.ClusterStatus.ClusterID)
	}

	// 2. Create the per-pod EtcdMembers (+ status prefill) and capture their
	// live UIDs — required before owner-referencing the legacy headless
	// Service to them in step 3.
	liveMembers := make([]*lll.EtcdMember, len(a.Members))
	for i, ma := range a.Members {
		if err := c.Create(ctx, ma.Member); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create EtcdMember %q: %w", ma.Member.Name, err)
		}
		liveMember := &lll.EtcdMember{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: ma.Member.Name}, liveMember); err != nil {
			return fmt.Errorf("re-read EtcdMember %q: %w", ma.Member.Name, err)
		}
		if liveMember.Status.MemberID == "" {
			liveMember.Status = ma.Status
			if err := c.Status().Update(ctx, liveMember); err != nil {
				return fmt.Errorf("prefill EtcdMember %q status: %w", ma.Member.Name, err)
			}
		}
		liveMembers[i] = liveMember
	}

	// 3. Point the legacy headless Service's ownerReferences at the adopted
	// members (replacing the legacy controller owner). Kubernetes deletes a
	// dependent once all its owners are gone, so with one owner ref per
	// adopted member the Service survives while any adopted member remains
	// and is auto-GC'd when the last one rolls away. Replacement (native)
	// members are not owners, so they never keep it alive. Done BEFORE the
	// legacy-CR deletion to avoid a premature-GC race.
	if a.HeadlessServiceName != "" {
		if err := pointServiceAtMembers(ctx, c, ns, a.HeadlessServiceName, liveMembers); err != nil {
			return err
		}
		fmt.Fprintf(out, "  owner-referenced legacy headless Service %q to %d adopted member(s) (auto-GCs as they roll)\n",
			a.HeadlessServiceName, len(liveMembers))
	}

	// 4. Dismantle the legacy control plane, keeping the data plane. Orphan
	// propagation everywhere so the pods/PVCs/Services survive.
	if p.DeleteRef != nil {
		orphan := metav1.DeletePropagationOrphan
		err := dyn.Resource(p.DeleteRef.GVR).Namespace(ns).
			Delete(ctx, p.DeleteRef.Name, metav1.DeleteOptions{PropagationPolicy: &orphan})
		if err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("orphan-delete legacy EtcdCluster: %w", err)
		}
		fmt.Fprintf(out, "  orphan-deleted legacy EtcdCluster (children survive)\n")
	}

	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: a.StatefulSetName}}
	orphan := metav1.DeletePropagationOrphan
	if err := c.Delete(ctx, sts, &client.DeleteOptions{PropagationPolicy: &orphan}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("orphan-delete legacy StatefulSet: %w", err)
	}
	if err := waitGone(ctx, c, types.NamespacedName{Namespace: ns, Name: a.StatefulSetName}, &appsv1.StatefulSet{}, 2*time.Minute); err != nil {
		return fmt.Errorf("await StatefulSet deletion: %w", err)
	}
	fmt.Fprintf(out, "  orphan-deleted legacy StatefulSet %q (pods survive)\n", a.StatefulSetName)

	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: a.ConfigMapName}}
	if err := c.Delete(ctx, cm); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete legacy cluster-state ConfigMap: %w", err)
	}
	// The new operator emits its own PDB under the same name; remove the
	// legacy one so the two never select the same pods concurrently.
	pdb := &policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: a.PDBName}}
	if err := c.Delete(ctx, pdb); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete legacy PodDisruptionBudget: %w", err)
	}

	// 5. Re-own the pods and PVCs to their EtcdMembers — only now that the
	// StatefulSet is gone and its controller can no longer fight us.
	for i, ma := range a.Members {
		if err := adoptPod(ctx, c, ns, ma.Member.Name, cluster.Name, liveMembers[i]); err != nil {
			return err
		}
		if err := adoptPVC(ctx, c, ns, ma.PVCName, cluster.Name, liveMembers[i]); err != nil {
			return err
		}
		fmt.Fprintf(out, "  adopted member %q (pod + PVC re-owned, memberID=%s)\n", ma.Member.Name, ma.Status.MemberID)
	}

	// 6. Client-Service cutover. The legacy client Service is named after the
	// cluster, which collides with the operator's native headless Service.
	// Delete it and immediately recreate a headless Service of the same name
	// (owned by the new cluster) so the DNS name keeps resolving with the
	// minimum possible gap, rather than leaving the window open until the
	// operator's first reconcile.
	if err := cutoverHeadlessService(ctx, c, ns, cluster.Name, liveCluster); err != nil {
		return err
	}
	fmt.Fprintf(out, "  cut over Service %q to the operator's native headless Service\n", cluster.Name)

	return nil
}

// pointServiceAtMembers replaces a Service's ownerReferences with one
// non-controller, non-blocking entry per EtcdMember. A full Update (not a
// merge patch) is used deliberately: the legacy controller owner reference
// must be STRIPPED, and a strategic merge patch keyed on owner UID would
// merge the new refs in alongside the stale one rather than replacing the
// list. Idempotent — a re-run rewrites the same refs.
func pointServiceAtMembers(ctx context.Context, c client.Client, ns, name string, members []*lll.EtcdMember) error {
	svc := &corev1.Service{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, svc); err != nil {
		if apierrors.IsNotFound(err) {
			// Already GC'd by a prior complete run (all adopted members
			// rolled away) — nothing to keep alive.
			return nil
		}
		return fmt.Errorf("read legacy headless Service %q: %w", name, err)
	}
	gvk := lll.GroupVersion.WithKind("EtcdMember")
	refs := make([]metav1.OwnerReference, 0, len(members))
	for _, m := range members {
		refs = append(refs, metav1.OwnerReference{
			APIVersion:         gvk.GroupVersion().String(),
			Kind:               gvk.Kind,
			Name:               m.Name,
			UID:                m.UID,
			Controller:         ptrTo(false),
			BlockOwnerDeletion: ptrTo(false),
		})
	}
	svc.OwnerReferences = refs
	if err := c.Update(ctx, svc); err != nil {
		return fmt.Errorf("owner-reference legacy headless Service %q to members: %w", name, err)
	}
	return nil
}

// cutoverHeadlessService ensures `name` is the operator's native headless
// Service, owned by the new EtcdCluster. If a ClusterIP Service already holds
// the name (the legacy client Service, whose name collides with the native
// headless), it is deleted and recreated headless — clusterIP is immutable,
// so an in-place flip is impossible. Idempotent: an already-headless Service
// at the name is left untouched.
func cutoverHeadlessService(ctx context.Context, c client.Client, ns, name string, owner *lll.EtcdCluster) error {
	svc := &corev1.Service{}
	err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, svc)
	switch {
	case apierrors.IsNotFound(err):
		// Nothing at the name — just create the headless Service below.
	case err != nil:
		return fmt.Errorf("read Service %q: %w", name, err)
	case svc.Spec.ClusterIP == corev1.ClusterIPNone:
		// Already headless (a prior run, or an override that never collided).
		return nil
	default:
		// A ClusterIP Service (the legacy client) holds the name. Delete it so
		// the headless Service can take the name.
		if err := c.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete legacy client Service %q: %w", name, err)
		}
		if err := waitGone(ctx, c, types.NamespacedName{Namespace: ns, Name: name}, &corev1.Service{}, time.Minute); err != nil {
			return fmt.Errorf("await legacy client Service %q deletion: %w", name, err)
		}
	}

	gvk := lll.GroupVersion.WithKind("EtcdCluster")
	headless := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Labels:    controllers.ClusterLabels(owner.Name),
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         gvk.GroupVersion().String(),
				Kind:               gvk.Kind,
				Name:               owner.Name,
				UID:                owner.UID,
				Controller:         ptrTo(true),
				BlockOwnerDeletion: ptrTo(true),
			}},
		},
		// Matches the operator's native headless Service (ensureServices), so
		// its first reconcile finds no drift to reconcile.
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 map[string]string{controllers.LabelCluster: owner.Name},
			Ports: []corev1.ServicePort{
				{Name: "client", Port: 2379},
				{Name: "peer", Port: 2380},
			},
		},
	}
	if err := c.Create(ctx, headless); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create native headless Service %q: %w", name, err)
	}
	return nil
}

// adoptPod stamps the operator's member labels (incl. role=voter — every
// adopted member is a voter) and rewrites the controller owner reference to
// the EtcdMember. The pod itself is not restarted; labels and owner refs are
// mutable on live pods.
func adoptPod(ctx context.Context, c client.Client, ns, podName, clusterName string, owner *lll.EtcdMember) error {
	pod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: podName}, pod); err != nil {
		return fmt.Errorf("read pod %q: %w", podName, err)
	}
	orig := pod.DeepCopy()
	if pod.Labels == nil {
		pod.Labels = map[string]string{}
	}
	for k, v := range controllers.MemberLabels(clusterName, podName) {
		pod.Labels[k] = v
	}
	pod.Labels[controllers.LabelRole] = controllers.RoleVoter
	setControllerOwner(&pod.ObjectMeta, owner)
	if err := c.Patch(ctx, pod, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("re-own pod %q: %w", podName, err)
	}
	return nil
}

// adoptPVC mirrors adoptPod for the member's data PVC. The new member
// controller refuses PVCs without its own controller owner reference
// (pvcOwnedBy), so this patch is what makes ensurePVC pass.
func adoptPVC(ctx context.Context, c client.Client, ns, pvcName, clusterName string, owner *lll.EtcdMember) error {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvcName}, pvc); err != nil {
		return fmt.Errorf("read PVC %q: %w", pvcName, err)
	}
	orig := pvc.DeepCopy()
	if pvc.Labels == nil {
		pvc.Labels = map[string]string{}
	}
	for k, v := range controllers.MemberLabels(clusterName, owner.Name) {
		pvc.Labels[k] = v
	}
	setControllerOwner(&pvc.ObjectMeta, owner)
	if err := c.Patch(ctx, pvc, client.MergeFrom(orig)); err != nil {
		return fmt.Errorf("re-own PVC %q: %w", pvcName, err)
	}
	return nil
}

// setControllerOwner replaces any existing controller owner reference with
// one pointing at the EtcdMember, matching what the member controller's
// SetControllerReference would produce.
func setControllerOwner(meta *metav1.ObjectMeta, owner *lll.EtcdMember) {
	gvk := lll.GroupVersion.WithKind("EtcdMember")
	replaceControllerRef(meta, metav1.OwnerReference{
		APIVersion:         gvk.GroupVersion().String(),
		Kind:               gvk.Kind,
		Name:               owner.Name,
		UID:                owner.UID,
		Controller:         ptrTo(true),
		BlockOwnerDeletion: ptrTo(true),
	})
}

// replaceControllerRef drops any previous controller=true reference (the
// orphaned StatefulSet's, a prior partial run's) and appends `ref`.
// Idempotent: a matching ref is left in place.
func replaceControllerRef(meta *metav1.ObjectMeta, ref metav1.OwnerReference) {
	kept := meta.OwnerReferences[:0]
	for _, o := range meta.OwnerReferences {
		if o.UID == ref.UID && o.Kind == ref.Kind {
			continue // re-added below in canonical form
		}
		if o.Controller != nil && *o.Controller {
			continue // displaced by the new controller owner
		}
		kept = append(kept, o)
	}
	meta.OwnerReferences = append(kept, ref)
}

// waitGone polls until the object disappears.
func waitGone(ctx context.Context, c client.Client, key types.NamespacedName, obj client.Object, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		err := c.Get(ctx, key, obj)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("%s/%s still present after %s", key.Namespace, key.Name, timeout)
		case <-time.After(2 * time.Second):
		}
	}
}

func ptrTo[T any](v T) *T { return &v }
