package controllers

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// trafficDistributionFieldManager is the server-side-apply field manager
// that owns spec.trafficDistribution on the client Service. Kept apart
// from the operator's merge patches so the apiserver tracks exactly which
// value the annotation put there.
const trafficDistributionFieldManager = "etcd-operator-traffic-distribution"

// Event reasons for AnnClientServiceTrafficDistribution problems. Each one
// leaves the cluster's reconcile running and the Service field unset.
const (
	ReasonTrafficDistributionInvalid      = "InvalidTrafficDistribution"
	ReasonTrafficDistributionRejected     = "TrafficDistributionRejected"
	ReasonTrafficDistributionNotPersisted = "TrafficDistributionNotPersisted"
)

// knownTrafficDistributions are the values corev1.ServiceSpec.TrafficDistribution
// defines. Whether the apiserver accepts one depends on its version, which
// reconcileClientTrafficDistribution handles at write time.
var knownTrafficDistributions = []string{
	corev1.ServiceTrafficDistributionPreferClose,
	corev1.ServiceTrafficDistributionPreferSameZone,
	corev1.ServiceTrafficDistributionPreferSameNode,
}

func clientServiceName(cluster *lll.EtcdCluster) string {
	return cluster.Name + "-client"
}

// desiredTrafficDistribution reads AnnClientServiceTrafficDistribution.
// It returns "" when the annotation is absent, and "" with valid=false
// when its value is not one of knownTrafficDistributions.
func desiredTrafficDistribution(cluster *lll.EtcdCluster) (value string, valid bool) {
	v, ok := cluster.Annotations[AnnClientServiceTrafficDistribution]
	if !ok {
		return "", true
	}
	for _, known := range knownTrafficDistributions {
		if v == known {
			return v, true
		}
	}
	return "", false
}

// reconcileClientTrafficDistribution drives spec.trafficDistribution on the
// client Service from AnnClientServiceTrafficDistribution, via server-side
// apply under trafficDistributionFieldManager:
//
//   - annotation set: the value is applied (forcing ownership) whenever the
//     Service carries a different one;
//   - annotation absent or invalid: the field manager's ownership is
//     released, which clears the field unless another manager also owns
//     it. A value set by someone else is never touched.
//
// A value the apiserver rejects (e.g. PreferSameZone before Kubernetes
// 1.34) or silently drops (no trafficDistribution support at all) is
// reported as a Warning Event and does not fail the reconcile.
func (r *EtcdClusterReconciler) reconcileClientTrafficDistribution(ctx context.Context, cluster *lll.EtcdCluster) error {
	svc := &corev1.Service{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: clientServiceName(cluster)}, svc); err != nil {
		// Just created and not in the cache yet: its watch event brings
		// the cluster back here.
		return client.IgnoreNotFound(err)
	}

	want, valid := desiredTrafficDistribution(cluster)
	if !valid {
		r.event(cluster, corev1.EventTypeWarning, ReasonTrafficDistributionInvalid, fmt.Sprintf(
			"annotation %s=%q is not one of %v; leaving trafficDistribution unset on Service %s",
			AnnClientServiceTrafficDistribution, cluster.Annotations[AnnClientServiceTrafficDistribution],
			knownTrafficDistributions, svc.Name))
	}

	if want == "" {
		owned, err := ownsTrafficDistribution(svc)
		if err != nil || !owned {
			return err
		}
		_, err = r.applyTrafficDistribution(ctx, svc, "")
		return err
	}

	if svc.Spec.TrafficDistribution != nil && *svc.Spec.TrafficDistribution == want {
		return nil
	}
	got, err := r.applyTrafficDistribution(ctx, svc, want)
	switch {
	case errors.IsInvalid(err) || errors.IsBadRequest(err):
		r.event(cluster, corev1.EventTypeWarning, ReasonTrafficDistributionRejected, fmt.Sprintf(
			"apiserver rejected trafficDistribution=%q on Service %s; leaving it unset: %v", want, svc.Name, err))
		owned, err := ownsTrafficDistribution(svc)
		if err != nil || !owned {
			return err
		}
		_, err = r.applyTrafficDistribution(ctx, svc, "")
		return err
	case err != nil:
		return err
	case got != want:
		r.event(cluster, corev1.EventTypeWarning, ReasonTrafficDistributionNotPersisted, fmt.Sprintf(
			"apiserver dropped trafficDistribution=%q on Service %s; it does not serve the field (Kubernetes 1.31+ required)", want, svc.Name))
	default:
		log.FromContext(ctx).Info("set client Service trafficDistribution", "service", svc.Name, "trafficDistribution", want)
	}
	return nil
}

// applyTrafficDistribution server-side-applies spec.trafficDistribution=value
// (or, for "", an empty configuration that releases the field) and returns
// the value the apiserver persisted.
func (r *EtcdClusterReconciler) applyTrafficDistribution(ctx context.Context, svc *corev1.Service, value string) (string, error) {
	ac := corev1ac.Service(svc.Name, svc.Namespace)
	if value != "" {
		ac.WithSpec(corev1ac.ServiceSpec().WithTrafficDistribution(value))
	}
	raw, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ac)
	if err != nil {
		return "", err
	}
	obj := &unstructured.Unstructured{Object: raw}
	if err := r.Patch(ctx, obj, client.Apply, client.FieldOwner(trafficDistributionFieldManager), client.ForceOwnership); err != nil {
		return "", err
	}
	got, _, err := unstructured.NestedString(obj.Object, "spec", "trafficDistribution")
	return got, err
}

// ownsTrafficDistribution reports whether trafficDistributionFieldManager
// holds spec.trafficDistribution on svc. The managedFields scan keeps the
// common case (the annotation was never used) from parsing anything.
func ownsTrafficDistribution(svc *corev1.Service) (bool, error) {
	applied := false
	for _, mf := range svc.ManagedFields {
		if mf.Manager == trafficDistributionFieldManager && mf.Operation == metav1.ManagedFieldsOperationApply {
			applied = true
			break
		}
	}
	if !applied {
		return false, nil
	}
	ac, err := corev1ac.ExtractService(svc, trafficDistributionFieldManager)
	if err != nil {
		return false, err
	}
	return ac.Spec != nil && ac.Spec.TrafficDistribution != nil, nil
}
