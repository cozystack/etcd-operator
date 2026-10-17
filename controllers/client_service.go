/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	// EventReasonTrafficDistributionRejected is emitted when the apiserver
	// refuses spec.trafficDistribution on the client Service, e.g.
	// PreferSameZone / PreferSameNode before Kubernetes 1.34.
	EventReasonTrafficDistributionRejected = "TrafficDistributionRejected"
	// EventReasonTrafficDistributionNotPersisted is emitted when the
	// apiserver accepts the write but drops the field, which is what an
	// apiserver that does not know spec.trafficDistribution does.
	EventReasonTrafficDistributionNotPersisted = "TrafficDistributionNotPersisted"

	// trafficDistributionRetryInterval is how long a value the apiserver
	// refused or dropped is left off the client Service before the
	// operator tries it again. It bounds the cost of an unsupported value
	// to one failed write per interval, and lets the value land on its own
	// once the apiserver is upgraded.
	trafficDistributionRetryInterval = time.Hour

	trafficDistributionField = "spec.trafficDistribution"
)

// trafficDistributionBackoff remembers, per client Service, a
// trafficDistribution value the apiserver would not keep, so the operator
// stops writing it on every reconcile. It is in-memory only: after an
// operator restart each unsupported value costs one more failed write.
type trafficDistributionBackoff struct {
	mu      sync.Mutex
	entries map[types.NamespacedName]trafficDistributionBackoffEntry
}

type trafficDistributionBackoffEntry struct {
	value string
	until time.Time
}

// suppressed reports whether value is still backed off for key. An entry
// for a different value (the spec changed) or an expired one does not
// count and is forgotten.
func (b *trafficDistributionBackoff) suppressed(key types.NamespacedName, value string, now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.entries[key]
	if !ok {
		return false
	}
	if e.value != value || !now.Before(e.until) {
		delete(b.entries, key)
		return false
	}
	return true
}

func (b *trafficDistributionBackoff) suppress(key types.NamespacedName, value string, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.entries == nil {
		b.entries = map[types.NamespacedName]trafficDistributionBackoffEntry{}
	}
	b.entries[key] = trafficDistributionBackoffEntry{value: value, until: now.Add(trafficDistributionRetryInterval)}
}

// isTrafficDistributionRejected reports whether err is the apiserver
// refusing spec.trafficDistribution. Other validation failures on the
// Service are not matched: they are not the operator's to work around.
func isTrafficDistributionRejected(err error) bool {
	if !errors.IsInvalid(err) {
		return false
	}
	status, ok := err.(errors.APIStatus)
	if !ok || status.Status().Details == nil {
		return false
	}
	causes := status.Status().Details.Causes
	if len(causes) == 0 {
		return false
	}
	for _, c := range causes {
		if c.Field != trafficDistributionField {
			return false
		}
	}
	return true
}

// clientServiceTrafficDistribution returns the trafficDistribution the
// client Service should carry this reconcile: the spec's value, unless that
// value is backed off.
func (r *EtcdClusterReconciler) clientServiceTrafficDistribution(cluster *lll.EtcdCluster, key types.NamespacedName) *string {
	cs := cluster.Spec.ClientService
	if cs == nil || cs.TrafficDistribution == nil {
		return nil
	}
	if r.tdBackoff.suppressed(key, *cs.TrafficDistribution, r.now()) {
		return nil
	}
	td := *cs.TrafficDistribution
	return &td
}

// trafficDistributionPersisted reports whether the Service returned by the
// apiserver carries the value the operator wrote.
func trafficDistributionPersisted(svc *corev1.Service, want string) bool {
	return svc.Spec.TrafficDistribution != nil && *svc.Spec.TrafficDistribution == want
}

func (r *EtcdClusterReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
