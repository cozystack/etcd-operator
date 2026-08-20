/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	// defragPolicyCondition is the single condition type on an EtcdDefragPolicy:
	// True while the policy is actively scheduling, False (with the reason) when
	// suspended or holding an unparseable schedule.
	defragPolicyCondition = "Active"

	// defragPolicyMaxCatchup bounds how many missed ticks the controller walks
	// after being down; beyond it a long backlog is collapsed into a single run
	// rather than replaying every slot.
	defragPolicyMaxCatchup = 100
)

// EtcdDefragPolicyReconciler stamps out EtcdDefrag runs on a cron schedule so
// the operator drives recurring defragmentation itself. Each run is a discrete
// EtcdDefrag owned by the policy (so it cascades on delete) and labelled with
// the policy name (so the controller can find its own runs).
type EtcdDefragPolicyReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Recorder emits scheduling events. Tests may leave it nil.
	Recorder record.EventRecorder

	// now is the clock, overridable in tests. nil means time.Now.
	now func() time.Time
}

//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcddefragpolicies,verbs=get;list;watch;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcddefragpolicies/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcddefrags,verbs=get;list;watch;create;delete

func (r *EtcdDefragPolicyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	pol := &lll.EtcdDefragPolicy{}
	if err := r.Get(ctx, req.NamespacedName, pol); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Observe the runs this policy owns.
	var runs lll.EtcdDefragList
	if err := r.List(ctx, &runs, client.InNamespace(pol.Namespace),
		client.MatchingLabels{LabelDefragPolicy: pol.Name}); err != nil {
		return ctrl.Result{}, err
	}
	active, finished := partitionDefragRuns(runs.Items)
	pol.Status.Active = defragRunRefs(active)
	if t := latestSuccessfulTime(finished); t != nil {
		pol.Status.LastSuccessfulTime = t
	}

	// Trim finished history to HistoryLimit (each run's own
	// ttlSecondsAfterFinished is the other cleanup path).
	if pol.Spec.HistoryLimit != nil {
		if err := r.gcHistory(ctx, finished, int(*pol.Spec.HistoryLimit)); err != nil {
			return ctrl.Result{}, err
		}
	}

	if pol.Spec.Suspend != nil && *pol.Spec.Suspend {
		setDefragPolicyCondition(pol, metav1.ConditionFalse, "Suspended", "scheduling is suspended")
		return ctrl.Result{}, r.Status().Update(ctx, pol)
	}

	sched, err := parseUTCSchedule(pol.Spec.Schedule)
	if err != nil {
		setDefragPolicyCondition(pol, metav1.ConditionFalse, "InvalidSchedule",
			fmt.Sprintf("cannot parse schedule %q: %v", pol.Spec.Schedule, err))
		// Only a spec change can fix this; the watch re-triggers, so don't requeue.
		return ctrl.Result{}, r.Status().Update(ctx, pol)
	}
	setDefragPolicyCondition(pol, metav1.ConditionTrue, "Scheduled", "policy is scheduling runs")

	now := r.clock()
	earliest := pol.CreationTimestamp.Time
	if pol.Status.LastScheduleTime != nil {
		earliest = pol.Status.LastScheduleTime.Time
	}
	due, next := nextSchedule(sched, earliest, now, defragPolicyMaxCatchup)

	if due == nil {
		if err := r.Status().Update(ctx, pol); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueFor(next, now)}, nil
	}
	tick := *due

	// A tick too far in the past (operator was down, or held by Forbid) is
	// skipped rather than started late.
	if d := pol.Spec.StartingDeadlineSeconds; d != nil && now.Sub(tick) > time.Duration(*d)*time.Second {
		r.event(pol, corev1.EventTypeWarning, "MissedSchedule",
			fmt.Sprintf("skipped scheduled time %s: past the %ds starting deadline", tickString(tick), *d))
		pol.Status.LastScheduleTime = &metav1.Time{Time: tick}
		if err := r.Status().Update(ctx, pol); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueFor(next, now)}, nil
	}

	if concurrencyPolicy(pol) == lll.ForbidConcurrent && len(active) > 0 {
		r.event(pol, corev1.EventTypeNormal, "ConcurrencyForbidden",
			fmt.Sprintf("skipped scheduled time %s: %d run(s) still active", tickString(tick), len(active)))
		pol.Status.LastScheduleTime = &metav1.Time{Time: tick}
		if err := r.Status().Update(ctx, pol); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: requeueFor(next, now)}, nil
	}

	run := r.buildDefrag(pol, tick)
	if err := controllerutil.SetControllerReference(pol, run, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, run); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
		// The deterministic name means a re-reconcile of the same tick is a
		// no-op rather than a duplicate run.
		logger.Info("defrag already stamped for this tick", "tick", tickString(tick), "name", run.Name)
	} else {
		r.event(pol, corev1.EventTypeNormal, "StampedRun",
			fmt.Sprintf("stamped EtcdDefrag %q for scheduled time %s", run.Name, tickString(tick)))
		pol.Status.Active = append(pol.Status.Active, corev1.LocalObjectReference{Name: run.Name})
	}
	pol.Status.LastScheduleTime = &metav1.Time{Time: tick}
	if err := r.Status().Update(ctx, pol); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: requeueFor(next, now)}, nil
}

// buildDefrag renders the EtcdDefrag stamped for a tick. The name is
// deterministic in the scheduled time so a re-reconcile of the same tick
// collides (IsAlreadyExists) instead of double-stamping.
func (r *EtcdDefragPolicyReconciler) buildDefrag(pol *lll.EtcdDefragPolicy, tick time.Time) *lll.EtcdDefrag {
	return &lll.EtcdDefrag{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", pol.Name, tick.Unix()),
			Namespace: pol.Namespace,
			Labels: map[string]string{
				LabelDefragPolicy: pol.Name,
				LabelCluster:      pol.Spec.ClusterRef.Name,
			},
		},
		Spec: lll.EtcdDefragSpec{
			ClusterRef:              pol.Spec.ClusterRef,
			Rule:                    pol.Spec.Rule.DeepCopy(),
			TTLSecondsAfterFinished: copyInt32(pol.Spec.TTLSecondsAfterFinished),
		},
	}
}

func (r *EtcdDefragPolicyReconciler) gcHistory(ctx context.Context, finished []lll.EtcdDefrag, limit int) error {
	if len(finished) <= limit {
		return nil
	}
	sort.Slice(finished, func(i, j int) bool {
		return defragFinishTime(&finished[i]).Before(defragFinishTime(&finished[j]))
	})
	for i := 0; i < len(finished)-limit; i++ {
		if err := r.Delete(ctx, &finished[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

func (r *EtcdDefragPolicyReconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *EtcdDefragPolicyReconciler) event(obj client.Object, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(obj, eventType, reason, msg)
	}
}

func (r *EtcdDefragPolicyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.now == nil {
		r.now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&lll.EtcdDefragPolicy{}).
		Owns(&lll.EtcdDefrag{}).
		Complete(r)
}

// ── pure helpers ────────────────────────────────────────────────────────────

// parseUTCSchedule parses a standard five-field cron expression in UTC. A
// user-supplied CRON_TZ/TZ prefix is honoured as-is; otherwise UTC is forced so
// the schedule does not silently follow the operator process's local zone.
func parseUTCSchedule(schedule string) (cron.Schedule, error) {
	spec := strings.TrimSpace(schedule)
	if !strings.Contains(spec, "TZ=") {
		spec = "CRON_TZ=UTC " + spec
	}
	return cron.ParseStandard(spec)
}

// nextSchedule returns the most recent scheduled time at or before now that is
// strictly after earliest (nil if the next tick is still in the future), and
// the next tick after now. A backlog longer than maxCatchup is collapsed into a
// single run stamped at now.
func nextSchedule(sched cron.Schedule, earliest, now time.Time, maxCatchup int) (due *time.Time, next time.Time) {
	t := sched.Next(earliest)
	if t.After(now) {
		return nil, t
	}
	last := t
	for n := 0; ; n++ {
		t = sched.Next(t)
		if t.After(now) {
			break
		}
		if n >= maxCatchup {
			last = now
			return &last, sched.Next(now)
		}
		last = t
	}
	return &last, t
}

// requeueFor is the delay until next, floored so a just-passed boundary still
// yields a positive requeue.
func requeueFor(next, now time.Time) time.Duration {
	if d := next.Sub(now); d > 0 {
		return d
	}
	return time.Second
}

func concurrencyPolicy(pol *lll.EtcdDefragPolicy) lll.ConcurrencyPolicy {
	if pol.Spec.ConcurrencyPolicy == "" {
		return lll.ForbidConcurrent
	}
	return pol.Spec.ConcurrencyPolicy
}

func partitionDefragRuns(items []lll.EtcdDefrag) (active, finished []lll.EtcdDefrag) {
	for i := range items {
		switch items[i].Status.Phase {
		case lll.EtcdDefragPhaseComplete, lll.EtcdDefragPhaseFailed:
			finished = append(finished, items[i])
		default:
			active = append(active, items[i])
		}
	}
	return active, finished
}

func defragRunRefs(items []lll.EtcdDefrag) []corev1.LocalObjectReference {
	if len(items) == 0 {
		return nil
	}
	out := make([]corev1.LocalObjectReference, 0, len(items))
	for i := range items {
		out = append(out, corev1.LocalObjectReference{Name: items[i].Name})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func latestSuccessfulTime(items []lll.EtcdDefrag) *metav1.Time {
	var best *metav1.Time
	for i := range items {
		d := &items[i]
		if d.Status.Phase != lll.EtcdDefragPhaseComplete || d.Status.CompletedAt == nil {
			continue
		}
		if best == nil || d.Status.CompletedAt.After(best.Time) {
			best = d.Status.CompletedAt
		}
	}
	return best
}

// defragFinishTime orders finished runs for history GC: completion time, or the
// creation time when a run finished without stamping CompletedAt.
func defragFinishTime(d *lll.EtcdDefrag) time.Time {
	if d.Status.CompletedAt != nil {
		return d.Status.CompletedAt.Time
	}
	return d.CreationTimestamp.Time
}

func setDefragPolicyCondition(pol *lll.EtcdDefragPolicy, status metav1.ConditionStatus, reason, msg string) {
	setCondition(&pol.Status.Conditions, metav1.Condition{
		Type:               defragPolicyCondition,
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: pol.Generation,
	})
}

func tickString(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func copyInt32(p *int32) *int32 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
