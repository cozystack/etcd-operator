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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestParseUTCSchedule(t *testing.T) {
	if _, err := parseUTCSchedule("0 3 * * *"); err != nil {
		t.Fatalf("valid schedule rejected: %v", err)
	}
	// UTC is forced: a schedule with no TZ is evaluated in UTC regardless of the
	// process zone. "0 0 * * *" from 12:00 UTC lands on the next UTC midnight.
	sched, err := parseUTCSchedule("0 0 * * *")
	if err != nil {
		t.Fatal(err)
	}
	got := sched.Next(time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC))
	if want := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("Next = %s, want %s", got, want)
	}
	if _, err := parseUTCSchedule("not a schedule"); err == nil {
		t.Error("expected an error for an unparseable schedule")
	}
}

func TestNextSchedule(t *testing.T) {
	sched, err := parseUTCSchedule("0 * * * *") // top of every hour
	if err != nil {
		t.Fatal(err)
	}

	// Next tick still in the future: nothing due.
	if due, next := nextSchedule(sched, epoch, epoch.Add(30*time.Minute), 100); due != nil {
		t.Errorf("due = %s, want nil (next tick is in the future)", due)
	} else if want := epoch.Add(time.Hour); !next.Equal(want) {
		t.Errorf("next = %s, want %s", next, want)
	}

	// One tick due: the most recent boundary at or before now.
	if due, next := nextSchedule(sched, epoch, epoch.Add(90*time.Minute), 100); due == nil {
		t.Fatal("due = nil, want the 01:00 tick")
	} else if !due.Equal(epoch.Add(time.Hour)) {
		t.Errorf("due = %s, want %s", due, epoch.Add(time.Hour))
	} else if !next.Equal(epoch.Add(2 * time.Hour)) {
		t.Errorf("next = %s, want %s", next, epoch.Add(2*time.Hour))
	}

	// A long backlog collapses to a single run stamped at now.
	now := epoch.Add(1000 * time.Hour)
	if due, _ := nextSchedule(sched, epoch, now, 100); due == nil || !due.Equal(now) {
		t.Errorf("due = %v, want collapse to now (%s)", due, now)
	}
}

// ── controller ──────────────────────────────────────────────────────────────

func defragPolicy(name, schedule string, opts ...func(*lll.EtcdDefragPolicy)) *lll.EtcdDefragPolicy {
	p := &lll.EtcdDefragPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", CreationTimestamp: metav1.NewTime(epoch)},
		Spec:       lll.EtcdDefragPolicySpec{ClusterRef: corev1.LocalObjectReference{Name: "c1"}, Schedule: schedule},
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

func policyReconciler(t *testing.T, now time.Time, objs ...client.Object) (*EtcdDefragPolicyReconciler, client.Client) {
	t.Helper()
	c, s := newTestClient(t, objs...)
	return &EtcdDefragPolicyReconciler{Client: c, Scheme: s, Recorder: record.NewFakeRecorder(20), now: func() time.Time { return now }}, c
}

func listPolicyRuns(t *testing.T, c client.Client, policy string) []lll.EtcdDefrag {
	t.Helper()
	var runs lll.EtcdDefragList
	if err := c.List(context.Background(), &runs, client.InNamespace("ns"), client.MatchingLabels{LabelDefragPolicy: policy}); err != nil {
		t.Fatalf("list runs: %v", err)
	}
	return runs.Items
}

func reconcilePolicy(t *testing.T, r *EtcdDefragPolicyReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: nn(name, "ns")})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

// A due tick stamps one EtcdDefrag, owned by and labelled with the policy,
// carrying the policy's rule/ttl, and records lastScheduleTime.
func TestDefragPolicy_StampsWhenDue(t *testing.T) {
	ttl := int32(3600)
	pol := defragPolicy("p", "0 * * * *", func(p *lll.EtcdDefragPolicy) {
		p.Spec.Rule = &lll.DefragRule{All: true}
		p.Spec.TTLSecondsAfterFinished = &ttl
	})
	r, c := policyReconciler(t, epoch.Add(90*time.Minute), pol)

	reconcilePolicy(t, r, "p")

	runs := listPolicyRuns(t, c, "p")
	if len(runs) != 1 {
		t.Fatalf("stamped %d runs, want 1", len(runs))
	}
	run := runs[0]
	if run.Spec.ClusterRef.Name != "c1" || run.Spec.Rule == nil || !run.Spec.Rule.All {
		t.Errorf("stamped run spec = %+v, want clusterRef c1 + rule.all", run.Spec)
	}
	if run.Spec.TTLSecondsAfterFinished == nil || *run.Spec.TTLSecondsAfterFinished != ttl {
		t.Errorf("stamped ttl = %v, want %d", run.Spec.TTLSecondsAfterFinished, ttl)
	}
	if run.Labels[LabelCluster] != "c1" {
		t.Errorf("missing cluster label: %v", run.Labels)
	}
	if len(run.OwnerReferences) != 1 || run.OwnerReferences[0].Name != "p" {
		t.Errorf("owner refs = %+v, want the policy", run.OwnerReferences)
	}
	got := mustGet(t, c, "p", "ns", &lll.EtcdDefragPolicy{})
	if got.Status.LastScheduleTime == nil || !got.Status.LastScheduleTime.Time.Equal(epoch.Add(time.Hour)) {
		t.Errorf("lastScheduleTime = %v, want 01:00", got.Status.LastScheduleTime)
	}
	if len(got.Status.Active) != 1 {
		t.Errorf("status.active = %v, want the stamped run", got.Status.Active)
	}
}

// Before the first tick, nothing is stamped and the policy requeues.
func TestDefragPolicy_NotDueYet(t *testing.T) {
	pol := defragPolicy("p", "0 0 * * *") // daily midnight
	r, c := policyReconciler(t, epoch.Add(time.Hour), pol)

	res := reconcilePolicy(t, r, "p")
	if len(listPolicyRuns(t, c, "p")) != 0 {
		t.Fatalf("stamped a run before the first tick")
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue toward the next tick, got %+v", res)
	}
}

// A suspended policy stamps nothing and reports Suspended.
func TestDefragPolicy_Suspended(t *testing.T) {
	suspend := true
	pol := defragPolicy("p", "0 * * * *", func(p *lll.EtcdDefragPolicy) { p.Spec.Suspend = &suspend })
	r, c := policyReconciler(t, epoch.Add(90*time.Minute), pol)

	reconcilePolicy(t, r, "p")
	if len(listPolicyRuns(t, c, "p")) != 0 {
		t.Fatalf("suspended policy stamped a run")
	}
	got := mustGet(t, c, "p", "ns", &lll.EtcdDefragPolicy{})
	if cond := findPolicyCond(got); cond == nil || cond.Reason != "Suspended" {
		t.Errorf("condition = %+v, want Suspended", cond)
	}
}

// An unparseable schedule reports InvalidSchedule and stamps nothing.
func TestDefragPolicy_InvalidSchedule(t *testing.T) {
	pol := defragPolicy("p", "every blue moon")
	r, c := policyReconciler(t, epoch.Add(time.Hour), pol)

	reconcilePolicy(t, r, "p")
	if len(listPolicyRuns(t, c, "p")) != 0 {
		t.Fatalf("stamped a run on an invalid schedule")
	}
	got := mustGet(t, c, "p", "ns", &lll.EtcdDefragPolicy{})
	if cond := findPolicyCond(got); cond == nil || cond.Reason != "InvalidSchedule" {
		t.Errorf("condition = %+v, want InvalidSchedule", cond)
	}
}

// With the default Forbid policy, a due tick is skipped while a previous run is
// still active — no second run is stamped, but the tick is consumed.
func TestDefragPolicy_ForbidConcurrent(t *testing.T) {
	pol := defragPolicy("p", "0 * * * *")
	active := activeRun("p-existing", "p")
	r, c := policyReconciler(t, epoch.Add(90*time.Minute), pol, active)

	reconcilePolicy(t, r, "p")
	if runs := listPolicyRuns(t, c, "p"); len(runs) != 1 {
		t.Fatalf("Forbid stamped a concurrent run: %d runs", len(runs))
	}
	got := mustGet(t, c, "p", "ns", &lll.EtcdDefragPolicy{})
	if got.Status.LastScheduleTime == nil {
		t.Errorf("a forbidden tick should still advance lastScheduleTime")
	}
}

// With Allow, a due tick is stamped even while a previous run is active.
func TestDefragPolicy_AllowConcurrent(t *testing.T) {
	pol := defragPolicy("p", "0 * * * *", func(p *lll.EtcdDefragPolicy) { p.Spec.ConcurrencyPolicy = lll.AllowConcurrent })
	active := activeRun("p-existing", "p")
	r, c := policyReconciler(t, epoch.Add(90*time.Minute), pol, active)

	reconcilePolicy(t, r, "p")
	if runs := listPolicyRuns(t, c, "p"); len(runs) != 2 {
		t.Fatalf("Allow did not stamp a concurrent run: %d runs", len(runs))
	}
}

// HistoryLimit trims the oldest finished runs, keeping the newest.
func TestDefragPolicy_HistoryLimit(t *testing.T) {
	limit := int32(1)
	pol := defragPolicy("p", "0 0 * * *", func(p *lll.EtcdDefragPolicy) { p.Spec.HistoryLimit = &limit }) // not due
	old1 := finishedRun("p-1", "p", epoch.Add(1*time.Hour))
	old2 := finishedRun("p-2", "p", epoch.Add(2*time.Hour))
	newest := finishedRun("p-3", "p", epoch.Add(3*time.Hour))
	r, c := policyReconciler(t, epoch.Add(90*time.Minute), pol, old1, old2, newest)

	reconcilePolicy(t, r, "p")
	runs := listPolicyRuns(t, c, "p")
	if len(runs) != 1 {
		t.Fatalf("history GC kept %d runs, want 1", len(runs))
	}
	if runs[0].Name != "p-3" {
		t.Errorf("GC kept %q, want the newest p-3", runs[0].Name)
	}
}

func activeRun(name, policy string) *lll.EtcdDefrag {
	return &lll.EtcdDefrag{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{LabelDefragPolicy: policy}},
		Status:     lll.EtcdDefragStatus{Phase: lll.EtcdDefragPhaseRunning},
	}
}

func finishedRun(name, policy string, completed time.Time) *lll.EtcdDefrag {
	ct := metav1.NewTime(completed)
	return &lll.EtcdDefrag{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", Labels: map[string]string{LabelDefragPolicy: policy}},
		Status:     lll.EtcdDefragStatus{Phase: lll.EtcdDefragPhaseComplete, CompletedAt: &ct},
	}
}

func findPolicyCond(pol *lll.EtcdDefragPolicy) *metav1.Condition {
	for i := range pol.Status.Conditions {
		if pol.Status.Conditions[i].Type == defragPolicyCondition {
			return &pol.Status.Conditions[i]
		}
	}
	return nil
}
