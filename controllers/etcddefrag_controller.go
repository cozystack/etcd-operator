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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	etcdserverpb "go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	// Rule defaults (see api DefragRule). A defrag can only reclaim
	// DbSize-DbSizeInUse, so freeSpaceAbove is the always-applied floor.
	defaultDefragFreeSpace  = int64(200 << 20) // 200Mi
	defaultDefragMinReclaim = int64(32 << 20)  // 32Mi
	// defaultEtcdQuotaBytes mirrors etcd's built-in --quota-backend-bytes
	// default (2Gi), used when the cluster leaves spec.options.quotaBackendBytes
	// unset.
	defaultEtcdQuotaBytes = int64(2 << 30)

	// defragStatusTimeout bounds a per-member Maintenance Status probe;
	// defragRPCTimeout bounds a single stop-the-world Defragment call.
	defragStatusTimeout = 5 * time.Second
	defragRPCTimeout    = 5 * time.Minute
	// defragRequeueAfter paces the one-member-per-pass sweep and the retry while
	// deferred on an unhealthy cluster.
	defragRequeueAfter = 10 * time.Second

	// defragMaxSupportedMembers bounds the worst-case serial sweep the active
	// deadline must outlast. etcd clusters are odd-sized and rarely exceed 7.
	defragMaxSupportedMembers = 7
	// defragActiveDeadline bounds a whole run (Running + waiting-while-Pending),
	// so a run stuck on an unhealthy cluster can't hold the per-cluster slot
	// forever. It must outlast the worst-case serial sweep — one member per pass,
	// each pass probing every member then a stop-the-world Defragment and a
	// requeue gap — or a healthy-but-slow big-backend cluster is killed mid-sweep;
	// derived from the constants above so it stays consistent if any is retuned.
	defragActiveDeadline = defragMaxSupportedMembers*(defragRPCTimeout+defragRequeueAfter+defragMaxSupportedMembers*defragStatusTimeout) + 5*time.Minute

	// defragMaxConcurrentReconciles lets distinct clusters' runs proceed in
	// parallel (per-cluster serialization is enforced separately by oldestActive).
	defragMaxConcurrentReconciles = 4
)

// EtcdDefragReconciler drives an EtcdDefrag: a one-shot, run-to-completion
// defragmentation of an EtcdCluster's members. It defragments members one at a
// time (followers before the leader) on a healthy cluster, deferring rather
// than forcing while the cluster is degraded, and records per-member outcomes
// in status.
type EtcdDefragReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// EtcdClientFactory builds an etcd client; tests inject a fake.
	EtcdClientFactory EtcdClientFactory

	// Recorder emits events (e.g. when a due defrag is deferred). Tests may
	// leave it nil.
	Recorder record.EventRecorder
}

//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcddefrags,verbs=get;list;watch;update;patch;delete
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcddefrags/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdclusters,verbs=get;list;watch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdmembers,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=events,verbs=create;patch

func (r *EtcdDefragReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	df := &lll.EtcdDefrag{}
	if err := r.Get(ctx, req.NamespacedName, df); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal: only TTL GC remains.
	if df.Status.Phase == lll.EtcdDefragPhaseComplete || df.Status.Phase == lll.EtcdDefragPhaseFailed {
		return r.handleTTL(ctx, df)
	}

	cluster := &lll.EtcdCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: df.Namespace, Name: df.Spec.ClusterRef.Name}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.fail(ctx, df, "ClusterNotFound",
				fmt.Sprintf("EtcdCluster %q not found in namespace %q", df.Spec.ClusterRef.Name, df.Namespace))
		}
		return ctrl.Result{}, err
	}

	// Serialize per cluster: only the oldest non-terminal EtcdDefrag targeting
	// this cluster may act; the rest wait in Pending.
	if active, err := r.oldestActive(ctx, df.Namespace, cluster.Name); err != nil {
		return ctrl.Result{}, err
	} else if active != "" && active != df.Name {
		if setDefragCondition(df, metav1.ConditionFalse, "Queued",
			fmt.Sprintf("waiting for EtcdDefrag %q to finish", active)) || df.Status.Phase != lll.EtcdDefragPhasePending {
			df.Status.Phase = lll.EtcdDefragPhasePending
			if err := r.Status().Update(ctx, df); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: defragRequeueAfter}, nil
	}

	var memberList lll.EtcdMemberList
	if err := r.List(ctx, &memberList, client.InNamespace(df.Namespace),
		client.MatchingLabels{LabelCluster: cluster.Name}); err != nil {
		return ctrl.Result{}, err
	}
	running := filterRunningMembers(memberList.Items)

	c, backends, err := r.dialAndProbe(ctx, cluster, running)
	if err != nil {
		// Without a client we can neither defragment nor disarm; if the run has
		// outlived its deadline while stuck here, fail it rather than requeue
		// forever.
		if exceededDeadline(df) {
			return r.fail(ctx, df, "DeadlineExceeded", deadlineMsg(df))
		}
		logger.Error(err, "defrag: cannot dial/probe cluster; retrying")
		return ctrl.Result{RequeueAfter: defragRequeueAfter}, nil
	}
	defer c.Close()

	// Overall deadline: a run that can't make progress must fail, not linger. A
	// run that already reclaimed space disarms NOSPACE on the way out (failRun),
	// so a deadline-terminated partial sweep still lifts the wedge that admitted
	// it. Checked after dialing so the disarm has a client.
	if exceededDeadline(df) {
		return r.failRun(ctx, df, c, "DeadlineExceeded", deadlineMsg(df))
	}

	// Health gate: defrag is only safe when every desired member is present,
	// reachable, free of blocking alarms and agreeing on a leader.
	if reason, ok := clusterDefragHealthy(cluster, running, backends); !ok {
		msg := "a defragmentation is due but the cluster is not fully healthy; deferring to protect quorum: " + reason
		if setDefragCondition(df, metav1.ConditionFalse, "ClusterNotHealthy", msg) {
			r.event(df, corev1.EventTypeWarning, "DefragDeferred", msg)
		}
		// Keep a run that has already started (partial results in
		// status.members) in Running and let the condition carry the reason;
		// only a run that never started falls back to Pending. StartedAt is what
		// the active-deadline is measured against, so it must not be re-stamped
		// by such a flap — see the Running transition below.
		if df.Status.StartedAt == nil {
			df.Status.Phase = lll.EtcdDefragPhasePending
		}
		if err := r.Status().Update(ctx, df); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: defragRequeueAfter}, nil
	}

	// Initialize the per-member work list once (followers first, leader last).
	if len(df.Status.Members) == 0 {
		df.Status.Members = plannedMembers(backends)
	}

	next := firstPendingMember(df)
	if next == nil {
		return r.finalize(ctx, df, c)
	}

	df.Status.Phase = lll.EtcdDefragPhaseRunning
	// Stamp StartedAt exactly once, on the first Running transition. The
	// active-deadline is measured from it, so re-stamping on a Pending->Running
	// flap (a cluster that recovers between health-gate blips) would keep
	// resetting the deadline and a stuck run could hold the per-cluster slot
	// forever. Done before the backend lookup so a run whose first planned member
	// has vanished is still stamped Running and its deadline measured from work,
	// not creation.
	if df.Status.StartedAt == nil {
		now := metav1.Now()
		df.Status.StartedAt = &now
	}
	// Refresh the condition each active pass so a run that resumes after a
	// health-gate flap does not stay reading ClusterNotHealthy.
	setDefragCondition(df, metav1.ConditionTrue, "Running", "defragmenting members")

	b := backendByName(backends, next.Name)
	if b == nil {
		// A planned member vanished between passes despite the health gate.
		markMember(df, next.Name, lll.DefragOutcomeFailed, "MemberGone", nil)
		return r.persistAndRequeue(ctx, df)
	}

	quota := effectiveQuotaBytes(cluster)
	if trig, reason := defragRuleTriggered(df.Spec.Rule, b.status.DbSize, b.status.DbSizeInUse, quota); !trig {
		markMember(df, b.member.Name, lll.DefragOutcomeSkipped, reason, b)
		return r.persistAndRequeue(ctx, df)
	}

	dctx, cancel := context.WithTimeout(ctx, defragRPCTimeout)
	defer cancel()
	if _, derr := c.Defragment(dctx, b.endpoint); derr != nil {
		msg := fmt.Sprintf("defragmentation of member %s failed: %v", b.member.Name, derr)
		markMember(df, b.member.Name, lll.DefragOutcomeFailed, "RPCError", b)
		r.event(df, corev1.EventTypeWarning, "DefragFailed", msg)
		logger.Error(derr, "defrag: member Defragment failed", "member", b.member.Name)
		return r.persistAndRequeue(ctx, df)
	}

	// Read the post-defrag size for the record. When the read fails, leave the
	// after-size unset rather than defaulting it to the before-size — reporting
	// a successful defrag as reclaiming zero would be silently wrong in the very
	// field the per-member status exists to provide.
	outcomeReason := ""
	if after, serr := statusWithTimeout(ctx, c, b.endpoint); serr == nil {
		b.after = after.DbSize
		b.afterKnown = true
	} else {
		outcomeReason = "AfterSizeUnavailable"
		logger.Info("defrag: post-defrag Status read failed; reclaimed size unrecorded",
			"member", b.member.Name, "err", serr)
	}
	markMember(df, b.member.Name, lll.DefragOutcomeDefragmented, outcomeReason, b)
	df.Status.Defragmented++
	logger.Info("defragmented member", "member", b.member.Name)
	return r.persistAndRequeue(ctx, df)
}

// memberBackend pairs a running member with its live Maintenance Status.
type memberBackend struct {
	member     *lll.EtcdMember
	endpoint   string
	status     *clientv3.StatusResponse
	after      int64 // DbSize after a successful defrag; valid only if afterKnown
	afterKnown bool  // whether the post-defrag Status read succeeded
}

// dialAndProbe opens one client and reads every running member's Status. The
// caller closes the client.
func (r *EtcdDefragReconciler) dialAndProbe(ctx context.Context, cluster *lll.EtcdCluster, running []lll.EtcdMember) (EtcdClusterClient, []memberBackend, error) {
	tlsCfg, err := buildOperatorTLSConfig(ctx, r.Client, cluster)
	if err != nil {
		return nil, nil, err
	}
	user, pass, _, err := resolveEtcdCredentials(ctx, r.Client, cluster)
	if err != nil {
		return nil, nil, err
	}
	scheme := clusterClientScheme(cluster)
	endpoints := make([]string, len(running))
	for i := range running {
		endpoints[i] = clientURL(scheme, running[i].Name, memberServiceName(&running[i]), cluster.Namespace)
	}
	c, err := r.EtcdClientFactory(ctx, endpoints, tlsCfg, user, pass)
	if err != nil {
		return nil, nil, err
	}
	backends := make([]memberBackend, 0, len(running))
	for i := range running {
		resp, serr := statusWithTimeout(ctx, c, endpoints[i])
		if serr != nil {
			// Unreachable member: record a nil-status backend so the health gate
			// sees the gap.
			backends = append(backends, memberBackend{member: &running[i], endpoint: endpoints[i]})
			continue
		}
		backends = append(backends, memberBackend{member: &running[i], endpoint: endpoints[i], status: resp})
	}
	return c, backends, nil
}

func statusWithTimeout(ctx context.Context, c EtcdClusterClient, endpoint string) (*clientv3.StatusResponse, error) {
	sctx, cancel := context.WithTimeout(ctx, defragStatusTimeout)
	defer cancel()
	return c.Status(sctx, endpoint)
}

// disarmNoSpaceAlarms clears every armed NOSPACE alarm. The health gate lets a
// NOSPACE cluster through so the run can reclaim its backend space; etcd keeps
// each member's alarm armed — and the cluster read-only — until it is explicitly
// disarmed. AlarmList returns one entry per member that raised NOSPACE, so a
// transient failure on one must not abandon the rest: the loop continues and
// joins the errors, naming every member it could not disarm. Best-effort in that
// etcd re-arms on the next write if the space was not actually freed.
func (r *EtcdDefragReconciler) disarmNoSpaceAlarms(ctx context.Context, c EtcdClusterClient) error {
	lctx, cancel := context.WithTimeout(ctx, defragStatusTimeout)
	defer cancel()
	resp, err := c.AlarmList(lctx)
	if err != nil {
		return err
	}
	var errs error
	for _, a := range resp.Alarms {
		if a == nil || a.Alarm != etcdserverpb.AlarmType_NOSPACE {
			continue
		}
		dctx, dcancel := context.WithTimeout(ctx, defragStatusTimeout)
		_, derr := c.AlarmDisarm(dctx, (*clientv3.AlarmMember)(a))
		dcancel()
		if derr != nil {
			errs = errors.Join(errs, fmt.Errorf("member %d: %w", a.MemberID, derr))
		}
	}
	return errs
}

// maybeDisarm clears any armed NOSPACE alarm when the run reclaimed backend
// space, on any terminal path — a partial or deadline-terminated sweep still
// relieves the wedge that admitted it, and etcd holds the cluster read-only
// until the alarm is disarmed. Best-effort: a failure is logged and surfaced as
// an event rather than propagated, since etcd re-arms on the next write if the
// space was not actually freed.
func (r *EtcdDefragReconciler) maybeDisarm(ctx context.Context, df *lll.EtcdDefrag, c EtcdClusterClient) {
	if c == nil || df.Status.Defragmented == 0 {
		return
	}
	if err := r.disarmNoSpaceAlarms(ctx, c); err != nil {
		log.FromContext(ctx).Error(err, "defrag: could not disarm NOSPACE alarm after sweep")
		r.event(df, corev1.EventTypeWarning, "AlarmDisarmFailed",
			fmt.Sprintf("reclaimed backend space but could not disarm NOSPACE alarm; cluster may stay read-only: %v", err))
	}
}

// clusterDefragHealthy reports whether the cluster is safe to defragment: every
// desired member present and reachable, free of blocking alarms, and agreeing on
// a single non-zero leader. Status is a local read — a member answers it while
// partitioned or alarmed — so agreement and reported alarms are checked, not just
// that the RPC returned.
//
// A NOSPACE alarm does not block: a backend at its quota is exactly what a defrag
// is meant to relieve, and refusing it would leave the one cluster this feature
// exists for read-only forever. Every other alarm (notably CORRUPT) blocks.
func clusterDefragHealthy(cluster *lll.EtcdCluster, running []lll.EtcdMember, backends []memberBackend) (string, bool) {
	desired := 0
	if cluster.Status.Observed != nil {
		desired = int(cluster.Status.Observed.Replicas)
	}
	if desired == 0 || len(running) != desired {
		return fmt.Sprintf("have %d running members, want %d", len(running), desired), false
	}
	var leader uint64
	for i := range backends {
		b := &backends[i]
		if b.status == nil {
			return fmt.Sprintf("member %s is unreachable", b.member.Name), false
		}
		if e, blocking := blockingStatusError(b.status.Errors); blocking {
			return fmt.Sprintf("member %s reports: %s", b.member.Name, e), false
		}
		if b.status.Leader == 0 {
			return fmt.Sprintf("member %s reports no leader", b.member.Name), false
		}
		if leader == 0 {
			leader = b.status.Leader
		} else if b.status.Leader != leader {
			return "members disagree on the leader", false
		}
	}
	return "", true
}

// blockingStatusError returns the first StatusResponse.Errors entry that should
// block a defragmentation, and whether one exists. etcd reports active alarms
// there as "memberID:<id> alarm:<TYPE>"; a NOSPACE line is the reclaimable-space
// case defrag relieves and does not block, while anything else (a CORRUPT alarm
// or any other health string) does.
func blockingStatusError(errs []string) (string, bool) {
	for _, e := range errs {
		if strings.Contains(e, "alarm:"+etcdserverpb.AlarmType_NOSPACE.String()) {
			continue
		}
		return e, true
	}
	return "", false
}

// plannedMembers builds the ordered work list: followers first, the leader
// last (its defrag is the most disruptive and is done only after followers
// prove defrag is healthy on this cluster).
func plannedMembers(backends []memberBackend) []lll.MemberDefragStatus {
	followers := make([]lll.MemberDefragStatus, 0, len(backends))
	var leader []lll.MemberDefragStatus
	for i := range backends {
		b := &backends[i]
		row := lll.MemberDefragStatus{Name: b.member.Name, Outcome: lll.DefragOutcomePending}
		if isLeaderStatus(b.status) {
			row.Role = lll.MemberRoleLeader
			leader = append(leader, row)
		} else {
			row.Role = lll.MemberRoleFollower
			followers = append(followers, row)
		}
	}
	return append(followers, leader...)
}

func isLeaderStatus(s *clientv3.StatusResponse) bool {
	return s != nil && s.Header != nil && s.Leader != 0 && s.Leader == s.Header.MemberId
}

func firstPendingMember(df *lll.EtcdDefrag) *lll.MemberDefragStatus {
	for i := range df.Status.Members {
		if df.Status.Members[i].Outcome == lll.DefragOutcomePending {
			return &df.Status.Members[i]
		}
	}
	return nil
}

func backendByName(backends []memberBackend, name string) *memberBackend {
	for i := range backends {
		if backends[i].member.Name == name {
			return &backends[i]
		}
	}
	return nil
}

func markMember(df *lll.EtcdDefrag, name string, outcome lll.DefragOutcome, reason string, b *memberBackend) {
	for i := range df.Status.Members {
		m := &df.Status.Members[i]
		if m.Name != name {
			continue
		}
		m.Outcome = outcome
		m.Reason = reason
		now := metav1.Now()
		m.FinishedAt = &now
		if b != nil && b.status != nil {
			m.DBSizeBefore = b.status.DbSize
			if b.afterKnown {
				m.DBSizeAfter = b.after
				if outcome == lll.DefragOutcomeDefragmented && b.status.DbSize >= b.after {
					m.ReclaimedBytes = b.status.DbSize - b.after
				}
			}
		}
		return
	}
}

// effectiveQuotaBytes is the backend quota the cluster's members run with: the
// latched spec.options.quotaBackendBytes, or etcd's 2Gi default.
func effectiveQuotaBytes(cluster *lll.EtcdCluster) int64 {
	if o := cluster.Status.Observed; o != nil && o.Options != nil &&
		o.Options.QuotaBackendBytes != nil && *o.Options.QuotaBackendBytes > 0 {
		return *o.Options.QuotaBackendBytes
	}
	return defaultEtcdQuotaBytes
}

// defragRuleTriggered reports whether a member's backend meets the rule, and a
// reason when it does not. rule.All is unconditional. Otherwise the free-space
// arm (reclaimable > freeSpaceAbove, default 200Mi) is always applied; the
// quota arm additionally fires under quota pressure but only when there is at
// least MinReclaim to reclaim — so a full-but-unfragmented backend is never
// defragmented for nothing.
func defragRuleTriggered(rule *lll.DefragRule, dbSize, dbSizeInUse, quota int64) (bool, string) {
	if rule != nil && rule.All {
		return true, ""
	}

	free := defaultDefragFreeSpace
	minReclaim := defaultDefragMinReclaim
	quotaUsage := 0.0
	quotaSet := false
	if rule != nil {
		if rule.FreeSpaceAbove != nil {
			free = rule.FreeSpaceAbove.Value()
		}
		if rule.MinReclaim != nil {
			minReclaim = rule.MinReclaim.Value()
		}
		if p, ok := parsePercent(rule.QuotaUsageAbove); ok {
			quotaUsage = p
			quotaSet = true
		}
	}

	reclaimable := dbSize - dbSizeInUse
	if reclaimable > free {
		return true, ""
	}
	if quotaSet && quota > 0 && float64(dbSize) > quotaUsage*float64(quota) && reclaimable >= minReclaim {
		return true, ""
	}
	return false, "BelowThreshold"
}

// parsePercent parses "80%" into 0.80. Returns ok=false for anything outside
// 1–99 or non-numeric; the CRD pattern rejects such values at admission, so this
// is a defensive fallback. 100% is excluded on purpose: a backend never exceeds
// its quota (etcd raises NOSPACE first), so the quota arm could never fire.
func parsePercent(s string) (float64, bool) {
	n, err := strconv.Atoi(strings.TrimSuffix(s, "%"))
	if err != nil || n <= 0 || n >= 100 {
		return 0, false
	}
	return float64(n) / 100, true
}

// oldestActive returns the name of the oldest non-terminal EtcdDefrag targeting
// clusterName in namespace (creationTimestamp, name tiebreak), or "" if none.
func (r *EtcdDefragReconciler) oldestActive(ctx context.Context, namespace, clusterName string) (string, error) {
	var list lll.EtcdDefragList
	if err := r.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	active := make([]lll.EtcdDefrag, 0, len(list.Items))
	for _, d := range list.Items {
		if d.Spec.ClusterRef.Name != clusterName {
			continue
		}
		if d.Status.Phase == lll.EtcdDefragPhaseComplete || d.Status.Phase == lll.EtcdDefragPhaseFailed {
			continue
		}
		active = append(active, d)
	}
	if len(active) == 0 {
		return "", nil
	}
	sort.Slice(active, func(i, j int) bool {
		if !active[i].CreationTimestamp.Equal(&active[j].CreationTimestamp) {
			return active[i].CreationTimestamp.Before(&active[j].CreationTimestamp)
		}
		return active[i].Name < active[j].Name
	})
	return active[0].Name, nil
}

func (r *EtcdDefragReconciler) finalize(ctx context.Context, df *lll.EtcdDefrag, c EtcdClusterClient) (ctrl.Result, error) {
	phase := lll.EtcdDefragPhaseComplete
	reason := "Complete"
	for _, m := range df.Status.Members {
		if m.Outcome == lll.DefragOutcomeFailed {
			phase = lll.EtcdDefragPhaseFailed
			reason = "MemberFailed"
			break
		}
	}
	// A cluster admitted with a NOSPACE alarm stays read-only until the alarm is
	// disarmed; disarm whenever the sweep reclaimed space, even if a later member
	// failed — the reclaimed space is what lifts the wedge, and gating this on a
	// wholly-clean run would leave the one cluster this feature rescues read-only.
	r.maybeDisarm(ctx, df, c)
	df.Status.Phase = phase
	now := metav1.Now()
	df.Status.CompletedAt = &now
	status := metav1.ConditionTrue
	if phase == lll.EtcdDefragPhaseFailed {
		status = metav1.ConditionFalse
	}
	setDefragCondition(df, status, reason,
		fmt.Sprintf("%d/%d members defragmented", df.Status.Defragmented, len(df.Status.Members)))
	if err := r.Status().Update(ctx, df); err != nil {
		return ctrl.Result{}, err
	}
	return r.handleTTL(ctx, df)
}

// failRun is fail() for a terminal path that holds a client: it disarms any
// NOSPACE alarm the run relieved before recording the failure.
func (r *EtcdDefragReconciler) failRun(ctx context.Context, df *lll.EtcdDefrag, c EtcdClusterClient, reason, msg string) (ctrl.Result, error) {
	r.maybeDisarm(ctx, df, c)
	return r.fail(ctx, df, reason, msg)
}

// exceededDeadline reports whether the run has outlived defragActiveDeadline,
// measured from StartedAt once work began, else from creation. CreationTimestamp
// is zero before the apiserver stamps it (e.g. in unit tests), so a zero
// reference never trips the deadline.
func exceededDeadline(df *lll.EtcdDefrag) bool {
	if s := df.Status.StartedAt; s != nil {
		return time.Since(s.Time) > defragActiveDeadline
	}
	return !df.CreationTimestamp.IsZero() && time.Since(df.CreationTimestamp.Time) > defragActiveDeadline
}

func deadlineMsg(df *lll.EtcdDefrag) string {
	if df.Status.StartedAt != nil {
		return fmt.Sprintf("defragmentation did not complete within %s", defragActiveDeadline)
	}
	return fmt.Sprintf("defragmentation could not start within %s (cluster never became healthy)", defragActiveDeadline)
}

func (r *EtcdDefragReconciler) fail(ctx context.Context, df *lll.EtcdDefrag, reason, msg string) (ctrl.Result, error) {
	df.Status.Phase = lll.EtcdDefragPhaseFailed
	now := metav1.Now()
	df.Status.CompletedAt = &now
	if setDefragCondition(df, metav1.ConditionFalse, reason, msg) {
		r.event(df, corev1.EventTypeWarning, reason, msg)
	}
	if err := r.Status().Update(ctx, df); err != nil {
		return ctrl.Result{}, err
	}
	return r.handleTTL(ctx, df)
}

func (r *EtcdDefragReconciler) persistAndRequeue(ctx context.Context, df *lll.EtcdDefrag) (ctrl.Result, error) {
	// A Defragment RPC can block for defragRPCTimeout; an unrelated metadata write
	// (a kubectl label/annotate) in that window would make this status write
	// conflict and discard the recorded outcome, so the member would be
	// defragmented again next pass. The defrag controller owns these status
	// fields, so on conflict re-fetch and re-apply the computed status rather than
	// dropping it.
	desired := df.Status
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &lll.EtcdDefrag{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(df), latest); err != nil {
			return err
		}
		latest.Status = desired
		return r.Status().Update(ctx, latest)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: defragRequeueAfter}, nil
}

// handleTTL deletes a finished EtcdDefrag once TTLSecondsAfterFinished has
// elapsed, or requeues to delete it later. No TTL means keep it as history.
func (r *EtcdDefragReconciler) handleTTL(ctx context.Context, df *lll.EtcdDefrag) (ctrl.Result, error) {
	if df.Spec.TTLSecondsAfterFinished == nil || df.Status.CompletedAt == nil {
		return ctrl.Result{}, nil
	}
	expiry := df.Status.CompletedAt.Add(time.Duration(*df.Spec.TTLSecondsAfterFinished) * time.Second)
	if now := time.Now(); now.Before(expiry) {
		return ctrl.Result{RequeueAfter: expiry.Sub(now)}, nil
	}
	if err := r.Delete(ctx, df); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	return ctrl.Result{}, nil
}

func (r *EtcdDefragReconciler) event(obj client.Object, eventType, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Event(obj, eventType, reason, msg)
	}
}

// setDefragCondition upserts the DefragChecked condition and reports whether it
// changed — so callers emit an event only on a real transition, not every pass.
func setDefragCondition(df *lll.EtcdDefrag, status metav1.ConditionStatus, reason, msg string) bool {
	want := metav1.Condition{
		Type:               "DefragChecked",
		Status:             status,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: df.Generation,
	}
	for _, existing := range df.Status.Conditions {
		if existing.Type == want.Type {
			if existing.Status == want.Status && existing.Reason == want.Reason &&
				existing.Message == want.Message && existing.ObservedGeneration == want.ObservedGeneration {
				return false
			}
			break
		}
	}
	setCondition(&df.Status.Conditions, want)
	return true
}

func (r *EtcdDefragReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.EtcdClientFactory == nil {
		r.EtcdClientFactory = DefaultEtcdClientFactory
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&lll.EtcdDefrag{}).
		// A wedged member holds a Defragment RPC for up to defragRPCTimeout;
		// with a single worker that would stall every other cluster's run too.
		// oldestActive already serializes runs per cluster, so distinct clusters
		// are safe to reconcile in parallel.
		WithOptions(controller.Options{MaxConcurrentReconciles: defragMaxConcurrentReconciles}).
		Complete(r)
}
