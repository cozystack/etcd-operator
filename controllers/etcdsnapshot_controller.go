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
	"io"
	"regexp"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// snapshotMarker matches the line the snapshot-agent prints on success:
//
//	snapshot uploaded: uri="s3://bucket/key.db" size=12345 sha256=<64-hex>
var snapshotMarker = regexp.MustCompile(`snapshot uploaded: uri="(.+?)" size=(\d+) sha256=([a-f0-9]{64})`)

// snapshotAuthLatchTimeout bounds how long an EtcdSnapshot waits for its target
// cluster's auth to latch (status.authEnabled) before failing terminally.
// Measured from the snapshot's creation. It is set comfortably above the
// cluster's default progressDeadline (600s) so a normally-converging,
// auth-enabling cluster always makes the window — but a cluster that never
// latches (never converges, or a wrong spec.auth.rootCredentialsSecretRef
// keeps reconcileAuth failing) fails the snapshot with a clear reason instead of
// requeueing forever and piling up Pending snapshots under a CronJob driver.
const snapshotAuthLatchTimeout = 15 * time.Minute

// EtcdSnapshotReconciler reconciles EtcdSnapshot objects by running a snapshot Job.
type EtcdSnapshotReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader is the uncached reader, used to list the Job's Pods without a
	// broad cache/RBAC footprint.
	APIReader client.Reader
	// Clientset reads Pod logs (the controller-runtime client cannot).
	Clientset kubernetes.Interface
	// OperatorImage is the operator's own image; the snapshot agent runs from it.
	OperatorImage string
	// podLogReader fetches a pod container's log. Defaults to the Clientset-based
	// reader; injectable in tests (the fake Clientset's GetLogs stream is not
	// content-controllable) to exercise the Started→Complete marker-parsing path.
	podLogReader func(ctx context.Context, namespace, pod, container string) (string, error)
}

//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdsnapshots,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdsnapshots/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdsnapshots/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods/log,verbs=get

func (r *EtcdSnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	snapshot := &lll.EtcdSnapshot{}
	if err := r.Get(ctx, req.NamespacedName, snapshot); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal phases are sticky — a snapshot is a one-shot record.
	if snapshot.Status.Phase == lll.EtcdSnapshotStatusPhaseComplete ||
		snapshot.Status.Phase == lll.EtcdSnapshotStatusPhaseFailed {
		return ctrl.Result{}, nil
	}

	// Stamp Pending on first observation so `kubectl get etcdsnapshot` shows a
	// meaningful phase immediately and the documented lifecycle
	// (Pending → Started → Complete|Failed) is true rather than aspirational.
	if snapshot.Status.Phase == "" {
		snapshot.Status.Phase = lll.EtcdSnapshotStatusPhasePending
		if err := r.Status().Update(ctx, snapshot); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// The snapshot Job runs the operator's own image. Without it the Job would
	// be created with an empty image and the Pod would never schedule, leaving
	// the snapshot wedged in Started. Fail loudly with a clear reason instead.
	if r.OperatorImage == "" {
		return r.fail(ctx, snapshot, "OperatorImageNotConfigured",
			"operator image is not configured; set --operator-image / OPERATOR_IMAGE on the manager")
	}

	cluster := &lll.EtcdCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: snapshot.Namespace, Name: snapshot.Spec.ClusterRef.Name}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.fail(ctx, snapshot, "ClusterNotFound",
				fmt.Sprintf("EtcdCluster %q not found", snapshot.Spec.ClusterRef.Name))
		}
		return ctrl.Result{}, err
	}

	// The Job's env (root credentials) is captured once, at build time. If we
	// build it during the enable-auth bootstrap window — spec.auth.enabled
	// is set but status.authEnabled hasn't latched yet (or a stale/cache-lagged
	// read says false) — the Job freezes in anonymous creds, etcd rejects it,
	// and the snapshot fails terminally with no recovery. Wait for auth to latch
	// (mirrors resolveEtcdCredentials' gate) by requeueing rather than launching.
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.Enabled && !cluster.Status.AuthEnabled {
		if time.Since(snapshot.CreationTimestamp.Time) > snapshotAuthLatchTimeout {
			return r.fail(ctx, snapshot, "AuthLatchTimeout",
				fmt.Sprintf("cluster %q auth did not latch (status.authEnabled) within %s; "+
					"the cluster may not be converging, or spec.auth.rootCredentialsSecretRef may be wrong",
					cluster.Name, snapshotAuthLatchTimeout))
		}
		logger.Info("waiting for cluster auth to latch before backing up", "cluster", cluster.Name)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Ensure the snapshot Job exists.
	job := &batchv1.Job{}
	jobKey := types.NamespacedName{Namespace: snapshot.Namespace, Name: snapshotJobName(snapshot)}
	err := r.Get(ctx, jobKey, job)
	switch {
	case apierrors.IsNotFound(err):
		// If we already launched the Job (phase Started) and it is now gone,
		// it ran and was garbage-collected (TTLSecondsAfterFinished) — or
		// deleted — before we recorded a snapshot. Recreating it would take a
		// SECOND snapshot and the snapshot could loop forever; instead fail
		// terminally. Confirm via the uncached reader first so a cache lag
		// right after creation isn't mistaken for a lost Job.
		if snapshot.Status.Phase == lll.EtcdSnapshotStatusPhaseStarted {
			live := &batchv1.Job{}
			liveErr := r.APIReader.Get(ctx, jobKey, live)
			if liveErr == nil {
				// Cache lag — the Job does exist. Try again shortly.
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			if !apierrors.IsNotFound(liveErr) {
				return ctrl.Result{}, liveErr
			}
			return r.fail(ctx, snapshot, "SnapshotJobLost",
				"snapshot Job completed or was removed before its snapshot could be recorded; not re-running to avoid a duplicate snapshot")
		}
		job = buildSnapshotJob(snapshot, cluster, r.OperatorImage)
		if err := controllerutil.SetControllerReference(snapshot, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("created snapshot job", "job", job.Name)
		return r.setPhase(ctx, snapshot, lll.EtcdSnapshotStatusPhaseStarted,
			metav1.ConditionFalse, "JobCreated", "snapshot job created")
	case err != nil:
		return ctrl.Result{}, err
	}

	// Job exists — inspect its terminal state.
	if jobFailed(job) {
		return r.fail(ctx, snapshot, "JobFailed", "snapshot job failed")
	}
	if job.Status.Succeeded < 1 {
		// Still running.
		if snapshot.Status.Phase != lll.EtcdSnapshotStatusPhaseStarted {
			if _, err := r.setPhase(ctx, snapshot, lll.EtcdSnapshotStatusPhaseStarted,
				metav1.ConditionFalse, "JobRunning", "snapshot job running"); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Job succeeded — extract the snapshot coordinates from the agent's log.
	snap, err := r.extractSnapshot(ctx, snapshot, job)
	if err != nil {
		// Log may lag Job completion; retry within a short window.
		logger.Info("snapshot marker not yet available, retrying", "err", err.Error())
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	snapshot.Status.Artifact = snap
	return r.setPhase(ctx, snapshot, lll.EtcdSnapshotStatusPhaseComplete,
		metav1.ConditionTrue, "SnapshotStored", "snapshot captured and stored")
}

func jobFailed(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// extractSnapshot lists the Job's Pods, picks a succeeded one, and scans its
// log for the agent's marker line.
func (r *EtcdSnapshotReconciler) extractSnapshot(ctx context.Context, snapshot *lll.EtcdSnapshot, job *batchv1.Job) (*lll.SnapshotArtifact, error) {
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods,
		client.InNamespace(snapshot.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name},
	); err != nil {
		return nil, err
	}
	var podName string
	for i := range pods.Items {
		if pods.Items[i].Status.Phase == corev1.PodSucceeded {
			podName = pods.Items[i].Name
			break
		}
	}
	if podName == "" {
		return nil, fmt.Errorf("no succeeded pod for job %s yet", job.Name)
	}

	logText, err := r.readPodLog(ctx, snapshot.Namespace, podName, "snapshot-agent")
	if err != nil {
		return nil, err
	}
	return parseSnapshotMarker(logText)
}

// readPodLog returns the named pod container's log. Split out (and overridable
// via podLogReader) so the Started→Complete path is testable without a live log
// stream — the fake Clientset's GetLogs returns fixed, uncontrollable content.
func (r *EtcdSnapshotReconciler) readPodLog(ctx context.Context, namespace, pod, container string) (string, error) {
	if r.podLogReader != nil {
		return r.podLogReader(ctx, namespace, pod, container)
	}
	stream, err := r.Clientset.CoreV1().Pods(namespace).
		GetLogs(pod, &corev1.PodLogOptions{Container: container}).Stream(ctx)
	if err != nil {
		return "", err
	}
	defer stream.Close()
	data, err := io.ReadAll(stream)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// parseSnapshotMarker extracts the snapshot coordinates from agent log output.
func parseSnapshotMarker(logText string) (*lll.SnapshotArtifact, error) {
	m := snapshotMarker.FindStringSubmatch(logText)
	if m == nil {
		return nil, fmt.Errorf("snapshot marker not found in log")
	}
	// The regex guarantees digits, so this only fails on int64 overflow — surface
	// it rather than silently recording size 0 in status.artifact.sizeBytes.
	size, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse snapshot size %q: %w", m[2], err)
	}
	return &lll.SnapshotArtifact{
		URI:       m[1],
		SizeBytes: size,
		Checksum:  "sha256:" + m[3],
	}, nil
}

func (r *EtcdSnapshotReconciler) fail(ctx context.Context, snapshot *lll.EtcdSnapshot, reason, msg string) (ctrl.Result, error) {
	_, err := r.setPhase(ctx, snapshot, lll.EtcdSnapshotStatusPhaseFailed, metav1.ConditionFalse, reason, msg)
	return ctrl.Result{}, err
}

// setPhase records the lifecycle phase and the single Ready condition (Status
// True only in the terminal Complete phase). Using one condition keeps the
// status self-consistent: there is never a stale "Started=True" left on a
// Failed/Complete snapshot.
func (r *EtcdSnapshotReconciler) setPhase(ctx context.Context, snapshot *lll.EtcdSnapshot,
	phase lll.EtcdSnapshotStatusPhase, condStatus metav1.ConditionStatus, reason, msg string,
) (ctrl.Result, error) {
	snapshot.Status.Phase = phase
	setCondition(&snapshot.Status.Conditions, metav1.Condition{
		Type: lll.SnapshotReady, Status: condStatus, Reason: reason, Message: msg,
		ObservedGeneration: snapshot.Generation,
	})
	if err := r.Status().Update(ctx, snapshot); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *EtcdSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lll.EtcdSnapshot{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
