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

// snapshotMarker matches the line the backup-agent prints on success:
//
//	snapshot uploaded: uri="s3://bucket/key.db" size=12345 sha256=<64-hex>
var snapshotMarker = regexp.MustCompile(`snapshot uploaded: uri="(.+?)" size=(\d+) sha256=([a-f0-9]{64})`)

// backupAuthLatchTimeout bounds how long an EtcdBackup waits for its target
// cluster's auth to latch (status.authEnabled) before failing terminally.
// Measured from the backup's creation. It is set comfortably above the
// cluster's default progressDeadline (600s) so a normally-converging,
// auth-enabling cluster always makes the window — but a cluster that never
// latches (never converges, or a wrong spec.auth.rootCredentialsSecretRef
// keeps reconcileAuth failing) fails the backup with a clear reason instead of
// requeueing forever and piling up Pending backups under a CronJob driver.
const backupAuthLatchTimeout = 15 * time.Minute

// EtcdBackupReconciler reconciles EtcdBackup objects by running a snapshot Job.
type EtcdBackupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// APIReader is the uncached reader, used to list the Job's Pods without a
	// broad cache/RBAC footprint.
	APIReader client.Reader
	// Clientset reads Pod logs (the controller-runtime client cannot).
	Clientset kubernetes.Interface
	// OperatorImage is the operator's own image; the backup agent runs from it.
	OperatorImage string
	// podLogReader fetches a pod container's log. Defaults to the Clientset-based
	// reader; injectable in tests (the fake Clientset's GetLogs stream is not
	// content-controllable) to exercise the Started→Complete marker-parsing path.
	podLogReader func(ctx context.Context, namespace, pod, container string) (string, error)
}

//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdbackups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdbackups/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=etcd-operator.cozystack.io,resources=etcdbackups/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch
//+kubebuilder:rbac:groups="",resources=pods/log,verbs=get

func (r *EtcdBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	backup := &lll.EtcdBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Terminal phases are sticky — a backup is a one-shot record.
	if backup.Status.Phase == lll.EtcdBackupStatusPhaseComplete ||
		backup.Status.Phase == lll.EtcdBackupStatusPhaseFailed {
		return ctrl.Result{}, nil
	}

	// Stamp Pending on first observation so `kubectl get etcdbackup` shows a
	// meaningful phase immediately and the documented lifecycle
	// (Pending → Started → Complete|Failed) is true rather than aspirational.
	if backup.Status.Phase == "" {
		backup.Status.Phase = lll.EtcdBackupStatusPhasePending
		if err := r.Status().Update(ctx, backup); err != nil {
			if apierrors.IsConflict(err) {
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// The backup Job runs the operator's own image. Without it the Job would
	// be created with an empty image and the Pod would never schedule, leaving
	// the backup wedged in Started. Fail loudly with a clear reason instead.
	if r.OperatorImage == "" {
		return r.fail(ctx, backup, "OperatorImageNotConfigured",
			"operator image is not configured; set --operator-image / OPERATOR_IMAGE on the manager")
	}

	cluster := &lll.EtcdCluster{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: backup.Namespace, Name: backup.Spec.ClusterRef.Name}, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return r.fail(ctx, backup, "ClusterNotFound",
				fmt.Sprintf("EtcdCluster %q not found", backup.Spec.ClusterRef.Name))
		}
		return ctrl.Result{}, err
	}

	// The Job's env (root credentials) is captured once, at build time. If we
	// build it during the enable-auth bootstrap window — spec.auth.enabled
	// is set but status.authEnabled hasn't latched yet (or a stale/cache-lagged
	// read says false) — the Job freezes in anonymous creds, etcd rejects it,
	// and the backup fails terminally with no recovery. Wait for auth to latch
	// (mirrors resolveEtcdCredentials' gate) by requeueing rather than launching.
	if cluster.Spec.Auth != nil && cluster.Spec.Auth.Enabled && !cluster.Status.AuthEnabled {
		if time.Since(backup.CreationTimestamp.Time) > backupAuthLatchTimeout {
			return r.fail(ctx, backup, "AuthLatchTimeout",
				fmt.Sprintf("cluster %q auth did not latch (status.authEnabled) within %s; "+
					"the cluster may not be converging, or spec.auth.rootCredentialsSecretRef may be wrong",
					cluster.Name, backupAuthLatchTimeout))
		}
		logger.Info("waiting for cluster auth to latch before backing up", "cluster", cluster.Name)
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Ensure the snapshot Job exists.
	job := &batchv1.Job{}
	jobKey := types.NamespacedName{Namespace: backup.Namespace, Name: backupJobName(backup)}
	err := r.Get(ctx, jobKey, job)
	switch {
	case apierrors.IsNotFound(err):
		// If we already launched the Job (phase Started) and it is now gone,
		// it ran and was garbage-collected (TTLSecondsAfterFinished) — or
		// deleted — before we recorded a snapshot. Recreating it would take a
		// SECOND snapshot and the backup could loop forever; instead fail
		// terminally. Confirm via the uncached reader first so a cache lag
		// right after creation isn't mistaken for a lost Job.
		if backup.Status.Phase == lll.EtcdBackupStatusPhaseStarted {
			live := &batchv1.Job{}
			liveErr := r.APIReader.Get(ctx, jobKey, live)
			if liveErr == nil {
				// Cache lag — the Job does exist. Try again shortly.
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			if !apierrors.IsNotFound(liveErr) {
				return ctrl.Result{}, liveErr
			}
			return r.fail(ctx, backup, "BackupJobLost",
				"backup Job completed or was removed before its snapshot could be recorded; not re-running to avoid a duplicate snapshot")
		}
		job = buildBackupJob(backup, cluster, r.OperatorImage)
		if err := controllerutil.SetControllerReference(backup, job, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, job); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("created backup job", "job", job.Name)
		return r.setPhase(ctx, backup, lll.EtcdBackupStatusPhaseStarted,
			metav1.ConditionFalse, "JobCreated", "snapshot job created")
	case err != nil:
		return ctrl.Result{}, err
	}

	// Job exists — inspect its terminal state.
	if jobFailed(job) {
		return r.fail(ctx, backup, "JobFailed", "backup job failed")
	}
	if job.Status.Succeeded < 1 {
		// Still running.
		if backup.Status.Phase != lll.EtcdBackupStatusPhaseStarted {
			if _, err := r.setPhase(ctx, backup, lll.EtcdBackupStatusPhaseStarted,
				metav1.ConditionFalse, "JobRunning", "snapshot job running"); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Job succeeded — extract the snapshot coordinates from the agent's log.
	snap, err := r.extractSnapshot(ctx, backup, job)
	if err != nil {
		// Log may lag Job completion; retry within a short window.
		logger.Info("snapshot marker not yet available, retrying", "err", err.Error())
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	backup.Status.Snapshot = snap
	return r.setPhase(ctx, backup, lll.EtcdBackupStatusPhaseComplete,
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
func (r *EtcdBackupReconciler) extractSnapshot(ctx context.Context, backup *lll.EtcdBackup, job *batchv1.Job) (*lll.BackupSnapshot, error) {
	pods := &corev1.PodList{}
	if err := r.APIReader.List(ctx, pods,
		client.InNamespace(backup.Namespace),
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

	logText, err := r.readPodLog(ctx, backup.Namespace, podName, "backup-agent")
	if err != nil {
		return nil, err
	}
	return parseSnapshotMarker(logText)
}

// readPodLog returns the named pod container's log. Split out (and overridable
// via podLogReader) so the Started→Complete path is testable without a live log
// stream — the fake Clientset's GetLogs returns fixed, uncontrollable content.
func (r *EtcdBackupReconciler) readPodLog(ctx context.Context, namespace, pod, container string) (string, error) {
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
func parseSnapshotMarker(logText string) (*lll.BackupSnapshot, error) {
	m := snapshotMarker.FindStringSubmatch(logText)
	if m == nil {
		return nil, fmt.Errorf("snapshot marker not found in log")
	}
	// The regex guarantees digits, so this only fails on int64 overflow — surface
	// it rather than silently recording size 0 in status.snapshot.sizeBytes.
	size, err := strconv.ParseInt(m[2], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse snapshot size %q: %w", m[2], err)
	}
	return &lll.BackupSnapshot{
		URI:       m[1],
		SizeBytes: size,
		Checksum:  "sha256:" + m[3],
	}, nil
}

func (r *EtcdBackupReconciler) fail(ctx context.Context, backup *lll.EtcdBackup, reason, msg string) (ctrl.Result, error) {
	_, err := r.setPhase(ctx, backup, lll.EtcdBackupStatusPhaseFailed, metav1.ConditionFalse, reason, msg)
	return ctrl.Result{}, err
}

// setPhase records the lifecycle phase and the single Ready condition (Status
// True only in the terminal Complete phase). Using one condition keeps the
// status self-consistent: there is never a stale "Started=True" left on a
// Failed/Complete backup.
func (r *EtcdBackupReconciler) setPhase(ctx context.Context, backup *lll.EtcdBackup,
	phase lll.EtcdBackupStatusPhase, condStatus metav1.ConditionStatus, reason, msg string,
) (ctrl.Result, error) {
	backup.Status.Phase = phase
	setCondition(&backup.Status.Conditions, metav1.Condition{
		Type: lll.BackupReady, Status: condStatus, Reason: reason, Message: msg,
		ObservedGeneration: backup.Generation,
	})
	if err := r.Status().Update(ctx, backup); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *EtcdBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&lll.EtcdBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
