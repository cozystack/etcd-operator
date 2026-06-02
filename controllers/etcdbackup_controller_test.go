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

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

func reconcileBackup(t *testing.T, r *EtcdBackupReconciler, name string) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: name, Namespace: "ns"},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

// First reconcile of a fresh backup stamps Pending so the object shows a
// meaningful phase immediately and the documented lifecycle holds.
func TestBackupReconcile_FirstReconcileSetsPending(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	c, s := newTestClient(t, cluster, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	reconcileBackup(t, r, "b1")

	if got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{}); got.Status.Phase != lll.EtcdBackupStatusPhasePending {
		t.Errorf("phase after first reconcile = %q, want Pending", got.Status.Phase)
	}
	// No Job is created until after Pending is recorded.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "b1-backup", Namespace: "ns"}, &batchv1.Job{}); err == nil {
		t.Error("a Job was created on the Pending-stamping reconcile")
	}
}

func TestBackupReconcile_MissingClusterFails(t *testing.T) {
	backup := s3Backup("b1", "absent")
	c, s := newTestClient(t, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	reconcileBackup(t, r, "b1") // stamps Pending
	reconcileBackup(t, r, "b1") // resolves clusterRef → fails

	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if len(got.Status.Conditions) == 0 || got.Status.Conditions[0].Reason != "ClusterNotFound" {
		t.Errorf("expected ClusterNotFound condition, got %+v", got.Status.Conditions)
	}
}

func TestBackupReconcile_CreatesJobAndStarts(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	c, s := newTestClient(t, cluster, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	reconcileBackup(t, r, "b1") // stamps Pending
	reconcileBackup(t, r, "b1") // creates the Job, moves to Started

	// Job created, owner-referenced by the backup.
	job := mustGet(t, c, "b1-backup", "ns", &batchv1.Job{})
	if len(job.OwnerReferences) == 0 || job.OwnerReferences[0].Name != "b1" {
		t.Errorf("job not owned by backup: %+v", job.OwnerReferences)
	}
	if job.Spec.Template.Spec.Containers[0].Image != "operator:latest" {
		t.Errorf("job image = %q", job.Spec.Template.Spec.Containers[0].Image)
	}

	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseStarted {
		t.Errorf("phase = %q, want Started", got.Status.Phase)
	}

	// Second reconcile with the Job still running must not re-create or error.
	reconcileBackup(t, r, "b1")
	if got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{}); got.Status.Phase != lll.EtcdBackupStatusPhaseStarted {
		t.Errorf("phase after second reconcile = %q, want Started", got.Status.Phase)
	}
}

// With no operator image configured the backup Job would run an empty image
// and never schedule. The reconcile must fail loudly instead of creating it.
func TestBackupReconcile_NoOperatorImageFails(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	c, s := newTestClient(t, cluster, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: ""}

	reconcileBackup(t, r, "b1") // stamps Pending
	reconcileBackup(t, r, "b1") // image guard → fails

	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if len(got.Status.Conditions) == 0 || got.Status.Conditions[0].Reason != "OperatorImageNotConfigured" {
		t.Errorf("expected OperatorImageNotConfigured condition, got %+v", got.Status.Conditions)
	}
	// No Job must have been created.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "b1-backup", Namespace: "ns"}, &batchv1.Job{}); err == nil {
		t.Error("a backup Job was created despite no operator image")
	}
}

// During the enable-auth bootstrap window (spec.auth.enabled set but
// status.authEnabled not yet latched) the controller must NOT create a Job —
// its credential env is frozen at build time, so an anonymous Job would fail
// terminally. It must requeue and wait instead.
func TestBackupReconcile_WaitsForAuthToLatch(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Auth: &lll.AuthSpec{
				Enabled:                  true,
				RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "c1-root"},
			},
		},
		Status: lll.EtcdClusterStatus{AuthEnabled: false}, // not latched yet
	}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhasePending // past the Pending-stamp step
	backup.CreationTimestamp = metav1.Now()                // freshly created — inside the latch window
	c, s := newTestClient(t, cluster, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	res := reconcileBackup(t, r, "b1")
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue while auth is unlatched, got %+v", res)
	}
	// No credential-less Job may have been created.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "b1-backup", Namespace: "ns"}, &batchv1.Job{}); err == nil {
		t.Error("a backup Job was created before auth latched")
	}
	if got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{}); got.Status.Phase == lll.EtcdBackupStatusPhaseFailed {
		t.Error("backup was failed during the auth window; should have requeued")
	}
}

// If the cluster's auth never latches (never converges, or a wrong root
// credentials Secret keeps reconcileAuth failing), the backup must not requeue
// forever. Past backupAuthLatchTimeout it fails terminally with a clear reason
// instead of piling up as a stuck Pending under a CronJob driver.
func TestBackupReconcile_AuthNeverLatchesFails(t *testing.T) {
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Auth: &lll.AuthSpec{
				Enabled:                  true,
				RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: "c1-root"},
			},
		},
		Status: lll.EtcdClusterStatus{AuthEnabled: false}, // never latches
	}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhasePending
	// Created well before the latch deadline — the wait has been exceeded.
	backup.CreationTimestamp = metav1.NewTime(metav1.Now().Add(-2 * backupAuthLatchTimeout))
	c, s := newTestClient(t, cluster, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	res := reconcileBackup(t, r, "b1")
	if res.RequeueAfter > 0 {
		t.Errorf("expected terminal failure (no requeue) past the latch deadline, got %+v", res)
	}
	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if len(got.Status.Conditions) == 0 || got.Status.Conditions[0].Reason != "AuthLatchTimeout" {
		t.Errorf("expected AuthLatchTimeout condition, got %+v", got.Status.Conditions)
	}
	// No credential-less Job may have been created.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "b1-backup", Namespace: "ns"}, &batchv1.Job{}); err == nil {
		t.Error("a backup Job was created after the auth-latch timeout")
	}
}

// A terminal backup must report a self-consistent status: after a real
// Pending → Started → Failed progression, no condition may still be True (the
// in-progress signal must be cleared). Before the single-Ready-condition fix
// the Started condition was set True and never flipped, so a Failed backup
// carried both Started=True and a failure condition — this asserts that can't
// happen.
func TestBackupReconcile_NoStaleInProgressConditionOnFailure(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	c, s := newTestClient(t, cluster, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	reconcileBackup(t, r, "b1") // Pending
	reconcileBackup(t, r, "b1") // creates Job, moves to Started (sets the in-progress condition)

	// The Job now fails (e.g. the agent exited non-zero).
	job := mustGet(t, c, "b1-backup", "ns", &batchv1.Job{})
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue}}
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatalf("update job status: %v", err)
	}

	reconcileBackup(t, r, "b1") // observes the failed Job → Failed

	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	for _, cond := range got.Status.Conditions {
		if cond.Status == metav1.ConditionTrue {
			t.Errorf("terminal Failed backup has a lingering True condition %q (reason %q); the in-progress signal must be cleared",
				cond.Type, cond.Reason)
		}
	}
}

// The success path: a succeeded Job whose pod log carries the agent's marker
// must move the backup to Complete with status.snapshot populated from the
// parsed marker. This exercises extractSnapshot end-to-end — the pod-selection
// loop, the backup-agent container target, and wiring the parsed snapshot into
// status — which the per-phase and parseSnapshotMarker unit tests don't cover.
func TestBackupReconcile_SucceededJobRecordsSnapshotAndCompletes(t *testing.T) {
	const hash = "abc123def4567890abc123def4567890abc123def4567890abc123def4567890"
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhaseStarted
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b1-backup", Namespace: "ns"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "b1-backup-xyz", Namespace: "ns",
			Labels: map[string]string{"batch.kubernetes.io/job-name": "b1-backup"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodSucceeded},
	}
	c, s := newTestClient(t, cluster, backup, job, pod)

	var gotContainer string
	r := &EtcdBackupReconciler{
		Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest",
		podLogReader: func(_ context.Context, ns, podName, container string) (string, error) {
			gotContainer = container
			return `starting backup
snapshot uploaded: uri="s3://etcd/backups/b1.db" size=4096 sha256=` + hash + "\ndone\n", nil
		},
	}

	reconcileBackup(t, r, "b1")

	if gotContainer != "backup-agent" {
		t.Errorf("read log from container %q, want backup-agent", gotContainer)
	}
	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseComplete {
		t.Fatalf("phase = %q, want Complete", got.Status.Phase)
	}
	if got.Status.Snapshot == nil {
		t.Fatal("status.snapshot not populated on Complete")
	}
	if got.Status.Snapshot.URI != "s3://etcd/backups/b1.db" || got.Status.Snapshot.SizeBytes != 4096 ||
		got.Status.Snapshot.Checksum != "sha256:"+hash {
		t.Errorf("status.snapshot = %+v, want the parsed marker coordinates", got.Status.Snapshot)
	}
	// The Complete condition must be the single Ready condition, status True.
	if len(got.Status.Conditions) != 1 || got.Status.Conditions[0].Type != lll.BackupReady ||
		got.Status.Conditions[0].Status != metav1.ConditionTrue {
		t.Errorf("conditions = %+v, want one Ready=True", got.Status.Conditions)
	}
}

// A Job reporting Succeeded≥1 but whose pod hasn't reached PodSucceeded in the
// (uncached) lister yet must NOT complete the backup — extractSnapshot finds no
// succeeded pod, so the reconcile requeues and stays Started rather than
// recording an empty snapshot. Covers extractSnapshot's "no succeeded pod" path.
func TestBackupReconcile_SucceededJobNoSucceededPodRequeues(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhaseStarted
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b1-backup", Namespace: "ns"},
		Status:     batchv1.JobStatus{Succeeded: 1},
	}
	// Pod for the job exists but is still Running (not Succeeded).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "b1-backup-xyz", Namespace: "ns",
			Labels: map[string]string{"batch.kubernetes.io/job-name": "b1-backup"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	c, s := newTestClient(t, cluster, backup, job, pod)
	logCalled := false
	r := &EtcdBackupReconciler{
		Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest",
		podLogReader: func(context.Context, string, string, string) (string, error) {
			logCalled = true
			return "", nil
		},
	}

	res := reconcileBackup(t, r, "b1")
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue while no pod has succeeded, got %+v", res)
	}
	if logCalled {
		t.Error("read a pod log though no pod had succeeded yet")
	}
	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseStarted {
		t.Errorf("phase = %q, want Started (must not complete without a succeeded pod)", got.Status.Phase)
	}
	if got.Status.Snapshot != nil {
		t.Errorf("status.snapshot populated without a succeeded pod: %+v", got.Status.Snapshot)
	}
}

func TestBackupReconcile_JobFailedSetsFailed(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhaseStarted
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b1-backup", Namespace: "ns"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue},
		}},
	}
	c, s := newTestClient(t, cluster, backup, job)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	reconcileBackup(t, r, "b1")

	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if len(got.Status.Conditions) == 0 || got.Status.Conditions[0].Reason != "JobFailed" {
		t.Errorf("expected JobFailed condition, got %+v", got.Status.Conditions)
	}
}

// A backup Pod that hangs (e.g. dialing a parked cluster) is killed when the
// Job's ActiveDeadlineSeconds elapses — Kubernetes marks the Job Failed with
// reason DeadlineExceeded. The controller must turn that into a terminal
// EtcdBackup failure (not requeue in Started forever). This is the path that
// backstops a hung dial/snapshot, which BackoffLimit alone cannot catch.
func TestBackupReconcile_JobActiveDeadlineExceededFails(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhaseStarted
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "b1-backup", Namespace: "ns"},
		Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{
			{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"},
		}},
	}
	c, s := newTestClient(t, cluster, backup, job)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	res := reconcileBackup(t, r, "b1")
	if res.RequeueAfter > 0 {
		t.Errorf("expected terminal failure (no requeue) on a deadline-exceeded Job, got %+v", res)
	}
	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
}

// A backup already in Started whose Job has vanished (GC'd after TTL before
// the marker was read, or deleted) must fail terminally — NOT recreate the
// Job (which would take a duplicate snapshot) and NOT requeue forever.
func TestBackupReconcile_StartedButJobGoneFails(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhaseStarted
	// Note: no Job object in the client — it's "gone".
	c, s := newTestClient(t, cluster, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c, OperatorImage: "operator:latest"}

	reconcileBackup(t, r, "b1")

	got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{})
	if got.Status.Phase != lll.EtcdBackupStatusPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if len(got.Status.Conditions) == 0 || got.Status.Conditions[0].Reason != "BackupJobLost" {
		t.Errorf("expected BackupJobLost condition, got %+v", got.Status.Conditions)
	}
	// Crucially, no replacement Job may have been created (no re-snapshot).
	if err := c.Get(context.Background(), types.NamespacedName{Name: "b1-backup", Namespace: "ns"}, &batchv1.Job{}); err == nil {
		t.Error("a replacement backup Job was created; would take a duplicate snapshot")
	}
}

// Right after the Job is created the cached client may briefly not see it. A
// Started backup whose Job is missing from the cache but present via the
// uncached APIReader must requeue — NOT be failed as BackupJobLost.
func TestBackupReconcile_StartedJobCacheLagRequeues(t *testing.T) {
	cluster := &lll.EtcdCluster{ObjectMeta: metav1.ObjectMeta{Name: "c1", Namespace: "ns"}}
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhaseStarted
	// Cached client: cluster + backup, but the Job hasn't landed in the cache.
	cached, s := newTestClient(t, cluster, backup)
	// Uncached reader: the Job exists in the apiserver.
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "b1-backup", Namespace: "ns"}}
	apiReader, _ := newTestClient(t, job)

	r := &EtcdBackupReconciler{Client: cached, Scheme: s, APIReader: apiReader, OperatorImage: "operator:latest"}

	res := reconcileBackup(t, r, "b1")
	if res.RequeueAfter <= 0 {
		t.Errorf("expected a requeue while the Job is cache-lagged, got %+v", res)
	}
	if got := mustGet(t, cached, "b1", "ns", &lll.EtcdBackup{}); got.Status.Phase != lll.EtcdBackupStatusPhaseStarted {
		t.Errorf("phase = %q, want Started (must not fail on cache lag)", got.Status.Phase)
	}
}

func TestBackupReconcile_TerminalPhaseSticky(t *testing.T) {
	backup := s3Backup("b1", "c1")
	backup.Status.Phase = lll.EtcdBackupStatusPhaseComplete
	c, s := newTestClient(t, backup)
	r := &EtcdBackupReconciler{Client: c, Scheme: s, APIReader: c}

	reconcileBackup(t, r, "b1")

	// No Job should be created for a completed backup.
	if err := c.Get(context.Background(), types.NamespacedName{Name: "b1-backup", Namespace: "ns"}, &batchv1.Job{}); err == nil {
		t.Error("a Job was created for an already-Complete backup")
	}
	if got := mustGet(t, c, "b1", "ns", &lll.EtcdBackup{}); got.Status.Phase != lll.EtcdBackupStatusPhaseComplete {
		t.Errorf("phase mutated to %q, want sticky Complete", got.Status.Phase)
	}
}

func TestParseSnapshotMarker(t *testing.T) {
	const hash = "abc123def4567890abc123def4567890abc123def4567890abc123def4567890"

	t.Run("uploaded", func(t *testing.T) {
		log := "starting backup\nsnapshot uploaded: uri=\"s3://etcd/backups/b1.db\" size=4096 sha256=" + hash + "\ndone\n"
		snap, err := parseSnapshotMarker(log)
		if err != nil {
			t.Fatalf("parseSnapshotMarker: %v", err)
		}
		if snap.URI != "s3://etcd/backups/b1.db" {
			t.Errorf("URI = %q", snap.URI)
		}
		if snap.SizeBytes != 4096 {
			t.Errorf("SizeBytes = %d, want 4096", snap.SizeBytes)
		}
		if snap.Checksum != "sha256:"+hash {
			t.Errorf("Checksum = %q", snap.Checksum)
		}
	})

	t.Run("pvc file uri", func(t *testing.T) {
		log := "snapshot uploaded: uri=\"file:///backup/data/b1.db\" size=10 sha256=" + hash
		snap, err := parseSnapshotMarker(log)
		if err != nil {
			t.Fatalf("parseSnapshotMarker: %v", err)
		}
		if snap.URI != "file:///backup/data/b1.db" {
			t.Errorf("URI = %q", snap.URI)
		}
	})

	t.Run("no marker", func(t *testing.T) {
		if _, err := parseSnapshotMarker("nothing useful here\n"); err == nil {
			t.Error("expected error when marker absent")
		}
	})

	t.Run("malformed hash", func(t *testing.T) {
		// Too-short hash must not match the 64-hex requirement.
		log := "snapshot uploaded: uri=\"s3://b/k.db\" size=1 sha256=deadbeef"
		if _, err := parseSnapshotMarker(log); err == nil {
			t.Error("expected error for malformed checksum")
		}
	})

	t.Run("int64-overflowing size errors (not silently zero)", func(t *testing.T) {
		// The regex matches arbitrary digits; a size past int64 must surface an
		// error rather than be discarded into a misleading sizeBytes=0.
		log := "snapshot uploaded: uri=\"s3://b/k.db\" size=99999999999999999999999999 sha256=" + hash
		if _, err := parseSnapshotMarker(log); err == nil {
			t.Error("expected error for an int64-overflowing size")
		}
	})
}
