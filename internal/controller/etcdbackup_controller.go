/*
Copyright 2024 The etcd-operator Authors.

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

package controller

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
	"github.com/aenix-io/etcd-operator/internal/log"
)

// PodLogStreamer fetches a pod's log stream. Splitting this out keeps
// the post-success snapshot scan testable without spinning up a real
// apiserver (client-go's fake Clientset hardcodes a fixed log body,
// which is fine for production but useless for pinning the marker
// parse). Production wires this from the typed Clientset at SetupWith
// Manager; tests substitute a stub that returns a strings.Reader.
type PodLogStreamer func(ctx context.Context, namespace, podName, container string) (io.ReadCloser, error)

// EtcdBackupReconciler reconciles a EtcdBackup object
type EtcdBackupReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	OperatorImage string
	// APIReader is an UN-cached reader used for the per-Job
	// backup-agent pod List in extractSnapshotFromJob. Routing that
	// read through the default cached Client would force
	// controller-runtime to start a cluster-wide Pod informer (and
	// every typed cached read tries to start one), which requires
	// namespace-wide pods/watch RBAC — over-broad for a per-Job
	// one-off lookup that happens at most once per EtcdBackup. The
	// APIReader (mgr.GetAPIReader() in production) talks to the
	// apiserver directly, so the controller needs only get;list on
	// pods. Tests that wire a fake Client may leave this nil; the
	// pod-list path falls back to Client in that case.
	APIReader client.Reader
	// Clientset is used to stream backup-agent pod logs after the
	// backup Job succeeds so the controller can populate
	// EtcdBackup.status.snapshot with the exact URI/size/sha256 the
	// agent reported. The spec destination carries only the prefix;
	// BACKUP_INCLUDE_REVISION / BACKUP_TIMESTAMP rewrite the suffix
	// inside the agent. The typed controller-runtime client cannot
	// stream sub-resources like pods/log, hence the typed Clientset
	// alongside Client.
	Clientset kubernetes.Interface
	// LogStreamer optionally overrides the pod-log fetch. When nil, we
	// derive a streamer from Clientset on first use.
	LogStreamer PodLogStreamer
}

// snapshotMarkerRegexp matches the terminal "snapshot uploaded: ..."
// (S3) / "snapshot written: ..." (PVC) marker emitted by
// cmd/backup-agent/main.go after a successful upload/write. The
// shared shape lets one parser cover both destinations.
//
// Format pinned by cmd/backup-agent/main.go's uploadToS3 and
// writeToPVC:
//
//	snapshot uploaded: uri="s3://<bucket>/<key>" size=<int> sha256=<hex>
//	snapshot written: uri="file:///<abs-path>"  size=<int> sha256=<hex>
//
// The URI is a Go-quoted string (the agent emits it via %q) so S3
// keys / PVC paths containing whitespace or other shell-active
// characters survive the trip from agent log to status.snapshot.uri.
// scanBackupAgentLog runs strconv.Unquote on capture group 1 to
// recover the decoded form. The capture is `+` (not `*`) so an
// accidental `uri=""` is rejected at scan time rather than slipping
// through to a Status().Update() that the CRD pattern would refuse
// — keeping the regex contract aligned with the CRD's
// `^(s3|file)://.+`. Token order is load-bearing — adding a token at
// the end stays compatible (the regex is intentionally unanchored at
// the right); renaming or reordering breaks parsing and trips
// scanBackupAgentLog's "no marker found" terminal path.
//
// The sha256 capture is anchored to exactly 64 hex chars AND
// followed by either whitespace (a trailing token like
// "revision=N") or end-of-line. Without the trailing anchor a
// malformed agent build that emitted 80 hex chars for sha256
// would pass the first 64 as a "valid" hash and silently land a
// meaningless truncation on status.snapshot.checksum — defeating
// the integrity contract `[a-f0-9]{32,128}` is intended to enforce.
// The agent always emits a full sha256 (32 bytes = 64 hex chars);
// a shorter run means the log line was truncated mid-marker (e.g.
// bufio's pathological isPrefix split on an oversize line, or a
// forcibly torn stream) and is correctly rejected. If the agent
// ever adopts a different algorithm with a different output length,
// this regex (and the agent's emitter) must be updated in lockstep.
var snapshotMarkerRegexp = regexp.MustCompile(
	`^snapshot (?:uploaded|written): uri=("(?:[^"\\]|\\.)+") size=(\d+) sha256=([a-f0-9]{64})(?:\s|$)`,
)

// snapshotURIPrefixRegexp is the second-line defense after marker
// parsing: even if a malformed agent build emitted a syntactically
// valid quoted string that doesn't match the CRD pattern (e.g.
// uri="https://..."), reject it on the controller side so we return
// "no terminal snapshot marker found" and stay in the retry loop
// rather than failing the apiserver Update with a validation error
// and burning the retry budget on Status() conflicts.
//
// The regex is compiled from etcdaenixiov1alpha1.SnapshotURIPrefix
// Pattern — the single source of truth shared with the kubebuilder
// marker on BackupSnapshot.URI. If the CRD pattern evolves (e.g. to
// admit gs:// / azure:// schemes), updating the constant updates the
// controller automatically; the marker text must be updated alongside
// (kubebuilder can't reference Go constants).
var snapshotURIPrefixRegexp = regexp.MustCompile(etcdaenixiov1alpha1.SnapshotURIPrefixPattern)

// extractSnapshotTimeout caps how long the controller waits for the
// apiserver to deliver a pod's log stream (open + read-to-EOF). The
// scanner reads the full log because "latest marker wins" — a single
// short terminal line is the common case but log volume can grow if
// the agent prints progress / debug output, and apiserver pod-log
// proxy throughput on busy clusters is often well under 1 MiB/s.
// 2 minutes is comfortably below the Job's ActiveDeadlineSeconds
// (900s, factory/backup_job.go) and the pod TTL (600s), so a stuck
// stream still yields to the workqueue's exponential backoff long
// before the natural retry bound (pod GC) elapses.
const extractSnapshotTimeout = 2 * time.Minute

// extractSnapshotRetryInterval is how long to wait before retrying
// pod-log extraction when the agent pod exists for a Succeeded Job
// but has not yet transitioned to PodSucceeded in the controller's
// view (see errPodNotReady). Short enough that the typical
// sub-second watch-cache lag resolves on the first or second retry;
// the natural upper bound is the Job's pod TTL
// (TTLSecondsAfterFinished=600s, set in factory/backup_job.go) —
// once the pod is GC'd, extraction returns errPodGone and the
// controller finalizes immediately rather than requeuing forever.
const extractSnapshotRetryInterval = 5 * time.Second

// markerFlushGracePeriod is the window after Job completion during
// which an errNoMarker result is treated as the kubelet/runtime log
// flush still being in progress rather than a permanently missing
// marker. The agent prints its terminal "snapshot written/uploaded"
// line as the last thing before exit, but the CRI runtime (notably
// containerd in kind) can report a container as Succeeded a few
// hundred milliseconds before its stdout writer goroutine has
// drained the pipe to the on-disk container log — so the first
// GetLogs call after we observe Job.Status.Succeeded can EOF before
// the marker line lands. Once Job.Status.CompletionTime is older
// than this grace, any further "no marker" reads are taken at face
// value (the agent really never emitted the marker — which the
// agent's own code makes unreachable on a clean exit, so the only
// real path to this branch is a force-truncated log) and the
// controller finalizes Phase=Complete with status.snapshot=nil.
const markerFlushGracePeriod = 30 * time.Second

// Sentinel errors classify extractSnapshotFromJob failures so the
// caller can branch on root cause instead of swallowing every error
// into a single "retry-or-finalize" decision. Wrapped errors carry
// the underlying cause; callers use errors.Is to match.
var (
	// errPodGone is returned when no pods match the Job's selector.
	// Two known causes:
	//   1. The kubelet's TTLSecondsAfterFinished window elapsed and
	//      the pod was GC'd before the controller could read its log.
	//   2. An operator force-deleted the pod (kubectl delete --force
	//      --grace-period=0) after Job.Status.Succeeded was bumped
	//      but before this controller scanned the log.
	// In both cases the log is gone permanently; the controller
	// deliberately finalizes Phase=Complete with status.snapshot=nil
	// rather than spinning. status.snapshot being unset is the
	// observable signal that the artefact exists at the spec
	// destination but the agent-emitted coordinates were not
	// captured — restore callers must fall back to spec.destination.
	errPodGone = errors.New("backup-agent pod has been GC'd; log no longer recoverable")
	// errPodNotReady is returned when pods exist for this Job but
	// none has reached PodSucceeded. The Kubernetes Job controller
	// bumps Job.Status.Succeeded by counting pods it has already
	// observed in PodSucceeded, so under normal apiserver semantics
	// `Job.Status.Succeeded>=1 && no PodSucceeded` cannot occur in
	// production. We keep this sentinel as defense-in-depth against
	// (a) controller-runtime cache lag where THIS controller's
	// informer observes the Job status update before the matching
	// pod's phase update, and (b) test harnesses (envtest) that
	// fabricate Job.Succeeded without running a real Job controller.
	// On either we requeue rather than finalize with empty.
	errPodNotReady = errors.New("backup-agent pod has not reached PodSucceeded yet")
	// errNoMarker is returned when the pod log was readable but
	// contained no terminal marker line. Two physical causes that the
	// caller distinguishes via Job.Status.CompletionTime:
	//   - kubelet/CRI runtime log flush still in flight: the agent's
	//     marker line was written to stdout but the runtime hasn't
	//     drained it to the on-disk container log yet. Caller
	//     requeues while CompletionTime is within
	//     markerFlushGracePeriod.
	//   - genuine "no marker": after the grace window, the marker
	//     really isn't coming (torn log, force-truncated stream).
	//     Caller finalizes Phase=Complete with empty snapshot — no
	//     future read of this same log will recover what isn't there.
	errNoMarker = errors.New("agent log contained no terminal snapshot marker")
)

// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackups,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete
// Pod permissions are used solely by extractSnapshotFromJob: r.List
// reads the agent pod for a Succeeded Job (get;list) and the typed
// Clientset streams its terminal log line (pods/log get). We do NOT
// install a controller-runtime Watch on pods — reconciles are driven
// by the Job watch (Owns) plus RequeueAfter when the pod is racing the
// Job's Succeeded counter — so `watch` is intentionally absent.
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list
// +kubebuilder:rbac:groups="",resources=pods/log,verbs=get

// Reconcile handles EtcdBackup resources by creating backup Jobs and tracking their status.
func (r *EtcdBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log.Debug(ctx, "reconciling EtcdBackup")

	backup := &etcdaenixiov1alpha1.EtcdBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		if apierrors.IsNotFound(err) {
			log.Debug(ctx, "EtcdBackup not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if backup.Status.Phase == etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete ||
		backup.Status.Phase == etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed {
		return ctrl.Result{}, nil
	}

	cluster := &etcdaenixiov1alpha1.EtcdCluster{}
	clusterKey := types.NamespacedName{
		Name:      backup.Spec.ClusterRef.Name,
		Namespace: backup.Namespace,
	}
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:               etcdaenixiov1alpha1.EtcdBackupConditionFailed,
				Status:             metav1.ConditionTrue,
				Reason:             "ClusterNotFound",
				Message:            fmt.Sprintf("EtcdCluster %q not found", backup.Spec.ClusterRef.Name),
				ObservedGeneration: backup.Generation,
			})
			return r.updateStatus(ctx, backup)
		}
		return ctrl.Result{}, fmt.Errorf("failed to get EtcdCluster: %w", err)
	}

	existingJobs := &batchv1.JobList{}
	if err := r.List(ctx, existingJobs,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels{"etcd.aenix.io/etcdbackup-name": backup.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list Jobs: %w", err)
	}
	if len(existingJobs.Items) > 0 {
		return r.reconcileJobStatus(ctx, backup, &existingJobs.Items[0])
	}

	if r.OperatorImage == "" {
		backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupConditionFailed,
			Status:             metav1.ConditionTrue,
			Reason:             "ConfigurationError",
			Message:            "OPERATOR_IMAGE environment variable is not set; cannot create backup Job",
			ObservedGeneration: backup.Generation,
		})
		return r.updateStatus(ctx, backup)
	}

	job, err := factory.CreateBackupJob(backup, cluster, r.OperatorImage, r.Scheme)
	if err != nil {
		// Validation errors (factory.ErrInvalidSpec) are terminal:
		// the backup-Job builder rejected a user-supplied field
		// (e.g. pvc.subPath) and no reconcile retry will recover.
		// Surface as Phase=Failed so the user sees and fixes it,
		// instead of silently spinning on the workqueue forever.
		if errors.Is(err, factory.ErrInvalidSpec) {
			backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:               etcdaenixiov1alpha1.EtcdBackupConditionFailed,
				Status:             metav1.ConditionTrue,
				Reason:             "InvalidSpec",
				Message:            err.Error(),
				ObservedGeneration: backup.Generation,
			})
			return r.updateStatus(ctx, backup)
		}
		return ctrl.Result{}, fmt.Errorf("failed to build backup Job: %w", err)
	}

	if err := r.Create(ctx, job); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create backup Job: %w", err)
	}

	log.Info(ctx, "backup Job created", "job", job.Name)
	backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseStarted
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               etcdaenixiov1alpha1.EtcdBackupConditionStarted,
		Status:             metav1.ConditionTrue,
		Reason:             "JobCreated",
		Message:            fmt.Sprintf("Backup Job %q created", job.Name),
		ObservedGeneration: backup.Generation,
	})

	return r.updateStatus(ctx, backup)
}

func (r *EtcdBackupReconciler) reconcileJobStatus(
	ctx context.Context,
	backup *etcdaenixiov1alpha1.EtcdBackup,
	job *batchv1.Job,
) (ctrl.Result, error) {
	if job.Status.Succeeded >= 1 {
		// Extract the actual upload coordinates the backup-agent
		// wrote from its pod log. The agent rewrites the operator-
		// supplied S3_KEY / PVC_BACKUP_PATH at upload time (rev/
		// timestamp suffix injection), so the spec destination alone
		// cannot tell a restore caller which object to fetch.
		//
		// extractSnapshotFromJob distinguishes failure modes via
		// sentinel errors so we can react correctly to each:
		//
		//   - errPodNotReady: a pod exists for this Job but the
		//     controller's cached view has not yet observed it in
		//     PodSucceeded. Defense-in-depth path (see the sentinel's
		//     own doc for why this is rare in real clusters); we
		//     requeue with a short RequeueAfter rather than finalize.
		//     Status is left untouched.
		//
		//   - errPodGone: no pod matches the Job's selector — either
		//     the kubelet's TTL elapsed or someone force-deleted the
		//     pod before we read it. The log is permanently lost;
		//     finalize Phase=Complete with status.snapshot empty.
		//     This is the natural retry bound — it replaces the
		//     wall-clock budget the earlier version used, which
		//     could blow up across controller restarts (a leader
		//     handover or OOM that took longer than the budget
		//     would have caused us to give up while the log was
		//     still readable). The pod's lifetime is the only
		//     load-bearing clock.
		//
		//   - errNoMarker: log was streamed end-to-end but contained
		//     no terminal marker. Two physical causes:
		//       (a) the kubelet/CRI runtime has not yet flushed the
		//           agent's final stdout line to the on-disk container
		//           log — Job.Status.Succeeded can fire a few hundred
		//           milliseconds before containerd's stdio writer
		//           goroutine drains the pipe, so the first read
		//           after observing Succeeded races the writer. We
		//           detect this case via Job.Status.CompletionTime
		//           and requeue while it is fresh (within
		//           markerFlushGracePeriod). The retry budget is
		//           bounded by the pod TTL — once the pod is GC'd
		//           the next reconcile gets errPodGone.
		//       (b) the agent never emitted the marker (the agent's
		//           own code makes this unreachable on a clean exit,
		//           so the realistic path is a torn / truncated log).
		//           After the grace window expires we accept this and
		//           finalize Phase=Complete with empty snapshot.
		//
		//   - any other error (apiserver List failure, log Stream
		//     timeout/RST, etc.): treat as transient infrastructure
		//     and return err so the workqueue retries with
		//     exponential backoff. No status mutation. If the
		//     condition persists past the pod's TTL, the next
		//     reconcile gets errPodGone and finalizes.
		snap, extractErr := r.extractSnapshotFromJob(ctx, job)
		switch {
		case extractErr == nil:
			backup.Status.Snapshot = snap
		case errors.Is(extractErr, errPodNotReady):
			log.Info(ctx, "backup-agent pod not yet PodSucceeded; will retry",
				"job", job.Name)
			return ctrl.Result{RequeueAfter: extractSnapshotRetryInterval}, nil
		case errors.Is(extractErr, errPodGone):
			log.Info(ctx, "backup-agent pod GC'd before log was scanned; finalizing with empty status.snapshot",
				"job", job.Name)
		case errors.Is(extractErr, errNoMarker):
			if job.Status.CompletionTime != nil &&
				time.Since(job.Status.CompletionTime.Time) < markerFlushGracePeriod {
				log.Info(ctx, "agent log has no marker yet; waiting for kubelet log flush",
					"job", job.Name,
					"sinceCompletion", time.Since(job.Status.CompletionTime.Time))
				return ctrl.Result{RequeueAfter: extractSnapshotRetryInterval}, nil
			}
			log.Info(ctx, "backup-agent log contained no terminal marker after flush grace; finalizing with empty status.snapshot",
				"job", job.Name)
		default:
			// Transient apiserver / network / stream failure: do not
			// finalize. Return the error so controller-runtime drops
			// us back on the workqueue with exponential backoff. The
			// next reconcile retries; if the failure persists past
			// the pod's TTL, the subsequent reconcile receives
			// errPodGone and finalizes.
			return ctrl.Result{}, fmt.Errorf("extract snapshot info from agent log: %w", extractErr)
		}
		backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupConditionComplete,
			Status:             metav1.ConditionTrue,
			Reason:             "JobSucceeded",
			Message:            "Backup completed successfully",
			ObservedGeneration: backup.Generation,
		})
		return r.updateStatus(ctx, backup)
	}

	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:               etcdaenixiov1alpha1.EtcdBackupConditionFailed,
				Status:             metav1.ConditionTrue,
				Reason:             "JobFailed",
				Message:            c.Message,
				ObservedGeneration: backup.Generation,
			})
			return r.updateStatus(ctx, backup)
		}
	}

	if !meta.IsStatusConditionTrue(backup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionStarted) {
		backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseStarted
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupConditionStarted,
			Status:             metav1.ConditionTrue,
			Reason:             "JobCreated",
			Message:            fmt.Sprintf("Backup Job %q is running", job.Name),
			ObservedGeneration: backup.Generation,
		})
		return r.updateStatus(ctx, backup)
	}

	return ctrl.Result{}, nil
}

// pickLatestSucceededPod selects the PodSucceeded with the latest
// Status.StartTime from a List result whose ordering is not
// guaranteed by the apiserver. Returns nil when no pod has reached
// PodSucceeded. Ties on StartTime (including the all-unset case) are
// broken by Name descending so the result is deterministic across
// reconciles. When BackoffLimit stays at 0 there is at most one
// candidate, so this collapses to the trivial case; the ordering
// only matters if a future change ever raises BackoffLimit.
func pickLatestSucceededPod(pods []corev1.Pod) *corev1.Pod {
	var best *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.Status.Phase != corev1.PodSucceeded {
			continue
		}
		if best == nil {
			best = p
			continue
		}
		if podStartsAfter(p, best) {
			best = p
		}
	}
	return best
}

// podStartsAfter reports whether a's Status.StartTime is strictly
// after b's, falling back to Name-desc when StartTimes are equal or
// either is unset, so callers get a deterministic tiebreak.
func podStartsAfter(a, b *corev1.Pod) bool {
	switch {
	case a.Status.StartTime != nil && b.Status.StartTime != nil:
		if a.Status.StartTime.After(b.Status.StartTime.Time) {
			return true
		}
		if b.Status.StartTime.After(a.Status.StartTime.Time) {
			return false
		}
	case a.Status.StartTime != nil:
		return true
	case b.Status.StartTime != nil:
		return false
	}
	return a.Name > b.Name
}

func (r *EtcdBackupReconciler) updateStatus(ctx context.Context, backup *etcdaenixiov1alpha1.EtcdBackup) (ctrl.Result, error) {
	err := r.Status().Update(ctx, backup)
	if err == nil {
		return ctrl.Result{}, nil
	}
	if apierrors.IsConflict(err) {
		log.Debug(ctx, "conflict during EtcdBackup status update")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Error(ctx, err, "cannot update EtcdBackup status")
	return ctrl.Result{}, err
}

// extractSnapshotFromJob streams the backup-agent pod log for the
// given Job and returns the URI/size/sha256 the agent reported in its
// terminal "snapshot uploaded: ..." / "snapshot written: ..." marker.
//
// Returns one of the package-level sentinel errors on the known
// failure modes (errPodGone, errPodNotReady, errNoMarker) so the
// caller can branch on root cause; other errors (apiserver List
// failure, stream open failure, mid-read I/O failure) are returned
// wrapped, indicating transient infrastructure the workqueue should
// back off on.
//
// Caveats:
//   - The Job's pod has TTLSecondsAfterFinished=600 (factory/backup_
//     job.go); after 10m the pod is GC'd and this method returns
//     errPodGone. Caller finalizes Phase=Complete with status.snapshot
//     unset. That TTL is the only natural bound on the retry loop —
//     there is no separate wall-clock budget.
//   - Pod List ordering is not guaranteed by the apiserver. Today
//     factory/backup_job.go pins BackoffLimit=0 so there is at most
//     one PodSucceeded, but to keep this resilient if BackoffLimit
//     ever increases we pick the PodSucceeded with the LATEST
//     Status.StartTime — that is the run whose marker line reflects
//     the actual artefact on disk/in-bucket. A list-order tiebreak
//     on Name gives us a stable, deterministic choice when start
//     times match (or are unset).
//   - The log stream uses extractSnapshotTimeout as an upper bound on
//     apiserver responsiveness so a stuck Stream() cannot stall the
//     reconcile worker.
func (r *EtcdBackupReconciler) extractSnapshotFromJob(ctx context.Context, job *batchv1.Job) (*etcdaenixiov1alpha1.BackupSnapshot, error) {
	// Route the pod List through the UN-cached APIReader when
	// available; fall back to the cached Client only for tests that
	// don't supply one. See APIReader's docs on EtcdBackupReconciler
	// for why this matters (cache informers require pods/watch RBAC
	// that the per-Job one-off read does not justify).
	podReader := r.APIReader
	if podReader == nil {
		podReader = r.Client
	}
	pods := &corev1.PodList{}
	if err := podReader.List(ctx, pods,
		client.InNamespace(job.Namespace),
		client.MatchingLabels{"batch.kubernetes.io/job-name": job.Name},
	); err != nil {
		// An apiserver List failure is genuinely transient — return
		// as an unwrapped infrastructure error so the caller's
		// default branch returns to the workqueue with backoff.
		return nil, fmt.Errorf("list pods for Job %s: %w", job.Name, err)
	}
	if len(pods.Items) == 0 {
		// No pod survives for this Job. Either the kubelet's
		// TTLSecondsAfterFinished elapsed or the pod was force-
		// deleted; the log is gone for good. Caller will finalize
		// Phase=Complete with status.snapshot unset.
		return nil, errPodGone
	}
	pod := pickLatestSucceededPod(pods.Items)
	if pod == nil {
		// Pods exist but none has reached PodSucceeded. The
		// Kubernetes Job controller only bumps Job.Status.Succeeded
		// after observing pods in PodSucceeded, so reaching this
		// branch requires controller-runtime cache lag (informer
		// observed the Job update before the matching pod update) or
		// a test harness that fabricates Job.Succeeded without
		// running a Job controller. Caller requeues to re-observe.
		return nil, errPodNotReady
	}
	// Resolve the log streamer lazily — only once we have an actual
	// pod to read. Ordering pod-discovery BEFORE the streamer check
	// keeps the "pod gone" / "pod not ready" sentinels reachable
	// even in misconfigured deployments where Clientset is nil, and
	// keeps the failure modes orthogonal: the streamer is exercised
	// only when there is something to stream.
	streamer := r.LogStreamer
	if streamer == nil {
		if r.Clientset == nil {
			return nil, fmt.Errorf("clientset is not configured; log extraction disabled")
		}
		streamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
			return r.Clientset.CoreV1().Pods(ns).GetLogs(name, &corev1.PodLogOptions{
				Container: container,
			}).Stream(ctx)
		}
	}
	streamCtx, cancel := context.WithTimeout(ctx, extractSnapshotTimeout)
	defer cancel()
	stream, err := streamer(streamCtx, pod.Namespace, pod.Name, "backup-agent")
	if err != nil {
		// Transient stream-open failure (apiserver hiccup, pod
		// network teardown mid-extraction). Caller backs off via
		// workqueue.
		return nil, fmt.Errorf("stream logs for pod %s: %w", pod.Name, err)
	}
	defer func() { _ = stream.Close() }()
	return scanBackupAgentLog(stream)
}

// scanBackupAgentLog reads an agent log stream line-by-line and
// returns the snapshot info parsed from the LATEST terminal marker
// line. Returns an error when no marker is present (no successful
// write happened — the controller leaves status.snapshot unset).
//
// Pulled out for testability: tests pipe a strings.Reader without
// touching the kubernetes Clientset.
//
// Overly long lines (e.g. a stray etcd stack trace dumped on stderr
// that exceeded the buffer) are dropped on the floor rather than
// aborting the scan: bufio.Reader.ReadLine returns isPrefix=true on
// such lines, we drain the rest of that line, and move on. The
// terminal marker is a short, well-formed single line, so this is
// always safe for our parser.
func scanBackupAgentLog(r io.Reader) (*etcdaenixiov1alpha1.BackupSnapshot, error) {
	reader := bufio.NewReaderSize(r, 4096)
	var found *etcdaenixiov1alpha1.BackupSnapshot
	for {
		chunk, isPrefix, err := reader.ReadLine()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			// Preserve a marker we already parsed: a mid-stream
			// error (kubelet log proxy RST, network reset, apiserver
			// drop) AFTER the agent emitted its terminal marker
			// should NOT cause the caller to retry — by the time
			// the retry runs the pod may be GC'd and the URI lost
			// forever. The marker we already have is authoritative.
			if found != nil {
				return found, nil
			}
			return nil, fmt.Errorf("read agent log: %w", err)
		}
		if isPrefix {
			// Drain the rest of this overly long line so the next
			// iteration starts at a fresh line boundary.
			for isPrefix {
				_, isPrefix, err = reader.ReadLine()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					if found != nil {
						return found, nil
					}
					return nil, fmt.Errorf("read agent log: %w", err)
				}
			}
			continue
		}
		m := snapshotMarkerRegexp.FindSubmatch(chunk)
		if m == nil {
			continue
		}
		// m[1] = quoted uri, m[2] = size-digits, m[3] = sha256-hex.
		// Submatches reference chunk's storage, which ReadLine
		// invalidates on the next call; string() copies into a fresh
		// allocation. The URI is double-quoted Go-syntax (agent emits
		// it via %q) so a key/path containing spaces or other
		// shell-active characters round-trips through the marker.
		uri, unquoteErr := strconv.Unquote(string(m[1]))
		if unquoteErr != nil {
			// A regex-conformant capture that fails to unquote means
			// the agent emitted something exotic enough to confuse
			// strconv (extremely unlikely — %q is round-trip safe).
			// Skip rather than half-populate.
			continue
		}
		if !snapshotURIPrefixRegexp.MatchString(uri) {
			// Defense in depth: a malformed agent build that emitted
			// e.g. uri="https://..." would have a syntactically valid
			// marker but a URI the CRD pattern (^(s3|file)://.+) will
			// reject on Status().Update. Treating it as "no marker"
			// here keeps the controller in its retry loop until a
			// well-formed marker shows up (or the budget expires),
			// instead of burning retries on apiserver validation
			// errors.
			continue
		}
		parsedSize, parseErr := strconv.ParseInt(string(m[2]), 10, 64)
		if parseErr != nil {
			// strconv only fails on overflow here (regex pins
			// [0-9]+); fall through with size=0 so the URI + hash
			// still land and a reviewer reading status sees the
			// snapshot exists.
			parsedSize = 0
		}
		found = &etcdaenixiov1alpha1.BackupSnapshot{
			URI:       uri,
			SizeBytes: parsedSize,
			Checksum:  "sha256:" + string(m[3]),
		}
	}
	if found == nil {
		return nil, errNoMarker
	}
	return found, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdaenixiov1alpha1.EtcdBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
