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
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

// errorOnListClient wraps a controller-runtime client.Client and
// injects a hard failure on any List call. Used to pin the
// "transient apiserver List error → workqueue retry" branch — the
// fake client does not natively expose a way to make List fail.
type errorOnListClient struct {
	client.Client
	err error
}

func (e *errorOnListClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return e.err
}

// stubReadCloser wraps a strings.Reader as io.ReadCloser.
type stubReadCloser struct {
	io.Reader
}

func (stubReadCloser) Close() error { return nil }

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := scheme.AddToScheme(s); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	if err := etcdaenixiov1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add api scheme: %v", err)
	}
	return s
}

func buildPod(ns, name, jobName string, phase corev1.PodPhase) corev1.Pod {
	return corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      name,
			Labels:    map[string]string{"batch.kubernetes.io/job-name": jobName},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func buildJob(name string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
	}
}

const testS3SnapshotURI = "s3://b/k.db"

// TestExtractSnapshotFromJob_HappyPath pins the wire-up contract: a
// Succeeded pod's log stream containing the terminal marker yields a
// fully-populated BackupSnapshot. This is the load-bearing path the
// PR exists to deliver — unit-tested scanners and writers don't prove
// the controller actually calls them.
func TestExtractSnapshotFromJob_HappyPath(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-1")
	pod := buildPod(ns, "etcd-backup-1-abcde", job.Name, corev1.PodSucceeded)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&pod).
		Build()

	const wantSHA = "deadbeefcafef00d0000000000000000000000000000000000000000000000ff"
	const logBody = "taking etcd snapshot...\n" +
		"writing snapshot to /backup/data/snap.db\n" +
		`snapshot written: uri="file:///backup/data/snap.db" size=2048 sha256=` + wantSHA + "\n"

	r := &EtcdBackupReconciler{
		Client: c,
		LogStreamer: func(ctx context.Context, ns2, name, container string) (io.ReadCloser, error) {
			if container != "backup-agent" {
				t.Errorf("container = %q, want backup-agent", container)
			}
			if name != pod.Name {
				t.Errorf("pod name = %q, want %q", name, pod.Name)
			}
			return stubReadCloser{Reader: strings.NewReader(logBody)}, nil
		},
	}

	got, err := r.extractSnapshotFromJob(context.Background(), job)
	if err != nil {
		t.Fatalf("extractSnapshotFromJob: %v", err)
	}
	if got == nil {
		t.Fatal("got nil snapshot, want populated")
	}
	if got.URI != "file:///backup/data/snap.db" {
		t.Errorf("URI = %q, want file:///backup/data/snap.db", got.URI)
	}
	if got.SizeBytes != 2048 {
		t.Errorf("SizeBytes = %d, want 2048", got.SizeBytes)
	}
	if got.Checksum != "sha256:"+wantSHA {
		t.Errorf("Checksum = %q, want sha256:%s", got.Checksum, wantSHA)
	}
}

// TestPickLatestSucceededPod pins blocker D's contract: among
// multiple PodSucceeded entries (which BackoffLimit=0 makes
// impossible today but a future change could permit), the one with
// the most recent Status.StartTime wins. Without this the apiserver
// list order — which is unspecified — would silently determine which
// run's marker the controller reads, making status.snapshot
// non-deterministic.
func TestPickLatestSucceededPod(t *testing.T) {
	now := time.Now()
	older := metav1.NewTime(now.Add(-2 * time.Minute))
	newer := metav1.NewTime(now.Add(-30 * time.Second))

	mk := func(name string, phase corev1.PodPhase, start *metav1.Time) corev1.Pod {
		p := buildPod("ns", name, "job-x", phase)
		if start != nil {
			p.Status.StartTime = start
		}
		return p
	}

	tests := []struct {
		name     string
		pods     []corev1.Pod
		wantName string
	}{
		{
			name: "two succeeded — newer StartTime wins",
			pods: []corev1.Pod{
				mk("aaa-old", corev1.PodSucceeded, &older),
				mk("zzz-new", corev1.PodSucceeded, &newer),
			},
			wantName: "zzz-new",
		},
		{
			// Reverse the slice order to prove list-order alone does
			// not decide.
			name: "newer wins regardless of slice order",
			pods: []corev1.Pod{
				mk("zzz-new", corev1.PodSucceeded, &newer),
				mk("aaa-old", corev1.PodSucceeded, &older),
			},
			wantName: "zzz-new",
		},
		{
			name: "failed pods are ignored",
			pods: []corev1.Pod{
				mk("failed", corev1.PodFailed, &newer),
				mk("succeeded", corev1.PodSucceeded, &older),
			},
			wantName: "succeeded",
		},
		{
			// Both unset → Name-desc tiebreak (deterministic).
			name: "equal/unset StartTimes break by Name desc",
			pods: []corev1.Pod{
				mk("aaa", corev1.PodSucceeded, nil),
				mk("bbb", corev1.PodSucceeded, nil),
			},
			wantName: "bbb",
		},
		{
			// Distinct from the "both unset" row above: BOTH pods
			// carry an explicit, identical StartTime. The
			// podStartsAfter helper's After/After short-circuits do
			// not trigger (neither is strictly after the other), so
			// the function must fall through to Name-desc. Slice is
			// in name-ASCENDING order; the highest-named pod still
			// wins — proves the tiebreak is independent of slice
			// order even when StartTimes are set.
			name: "equal/SET StartTimes break by Name desc (slice in name-asc order)",
			pods: []corev1.Pod{
				mk("aaa", corev1.PodSucceeded, &older),
				mk("bbb", corev1.PodSucceeded, &older),
			},
			wantName: "bbb",
		},
		{
			name:     "no succeeded pods returns nil",
			pods:     []corev1.Pod{mk("p", corev1.PodFailed, &newer)},
			wantName: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pickLatestSucceededPod(tc.pods)
			if tc.wantName == "" {
				if got != nil {
					t.Fatalf("got %q, want nil", got.Name)
				}
				return
			}
			if got == nil {
				t.Fatalf("got nil, want %q", tc.wantName)
			}
			if got.Name != tc.wantName {
				t.Errorf("got %q, want %q", got.Name, tc.wantName)
			}
		})
	}
}

// TestExtractSnapshotFromJob_PrefersSucceededPod guards against a
// regression of the "BackoffLimit ever changes from 0" footgun: pod
// List ordering is not guaranteed, so a Failed pod returned before
// the Succeeded one must not be selected. The Succeeded pod's marker
// must win regardless of slice order.
func TestExtractSnapshotFromJob_PrefersSucceededPod(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-2")
	failedPod := buildPod(ns, "aaa-failed", job.Name, corev1.PodFailed)
	succeededPod := buildPod(ns, "zzz-succeeded", job.Name, corev1.PodSucceeded)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&failedPod, &succeededPod).
		Build()

	r := &EtcdBackupReconciler{
		Client: c,
		LogStreamer: func(ctx context.Context, ns2, name, container string) (io.ReadCloser, error) {
			if name == failedPod.Name {
				t.Fatalf("streamer called with failed pod %q; should pick succeeded pod", name)
			}
			if name != succeededPod.Name {
				t.Fatalf("streamer called with %q, want %q", name, succeededPod.Name)
			}
			return stubReadCloser{Reader: strings.NewReader(
				`snapshot uploaded: uri="` + testS3SnapshotURI + `" size=10 sha256=` + strings.Repeat("a", 64) + "\n",
			)}, nil
		},
	}

	got, err := r.extractSnapshotFromJob(context.Background(), job)
	if err != nil {
		t.Fatalf("extractSnapshotFromJob: %v", err)
	}
	if got == nil || got.URI != testS3SnapshotURI {
		t.Fatalf("got %+v, want URI=%s", got, testS3SnapshotURI)
	}
}

// TestExtractSnapshotFromJob_NoSucceededPod covers the case where
// pods exist but none has reached PodSucceeded — the kubelet hasn't
// caught up to the Job-controller's bumped Succeeded counter. The
// caller must see errPodNotReady so it can requeue, NOT a generic
// error that the reconcile-loop's default branch would treat as
// permanent.
func TestExtractSnapshotFromJob_NoSucceededPod(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-3")
	failedPod := buildPod(ns, "p-failed", job.Name, corev1.PodFailed)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&failedPod).
		Build()

	r := &EtcdBackupReconciler{
		Client: c,
		LogStreamer: func(ctx context.Context, ns2, name, container string) (io.ReadCloser, error) {
			t.Fatalf("streamer must not be called when no pod is Succeeded; called with %q", name)
			return nil, nil
		},
	}

	_, err := r.extractSnapshotFromJob(context.Background(), job)
	if !errors.Is(err, errPodNotReady) {
		t.Fatalf("got %v, want errPodNotReady so caller requeues without finalizing", err)
	}
}

// TestExtractSnapshotFromJob_NoPods covers the pod-GC'd-after-TTL
// case: no pods returned by List. Must return errPodGone so the
// caller finalizes Phase=Complete with status.snapshot empty (the log
// is gone for good — no further retry will recover it).
func TestExtractSnapshotFromJob_NoPods(t *testing.T) {
	job := buildJob("etcd-backup-4")

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		Build()

	r := &EtcdBackupReconciler{Client: c}
	_, err := r.extractSnapshotFromJob(context.Background(), job)
	if !errors.Is(err, errPodGone) {
		t.Fatalf("got %v, want errPodGone so caller finalizes with empty snapshot", err)
	}
}

// TestExtractSnapshotFromJob_NoMarkerInLog: a pod-log without the
// terminal marker (e.g. log GC truncated the stream after the agent
// started but before the upload finished) must surface errNoMarker
// — the caller finalizes with an empty snapshot rather than retrying
// indefinitely, because no future read of this same log will recover
// what isn't there.
func TestExtractSnapshotFromJob_NoMarkerInLog(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-5")
	pod := buildPod(ns, "p", job.Name, corev1.PodSucceeded)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&pod).
		Build()

	r := &EtcdBackupReconciler{
		Client: c,
		LogStreamer: func(ctx context.Context, ns2, name, container string) (io.ReadCloser, error) {
			return stubReadCloser{Reader: strings.NewReader(
				"taking etcd snapshot...\nuploading snapshot to s3://b/k (10 bytes)\n",
			)}, nil
		},
	}

	_, err := r.extractSnapshotFromJob(context.Background(), job)
	if !errors.Is(err, errNoMarker) {
		t.Fatalf("got %v, want errNoMarker so caller finalizes with empty snapshot", err)
	}
}

// TestExtractSnapshotFromJob_StreamerErrorIsTransient: when the
// streamer returns a generic error (apiserver hiccup, network reset
// mid-stream), the function must NOT collapse to one of the
// "finalize" sentinels — the reconciler classifies these as transient
// and returns them to the workqueue for exponential-backoff retry.
// Pinning this in a test prevents a future refactor from accidentally
// turning a transient apiserver stall into a permanent
// "Phase=Complete, snapshot=nil" outcome.
func TestExtractSnapshotFromJob_StreamerErrorIsTransient(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-stream-err")
	pod := buildPod(ns, "p", job.Name, corev1.PodSucceeded)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&pod).
		Build()

	streamErr := errors.New("simulated apiserver stall")
	r := &EtcdBackupReconciler{
		Client: c,
		LogStreamer: func(ctx context.Context, ns2, name, container string) (io.ReadCloser, error) {
			return nil, streamErr
		},
	}

	_, err := r.extractSnapshotFromJob(context.Background(), job)
	if err == nil {
		t.Fatal("expected stream error, got nil")
	}
	// The streamer's error must propagate (wrapped so the caller
	// keeps its diagnostic context) without being misclassified as
	// any of the "finalize" sentinels.
	if !errors.Is(err, streamErr) {
		t.Errorf("got %v, want wrapped streamErr", err)
	}
	if errors.Is(err, errPodGone) || errors.Is(err, errPodNotReady) || errors.Is(err, errNoMarker) {
		t.Errorf("got %v, want a transient error (none of the finalize sentinels)", err)
	}
}

// TestExtractSnapshotFromJob_ListErrorIsTransient: when the apiserver
// fails the List request (network, RBAC, throttling), the function
// must NOT collapse to errPodGone — that would falsely finalize with
// status.snapshot=nil on a transient failure. The caller's default
// branch must return the error to the workqueue.
func TestExtractSnapshotFromJob_ListErrorIsTransient(t *testing.T) {
	job := buildJob("etcd-backup-list-err")

	listErr := errors.New("simulated apiserver List failure")
	c := &errorOnListClient{
		Client: fake.NewClientBuilder().
			WithScheme(newScheme(t)).
			Build(),
		err: listErr,
	}

	r := &EtcdBackupReconciler{Client: c}
	_, err := r.extractSnapshotFromJob(context.Background(), job)
	if err == nil {
		t.Fatal("expected list error, got nil")
	}
	if !errors.Is(err, listErr) {
		t.Errorf("got %v, want wrapped listErr", err)
	}
	if errors.Is(err, errPodGone) {
		t.Error("transient apiserver List failure must NOT be classified as errPodGone")
	}
}

// TestExtractSnapshotFromJob_PreExpiredContext pins the sub-timeout
// behavior: an already-expired ctx propagates straight back to the
// streamer (which observes the deadline) and the function returns
// promptly instead of blocking the reconcile worker.
func TestExtractSnapshotFromJob_PreExpiredContext(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-6")
	pod := buildPod(ns, "p", job.Name, corev1.PodSucceeded)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&pod).
		Build()

	streamerCalled := false
	r := &EtcdBackupReconciler{
		Client: c,
		LogStreamer: func(ctx context.Context, ns2, name, container string) (io.ReadCloser, error) {
			streamerCalled = true
			// The reconcile worker's ctx is dead; the wrapping
			// extractSnapshotTimeout is irrelevant because the parent
			// ctx already exceeded its deadline.
			if err := ctx.Err(); err == nil {
				t.Fatal("expected ctx already expired inside streamer")
			}
			return nil, ctx.Err()
		},
	}

	parent, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	done := make(chan struct{})
	go func() {
		_, _ = r.extractSnapshotFromJob(parent, job)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("extractSnapshotFromJob did not return within 5s on pre-expired ctx")
	}
	if !streamerCalled {
		t.Error("streamer was never called")
	}
}

// TestExtractSnapshotFromJob_RoutesPodListThroughAPIReader pins
// the cache-bypass routing: the per-Job pod List must go through
// EtcdBackupReconciler.APIReader (mgr.GetAPIReader() in
// production), not through the manager's cached Client. The cached
// Client would force controller-runtime to start a cluster-wide
// Pod informer requiring namespace-wide pods/watch RBAC — a
// permission the operator does NOT grant (and should not need to
// grant) for a per-EtcdBackup one-off read.
//
// The test fails closed if a future refactor accidentally routes
// the read back through r.Client: Client.List is wired to always
// error, so success here can only mean the APIReader path was
// taken.
func TestExtractSnapshotFromJob_RoutesPodListThroughAPIReader(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-apireader")
	pod := buildPod(ns, "etcd-backup-apireader-xyz", job.Name, corev1.PodSucceeded)

	apiReader := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&pod).
		Build()
	// Client is wired to ALWAYS fail List. If extraction reaches it
	// the test fails — proving the APIReader branch is the actual
	// read path.
	cachedClient := &errorOnListClient{
		Client: fake.NewClientBuilder().
			WithScheme(newScheme(t)).
			Build(),
		err: errors.New("cached Client must not be used for pod reads"),
	}

	r := &EtcdBackupReconciler{
		Client:    cachedClient,
		APIReader: apiReader,
		LogStreamer: func(ctx context.Context, ns2, name, container string) (io.ReadCloser, error) {
			return stubReadCloser{Reader: strings.NewReader(
				`snapshot uploaded: uri="` + testS3SnapshotURI + `" size=10 sha256=` +
					strings.Repeat("a", 64) + "\n",
			)}, nil
		},
	}

	got, err := r.extractSnapshotFromJob(context.Background(), job)
	if err != nil {
		t.Fatalf("extractSnapshotFromJob (APIReader path): %v", err)
	}
	if got == nil || got.URI != testS3SnapshotURI {
		t.Fatalf("got %+v, want URI=%s (proves APIReader-routed pod was found)", got, testS3SnapshotURI)
	}
}

// TestExtractSnapshotFromJob_NoStreamerNoClientset covers the
// production-misconfiguration path: when neither LogStreamer nor
// Clientset is wired (which would happen if cmd/manager/main.go
// regressed and stopped building the typed Clientset), we return an
// error rather than panicking.
func TestExtractSnapshotFromJob_NoStreamerNoClientset(t *testing.T) {
	const ns = "default"
	job := buildJob("etcd-backup-7")
	pod := buildPod(ns, "p", job.Name, corev1.PodSucceeded)

	c := fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithObjects(&pod).
		Build()

	r := &EtcdBackupReconciler{Client: c}
	if _, err := r.extractSnapshotFromJob(context.Background(), job); err == nil {
		t.Fatal("expected error when neither LogStreamer nor Clientset is configured, got nil")
	}
}
