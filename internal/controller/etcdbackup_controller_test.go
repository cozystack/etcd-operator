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
	"fmt"
	"io"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

const testNamespace = "default"

var _ = Describe("EtcdBackup Controller", func() {
	const (
		clusterName   = "test-etcd-cluster"
		backupName    = "test-etcd-backup"
		operatorImage = "ghcr.io/aenix-io/etcd-operator:test"
	)

	var (
		reconciler *EtcdBackupReconciler
	)

	BeforeEach(func() {
		reconciler = &EtcdBackupReconciler{
			Client:        getK8sClient(),
			Scheme:        getK8sClient().Scheme(),
			OperatorImage: operatorImage,
		}
	})

	AfterEach(func() {
		// Cleanup resources
		backupList := &etcdaenixiov1alpha1.EtcdBackupList{}
		Expect(getK8sClient().List(ctx, backupList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range backupList.Items {
			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, &backupList.Items[i]))).To(Succeed())
		}
		clusterList := &etcdaenixiov1alpha1.EtcdClusterList{}
		Expect(getK8sClient().List(ctx, clusterList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range clusterList.Items {
			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, &clusterList.Items[i]))).To(Succeed())
		}
		jobList := &batchv1.JobList{}
		Expect(getK8sClient().List(ctx, jobList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range jobList.Items {
			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, &jobList.Items[i], client.PropagationPolicy(metav1.DeletePropagationBackground)))).To(Succeed())
		}
	})

	Context("When reconciling a PVC backup", func() {
		It("Should create a backup Job and set Started condition", func() {
			cluster := createTestCluster(ctx, clusterName, nil)
			backup := createTestPVCBackup(ctx, backupName, clusterName)

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			// Verify Job was created
			job := getBackupJob(ctx, backup.Name)
			Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"/backup-agent"}))
			Expect(*job.Spec.BackoffLimit).To(Equal(int32(0)))

			// Verify owner reference
			Expect(job.OwnerReferences).To(HaveLen(1))
			Expect(job.OwnerReferences[0].Name).To(Equal(backup.Name))

			// Verify Started phase and condition
			updatedBackup := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
			Expect(updatedBackup.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseStarted))
			Expect(meta.IsStatusConditionTrue(updatedBackup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionStarted)).To(BeTrue())

			_ = cluster // used to keep cluster in scope
		})
	})

	Context("When reconciling an S3 backup", func() {
		It("Should create a backup Job with S3 env vars", func() {
			cluster := createTestCluster(ctx, clusterName+"-s3", nil)
			backup := createTestS3Backup(ctx, backupName+"-s3", cluster.Name)

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			job := getBackupJob(ctx, backup.Name)

			container := job.Spec.Template.Spec.Containers[0]
			envNames := make([]string, len(container.Env))
			for i, e := range container.Env {
				envNames[i] = e.Name
			}
			Expect(envNames).To(ContainElement("S3_BUCKET"))
			Expect(envNames).To(ContainElement("S3_KEY"))
			Expect(envNames).To(ContainElement("AWS_ACCESS_KEY_ID"))
			Expect(envNames).To(ContainElement("AWS_SECRET_ACCESS_KEY"))
			Expect(envNames).To(ContainElement("BACKUP_DESTINATION"))
		})
	})

	Context("When EtcdCluster is not found", func() {
		It("Should set Failed condition", func() {
			backup := createTestPVCBackup(ctx, backupName+"-nocluster", "nonexistent-cluster")

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			updatedBackup := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
			Expect(updatedBackup.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed))
			failedCond := meta.FindStatusCondition(updatedBackup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed)
			Expect(failedCond).NotTo(BeNil())
			Expect(failedCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(failedCond.Reason).To(Equal("ClusterNotFound"))
		})
	})

	// A user-input validation error from CreateBackupJob (e.g. a
	// PVC SubPath containing "..") MUST land on
	// EtcdBackup.status as Phase=Failed/Reason=InvalidSpec. Before
	// the controller branched on factory.ErrInvalidSpec, the same
	// error was returned wrapped to the workqueue and silently
	// retried forever — the user saw a stuck Phase=Started with no
	// hint that their SubPath was at fault. The test stages a
	// realistic traversal subPath (the factory rejects it via
	// validatePVCSubPath wrapping with ErrInvalidSpec) and asserts
	// the controller surfaces it as a TERMINAL Failed condition AND
	// does not create any Job (validation must run before Create
	// so we don't leave a half-built artefact behind).
	Context("When backup spec is invalid (PVC SubPath traversal)", func() {
		It("Should set Failed/InvalidSpec without creating a Job", func() {
			cluster := createTestCluster(ctx, clusterName+"-invalid", nil)
			backup := &etcdaenixiov1alpha1.EtcdBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      backupName + "-invalid",
					Namespace: testNamespace,
				},
				Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: cluster.Name},
					Destination: etcdaenixiov1alpha1.BackupDestination{
						PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
							ClaimName: "test-backup-pvc",
							SubPath:   "../../escape",
						},
					},
				},
			}
			Expect(getK8sClient().Create(ctx, backup)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred(),
				"validation failure must NOT be returned to the workqueue — that would spin on a user-input error forever")
			Expect(result.Requeue).To(BeFalse())

			updated := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updated)).To(Succeed())
			Expect(updated.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed))
			failedCond := meta.FindStatusCondition(updated.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed)
			Expect(failedCond).NotTo(BeNil())
			Expect(failedCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(failedCond.Reason).To(Equal("InvalidSpec"))
			Expect(failedCond.Message).To(ContainSubstring("subPath"),
				"user-facing message must name the offending field so the user can locate it without consulting controller logs")

			// No Job must have been created — validation runs before
			// the Create call. A future refactor that flips that order
			// (validate-after-create) would leave a half-built artefact
			// behind on every invalid spec.
			jobList := &batchv1.JobList{}
			Expect(getK8sClient().List(ctx, jobList,
				client.InNamespace(testNamespace),
				client.MatchingLabels{"etcd.aenix.io/etcdbackup-name": backup.Name},
			)).To(Succeed())
			Expect(jobList.Items).To(BeEmpty())
		})
	})

	Context("When Job succeeds", func() {
		It("Should set Complete condition and populate status.snapshot from the pod log", func() {
			cluster := createTestCluster(ctx, clusterName+"-success", nil)
			backup := createTestPVCBackup(ctx, backupName+"-success", cluster.Name)

			// Wire a LogStreamer stub so the post-success extraction
			// path exercises the real Reconcile → Job-Succeeded →
			// stream → parse → Status().Update flow. Without this
			// the previous version of the test used the nil-Clientset
			// path, silently swallowing the extraction error and
			// turning the "snapshot wired through to status" contract
			// into dead code.
			const wantURI = "file:///backup/data/test-etcd-backup-success.db"
			const wantSize int64 = 4096
			const wantSHA = "deadbeefcafef00d0000000000000000000000000000000000000000000000ff"
			reconciler.LogStreamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
				Expect(container).To(Equal("backup-agent"))
				body := `snapshot written: uri="` + wantURI + `" size=4096 sha256=` + wantSHA + "\n"
				return io.NopCloser(strings.NewReader(body)), nil
			}

			// First reconcile: creates the Job
			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Simulate Job success and create the matching agent pod
			// (envtest doesn't run a Job controller, so we materialize
			// the pod the controller will look up via the
			// batch.kubernetes.io/job-name label).
			job := getBackupJob(ctx, backup.Name)
			now := metav1.Now()
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &now
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			agentPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: backup.Namespace,
					Name:      job.Name + "-agent",
					Labels:    map[string]string{"batch.kubernetes.io/job-name": job.Name},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "backup-agent",
						Image: "ghcr.io/aenix-io/etcd-operator:test",
					}},
				},
			}
			Expect(getK8sClient().Create(ctx, agentPod)).To(Succeed())
			agentPod.Status.Phase = corev1.PodSucceeded
			// Pin Status.StartTime so pickLatestSucceededPod's
			// selection is StartTime-driven rather than dependent on
			// the Name-desc tiebreak. A future refactor that requires
			// a non-nil StartTime to be considered would silently
			// turn this test green-but-vacuous without the pin.
			startTime := metav1.Now()
			agentPod.Status.StartTime = &startTime
			Expect(getK8sClient().Status().Update(ctx, agentPod)).To(Succeed())

			// Second reconcile: should set Complete AND status.snapshot
			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			updatedBackup := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
			Expect(updatedBackup.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete))
			Expect(meta.IsStatusConditionTrue(updatedBackup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionComplete)).To(BeTrue())

			// status.snapshot must round-trip through the apiserver
			// (the CRD's URI / checksum patterns apply on Update — if
			// either drifted from the agent's emitted format the
			// Status().Update would be rejected and updatedBackup
			// would not carry these values).
			Expect(updatedBackup.Status.Snapshot).NotTo(BeNil())
			Expect(updatedBackup.Status.Snapshot.URI).To(Equal(wantURI))
			Expect(updatedBackup.Status.Snapshot.SizeBytes).To(Equal(wantSize))
			Expect(updatedBackup.Status.Snapshot.Checksum).To(Equal("sha256:" + wantSHA))

			// Tidy up the agent pod for the AfterEach cleanup.
			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, agentPod))).To(Succeed())
		})

		// Pins the errPodNotReady defense-in-depth path: on a
		// reconcile pass where Job.Status.Succeeded>=1 but no
		// matching pod is in PodSucceeded, the controller MUST
		// requeue rather than finalize Phase=Complete with an empty
		// status.snapshot. The Kubernetes Job controller only bumps
		// Succeeded after observing PodSucceeded so this shape does
		// not occur in production clusters via the apiserver — the
		// path exists as a safety net for controller-runtime cache
		// lag and is exercised here by envtest (no Job controller
		// runs, so we fabricate Job.Succeeded independently of pod
		// phase). Once the pod transitions to PodSucceeded a
		// subsequent reconcile populates status.snapshot and flips
		// Phase.
		It("Should requeue and not finalize when no pod is yet in PodSucceeded (envtest-only state)", func() {
			cluster := createTestCluster(ctx, clusterName+"-race", nil)
			backup := createTestPVCBackup(ctx, backupName+"-race", cluster.Name)

			const wantURI = "file:///backup/data/test-etcd-backup-race.db"
			const wantSize int64 = 1024
			const wantSHA = "cafef00ddeadbeef00000000000000000000000000000000000000000000ff00"
			reconciler.LogStreamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
				body := fmt.Sprintf("snapshot written: uri=%q size=%d sha256=%s\n", wantURI, wantSize, wantSHA)
				return io.NopCloser(strings.NewReader(body)), nil
			}

			// First reconcile: creates the Job
			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Mark Job succeeded with a fresh CompletionTime; create
			// the matching pod but leave it in Running (the kubelet
			// hasn't caught up yet).
			job := getBackupJob(ctx, backup.Name)
			now := metav1.Now()
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &now
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			agentPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: backup.Namespace,
					Name:      job.Name + "-agent",
					Labels:    map[string]string{"batch.kubernetes.io/job-name": job.Name},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "backup-agent",
						Image: "ghcr.io/aenix-io/etcd-operator:test",
					}},
				},
			}
			Expect(getK8sClient().Create(ctx, agentPod)).To(Succeed())
			agentPod.Status.Phase = corev1.PodRunning
			// Pin Status.StartTime so pickLatestSucceededPod's later
			// selection (after the pod flips to PodSucceeded below)
			// is StartTime-driven rather than the Name-desc tiebreak.
			// A refactor that requires a non-nil StartTime to be
			// considered would otherwise silently route the second
			// reconcile through errPodNotReady forever, making this
			// test green-but-vacuous.
			startTime := metav1.Now()
			agentPod.Status.StartTime = &startTime
			Expect(getK8sClient().Status().Update(ctx, agentPod)).To(Succeed())

			// Second reconcile: pod still Running, extraction must
			// fail, controller must NOT finalize.
			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			racy := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, racy)).To(Succeed())
			Expect(racy.Status.Phase).NotTo(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete))
			Expect(racy.Status.Snapshot).To(BeNil())

			// Kubelet catches up: transition pod to PodSucceeded and
			// reconcile again. status.snapshot must now be populated
			// and Phase must flip to Complete.
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: agentPod.Name, Namespace: agentPod.Namespace}, agentPod)).To(Succeed())
			agentPod.Status.Phase = corev1.PodSucceeded
			Expect(getK8sClient().Status().Update(ctx, agentPod)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			final := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, final)).To(Succeed())
			Expect(final.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete))
			Expect(final.Status.Snapshot).NotTo(BeNil())
			Expect(final.Status.Snapshot.URI).To(Equal(wantURI))
			Expect(final.Status.Snapshot.SizeBytes).To(Equal(wantSize))
			Expect(final.Status.Snapshot.Checksum).To(Equal("sha256:" + wantSHA))

			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, agentPod))).To(Succeed())
		})

		// When the agent pod has been GC'd by the kubelet's
		// TTLSecondsAfterFinished window, no future retry will
		// recover the log. The controller must accept the loss and
		// finalize Phase=Complete with status.snapshot=nil rather
		// than spinning. The natural retry bound is pod lifetime —
		// there is no separate wall-clock budget.
		//
		// This test exercises the errPodGone branch: a Succeeded
		// Job with NO matching pod (TTL elapsed). LogStreamer must
		// not be called (extraction fails at the pod-list step) —
		// if it were called the test would fail loudly.
		It("Should finalize with empty snapshot when the agent pod has been GC'd", func() {
			cluster := createTestCluster(ctx, clusterName+"-gone", nil)
			backup := createTestPVCBackup(ctx, backupName+"-gone", cluster.Name)

			reconciler.LogStreamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
				defer GinkgoRecover()
				Fail("LogStreamer must not be called when no pod is present (extraction fails at the pod-list step)")
				return nil, nil
			}

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Mark Job succeeded with no matching pod (simulating
			// post-TTL GC). The completion-time value is irrelevant
			// under the pod-lifecycle-gated retry — finalization
			// happens on `len(pods) == 0` alone.
			job := getBackupJob(ctx, backup.Name)
			now := metav1.Now()
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &now
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			final := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, final)).To(Succeed())
			Expect(final.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete))
			Expect(meta.IsStatusConditionTrue(final.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionComplete)).To(BeTrue())
			Expect(final.Status.Snapshot).To(BeNil())
		})

		// Pins markerFlushGracePeriod's WITHIN-grace branch.
		// On kind / containerd the agent's terminal marker line
		// can race the kubelet's log-flush goroutine: pod
		// reports Succeeded before its last stdout line lands in
		// the on-disk container log. extractSnapshotFromJob then
		// streams a marker-less log and returns errNoMarker; if
		// the controller finalized immediately, status.snapshot
		// would be permanently empty for an otherwise-clean
		// backup. The controller must requeue while
		// Job.Status.CompletionTime is fresh (< markerFlush
		// GracePeriod). A future refactor that swaps the
		// comparison direction would silently flip
		// "self-recovering race" → "always finalize empty".
		It("Should requeue (not finalize) on errNoMarker within the flush grace window", func() {
			cluster := createTestCluster(ctx, clusterName+"-flush-grace", nil)
			backup := createTestPVCBackup(ctx, backupName+"-flush-grace", cluster.Name)
			_ = cluster

			// LogStreamer returns content that lacks the terminal
			// "snapshot written: …" marker → scanner returns
			// errNoMarker. The controller's grace window then
			// gates whether we requeue or finalize empty.
			reconciler.LogStreamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(
					"taking etcd snapshot...\nwriting snapshot to /backup/data/x.db\n",
				)), nil
			}

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			job := getBackupJob(ctx, backup.Name)
			now := metav1.Now()
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &now // FRESH — within grace
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			agentPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: backup.Namespace,
					Name:      job.Name + "-agent",
					Labels:    map[string]string{"batch.kubernetes.io/job-name": job.Name},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "backup-agent",
						Image: "ghcr.io/aenix-io/etcd-operator:test",
					}},
				},
			}
			Expect(getK8sClient().Create(ctx, agentPod)).To(Succeed())
			agentPod.Status.Phase = corev1.PodSucceeded
			startTime := metav1.Now()
			agentPod.Status.StartTime = &startTime
			Expect(getK8sClient().Status().Update(ctx, agentPod)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0),
				"within-grace errNoMarker must requeue so the next read can pick up the flushed marker")

			racy := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, racy)).To(Succeed())
			Expect(racy.Status.Phase).NotTo(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete))
			Expect(racy.Status.Snapshot).To(BeNil())

			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, agentPod))).To(Succeed())
		})

		// Pins the nil-CompletionTime safety branch in
		// reconcileJobStatus's errNoMarker handler. The Job
		// controller writes CompletionTime alongside
		// Succeeded>=1 in practice, but if a reconcile happens
		// to observe Succeeded set with CompletionTime still
		// nil (a Job-controller write that races), the
		// controller must requeue rather than fall through to
		// finalize-with-empty-snapshot. The finalize branch is
		// terminal, so "treat unknown timestamp as still
		// within grace" is the only safe call.
		It("Should requeue (not finalize) on errNoMarker when Job.CompletionTime is nil", func() {
			cluster := createTestCluster(ctx, clusterName+"-flush-nil", nil)
			backup := createTestPVCBackup(ctx, backupName+"-flush-nil", cluster.Name)
			_ = cluster

			reconciler.LogStreamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(
					"taking etcd snapshot...\nwriting snapshot to /backup/data/x.db\n",
				)), nil
			}

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			job := getBackupJob(ctx, backup.Name)
			job.Status.Succeeded = 1
			// CompletionTime intentionally NOT set — the race we
			// are guarding against.
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			agentPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: backup.Namespace,
					Name:      job.Name + "-agent",
					Labels:    map[string]string{"batch.kubernetes.io/job-name": job.Name},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "backup-agent",
						Image: "ghcr.io/aenix-io/etcd-operator:test",
					}},
				},
			}
			Expect(getK8sClient().Create(ctx, agentPod)).To(Succeed())
			agentPod.Status.Phase = corev1.PodSucceeded
			startTime := metav1.Now()
			agentPod.Status.StartTime = &startTime
			Expect(getK8sClient().Status().Update(ctx, agentPod)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0),
				"errNoMarker with nil CompletionTime must requeue — finalize-empty is unrecoverable")

			racy := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, racy)).To(Succeed())
			Expect(racy.Status.Phase).NotTo(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete))
			Expect(racy.Status.Snapshot).To(BeNil())

			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, agentPod))).To(Succeed())
		})

		// Pins markerFlushGracePeriod's AFTER-grace branch.
		// Once Job.Status.CompletionTime is older than the grace
		// window, a still-marker-less log means the marker is
		// genuinely missing (a torn / truncated stream — the
		// agent's own code makes "clean exit without marker"
		// unreachable). The controller must finalize
		// Phase=Complete with status.snapshot=nil rather than
		// requeue forever, otherwise stuck pods burn the
		// reconciler indefinitely.
		It("Should finalize with empty snapshot on errNoMarker after the flush grace window", func() {
			cluster := createTestCluster(ctx, clusterName+"-flush-stale", nil)
			backup := createTestPVCBackup(ctx, backupName+"-flush-stale", cluster.Name)
			_ = cluster

			reconciler.LogStreamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
				return io.NopCloser(strings.NewReader(
					"taking etcd snapshot...\nwriting snapshot to /backup/data/x.db\n",
				)), nil
			}

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			job := getBackupJob(ctx, backup.Name)
			// Job completed well outside grace (well past 30s).
			stale := metav1.NewTime(time.Now().Add(-10 * time.Minute))
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &stale
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			agentPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: backup.Namespace,
					Name:      job.Name + "-agent",
					Labels:    map[string]string{"batch.kubernetes.io/job-name": job.Name},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "backup-agent",
						Image: "ghcr.io/aenix-io/etcd-operator:test",
					}},
				},
			}
			Expect(getK8sClient().Create(ctx, agentPod)).To(Succeed())
			agentPod.Status.Phase = corev1.PodSucceeded
			startTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
			agentPod.Status.StartTime = &startTime
			Expect(getK8sClient().Status().Update(ctx, agentPod)).To(Succeed())

			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			final := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, final)).To(Succeed())
			Expect(final.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete),
				"after-grace errNoMarker must finalize — otherwise stuck pods spin the reconciler forever")
			Expect(meta.IsStatusConditionTrue(final.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionComplete)).To(BeTrue())
			Expect(final.Status.Snapshot).To(BeNil())

			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, agentPod))).To(Succeed())
		})

		// A transient stream failure (apiserver hiccup, network
		// reset) must NOT cause the controller to finalize with an
		// empty snapshot. It must return the error so
		// controller-runtime drops the request back on the
		// workqueue with exponential backoff; the next reconcile
		// retries while the pod is still alive. The
		// pod-lifecycle-gated design relies on this: if a
		// transient failure during the kubelet's TTL window
		// silently finalized, restore would lose the URI even
		// though the log was about to become readable again.
		It("Should return an error and not finalize on a transient stream failure", func() {
			cluster := createTestCluster(ctx, clusterName+"-stream-err", nil)
			backup := createTestPVCBackup(ctx, backupName+"-stream-err", cluster.Name)

			var streamerCalls int
			reconciler.LogStreamer = func(ctx context.Context, ns, name, container string) (io.ReadCloser, error) {
				streamerCalls++
				return nil, errors.New("simulated apiserver stall")
			}

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			job := getBackupJob(ctx, backup.Name)
			now := metav1.Now()
			job.Status.Succeeded = 1
			job.Status.CompletionTime = &now
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			// Stage a Succeeded pod so extraction reaches the
			// streamer, which returns an error. The controller must
			// surface that error AND leave status unchanged.
			agentPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: backup.Namespace,
					Name:      job.Name + "-agent",
					Labels:    map[string]string{"batch.kubernetes.io/job-name": job.Name},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:  "backup-agent",
						Image: "ghcr.io/aenix-io/etcd-operator:test",
					}},
				},
			}
			Expect(getK8sClient().Create(ctx, agentPod)).To(Succeed())
			agentPod.Status.Phase = corev1.PodSucceeded
			streamStart := metav1.Now()
			agentPod.Status.StartTime = &streamStart
			Expect(getK8sClient().Status().Update(ctx, agentPod)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			// The streamer must actually have been invoked — that
			// guard is what proves extraction reached the streaming
			// step (previously the equivalent test wired a Fail()
			// streamer that never fired, leaving the branch dead).
			Expect(streamerCalls).To(BeNumerically(">=", 1))
			// Controller-runtime convention: errors are returned to
			// the workqueue, which retries with backoff. The
			// EtcdBackup must NOT have transitioned to Complete and
			// must NOT have a snapshot.
			Expect(err).To(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			racy := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, racy)).To(Succeed())
			Expect(racy.Status.Phase).NotTo(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete))
			Expect(racy.Status.Snapshot).To(BeNil())

			Expect(client.IgnoreNotFound(getK8sClient().Delete(ctx, agentPod))).To(Succeed())
		})

		// Pins the URI-pattern contract between the controller's
		// defense-in-depth regex (compiled from
		// etcdaenixiov1alpha1.SnapshotURIPrefixPattern) and the CRD
		// pattern (kubebuilder marker on BackupSnapshot.URI). The
		// two are documented as KEEP IN SYNC; if the kubebuilder
		// marker text drifts from the constant the two halves would
		// disagree silently — this test exercises both halves via the
		// running apiserver and fails when they diverge:
		//   - A URI that satisfies the controller regex MUST be
		//     accepted by Status().Update (otherwise the controller
		//     would spin on retry, never finalizing).
		//   - A URI that does NOT satisfy the controller regex MUST
		//     be refused by Status().Update (otherwise the controller
		//     would gate on the wrong schemes and our claim that the
		//     two halves agree is false).
		It("Should agree between the controller URI regex and the CRD URI pattern", func() {
			cluster := createTestCluster(ctx, clusterName+"-uripat", nil)
			backup := createTestPVCBackup(ctx, backupName+"-uripat", cluster.Name)
			_ = cluster

			controllerRegex := snapshotURIPrefixRegexp
			validURI := "s3://b/some/key.db"
			Expect(controllerRegex.MatchString(validURI)).To(BeTrue(),
				"sanity: %q must match the controller regex for the test premise to hold", validURI)
			backup.Status.Snapshot = &etcdaenixiov1alpha1.BackupSnapshot{
				URI:       validURI,
				SizeBytes: 1,
				Checksum:  "sha256:" + strings.Repeat("a", 64),
			}
			Expect(getK8sClient().Status().Update(ctx, backup)).To(Succeed(),
				"controller-approved URI %q must be accepted by the CRD pattern; the two are KEEP IN SYNC", validURI)

			roundTripped := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, roundTripped)).To(Succeed())
			Expect(roundTripped.Status.Snapshot).NotTo(BeNil())
			Expect(roundTripped.Status.Snapshot.URI).To(Equal(validURI))

			invalidURI := "https://b/some/key.db"
			Expect(controllerRegex.MatchString(invalidURI)).To(BeFalse(),
				"sanity: %q must NOT match the controller regex for the test premise to hold", invalidURI)
			roundTripped.Status.Snapshot.URI = invalidURI
			err := getK8sClient().Status().Update(ctx, roundTripped)
			Expect(err).To(HaveOccurred(),
				"controller-rejected URI %q must also be rejected by the CRD pattern; otherwise the patterns disagree", invalidURI)
		})

		// Three things conspire to make BackupSnapshot.URI
		// always non-empty: (a) the marker regex requires `+`
		// (one or more) for the captured URI, (b) the CRD field
		// is `+kubebuilder:validation:Required`, (c) the CRD
		// pattern `^(s3|file)://.+` itself rejects an empty
		// suffix. Only (a) is currently pinned by unit tests on
		// scanBackupAgentLog. If a future re-generate of the CRD
		// dropped the required marker OR the pattern were
		// loosened to `^(s3|file)://.*`, the in-process scanner
		// would still reject empty URIs but a Status().Update
		// hand-crafted by another controller (or a future
		// helper) could land an empty URI and break restore
		// callers reading status.snapshot.uri. This envtest
		// exercises the ACTUAL apiserver-installed CRD: an
		// empty URI must produce a 422 on Update.
		It("Should reject an empty URI on Status().Update", func() {
			cluster := createTestCluster(ctx, clusterName+"-emptyuri", nil)
			backup := createTestPVCBackup(ctx, backupName+"-emptyuri", cluster.Name)
			_ = cluster

			backup.Status.Snapshot = &etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "",
				SizeBytes: 1,
				Checksum:  "sha256:" + strings.Repeat("a", 64),
			}
			err := getK8sClient().Status().Update(ctx, backup)
			Expect(err).To(HaveOccurred(),
				"installed CRD must reject an empty BackupSnapshot.URI: the field is Required and the Pattern ^(s3|file)://.+ refuses an empty suffix")
		})

		// Pins the CRD's checksum LOWER BOUND. The Go marker on
		// BackupSnapshot.Checksum requires [a-f0-9]{32,128}; if the
		// generated CRD drifts back to the unbounded [a-f0-9]+ that
		// earlier versions emitted, a truncated emit like "sha256:a"
		// would silently round-trip through the apiserver and land
		// in status.snapshot as a meaningless 1-byte hash — defeating
		// the integrity property the field exists to provide. This
		// e2e test exercises the actually-installed CRD via envtest
		// and fails if the bound is missing.
		It("Should reject a too-short checksum on Status().Update", func() {
			cluster := createTestCluster(ctx, clusterName+"-shortsum", nil)
			backup := createTestPVCBackup(ctx, backupName+"-shortsum", cluster.Name)
			_ = cluster

			backup.Status.Snapshot = &etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "file:///backup/data/snap.db",
				SizeBytes: 1,
				Checksum:  "sha256:a", // 1 hex char — below the 32 minimum.
			}
			err := getK8sClient().Status().Update(ctx, backup)
			Expect(err).To(HaveOccurred(),
				"installed CRD must enforce the [a-f0-9]{32,128} bound on checksum; a 1-char hex digest must be refused")
		})

		// Pin issue #6: the CRD's checksum pattern must accept
		// hyphenated algorithm names (sha3-256, blake2b-256,
		// blake3-256). The doc comment on
		// BackupSnapshot.Checksum explicitly says "consumers MUST
		// tolerate other algorithms via the prefix" — yet a
		// too-narrow regex previously forbade exactly the algos a
		// future agent build is most likely to adopt. This is the
		// only place that actually exercises the apiserver's CRD
		// schema validation; if the pattern were narrowed again
		// the Update below would return a 422 and fail this test.
		It("Should accept hyphenated checksum algorithms on Status().Update", func() {
			cluster := createTestCluster(ctx, clusterName+"-hashalgo", nil)
			backup := createTestPVCBackup(ctx, backupName+"-hashalgo", cluster.Name)
			_ = cluster

			// Hex bodies are sized to span the new CRD bound
			// ([a-f0-9]{32,128}): exactly 32 chars (minimum), 64
			// chars (sha3-256 actual length), and 128 chars (sha-512
			// maximum). If a future tightening narrows the upper or
			// lower bound, the corresponding case here will start
			// failing the Status().Update.
			cases := []string{
				"sha3-256:" + strings.Repeat("a", 64),
				"blake2b-256:" + strings.Repeat("b", 32),
				"blake3-512:" + strings.Repeat("c", 128),
			}
			for _, checksum := range cases {
				backup.Status.Snapshot = &etcdaenixiov1alpha1.BackupSnapshot{
					URI:       "file:///backup/data/snap.db",
					SizeBytes: 1024,
					Checksum:  checksum,
				}
				Expect(getK8sClient().Status().Update(ctx, backup)).To(Succeed(),
					"CRD pattern must accept %q", checksum)
				roundTripped := &etcdaenixiov1alpha1.EtcdBackup{}
				Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, roundTripped)).To(Succeed())
				Expect(roundTripped.Status.Snapshot).NotTo(BeNil())
				Expect(roundTripped.Status.Snapshot.Checksum).To(Equal(checksum))
				backup = roundTripped
			}
		})
	})

	Context("When Job fails", func() {
		It("Should set Failed condition", func() {
			cluster := createTestCluster(ctx, clusterName+"-fail", nil)
			backup := createTestPVCBackup(ctx, backupName+"-fail", cluster.Name)

			// First reconcile: creates the Job
			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Simulate Job failure
			job := getBackupJob(ctx, backup.Name)
			job.Status.Failed = 1
			job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
				Type:    batchv1.JobFailed,
				Status:  "True",
				Reason:  "BackoffLimitExceeded",
				Message: "Job has reached the specified backoff limit",
			})
			Expect(getK8sClient().Status().Update(ctx, job)).To(Succeed())

			// Second reconcile: should set Failed
			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			updatedBackup := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
			Expect(updatedBackup.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed))
			Expect(meta.IsStatusConditionTrue(updatedBackup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed)).To(BeTrue())
		})
	})

	Context("When backup is already complete", func() {
		It("Should not create another Job", func() {
			cluster := createTestCluster(ctx, clusterName+"-done", nil)
			backup := createTestPVCBackup(ctx, backupName+"-done", cluster.Name)

			// Set Complete phase and condition directly
			backup.Status.Phase = etcdaenixiov1alpha1.EtcdBackupStatusPhaseComplete
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:    etcdaenixiov1alpha1.EtcdBackupConditionComplete,
				Status:  metav1.ConditionTrue,
				Reason:  "JobSucceeded",
				Message: "Backup completed",
			})
			Expect(getK8sClient().Status().Update(ctx, backup)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			// Verify no Job was created
			jobList := &batchv1.JobList{}
			Expect(getK8sClient().List(ctx, jobList,
				client.InNamespace(testNamespace),
				client.MatchingLabels{"etcd.aenix.io/etcdbackup-name": backup.Name},
			)).To(Succeed())
			Expect(jobList.Items).To(BeEmpty())
		})
	})

	Context("When EtcdBackup does not exist", func() {
		It("Should return without error", func() {
			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: testNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})
	})

	Context("With TLS-enabled cluster", func() {
		It("Should create Job with TLS volume mounts", func() {
			security := &etcdaenixiov1alpha1.SecuritySpec{
				TLS: etcdaenixiov1alpha1.TLSSpec{
					ClientSecret:          "client-cert",
					ClientTrustedCASecret: "client-ca",
					ServerSecret:          "server-cert",
					ServerTrustedCASecret: "server-ca",
				},
			}
			cluster := createTestCluster(ctx, clusterName+"-tls", security)
			backup := createTestPVCBackup(ctx, backupName+"-tls", cluster.Name)

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			job := getBackupJob(ctx, backup.Name)

			container := job.Spec.Template.Spec.Containers[0]

			// Check TLS env vars
			envNames := make([]string, len(container.Env))
			for i, e := range container.Env {
				envNames[i] = e.Name
			}
			Expect(envNames).To(ContainElement("ETCD_TLS_ENABLED"))
			Expect(envNames).To(ContainElement("ETCD_TLS_CERT_PATH"))
			Expect(envNames).To(ContainElement("ETCD_TLS_KEY_PATH"))
			Expect(envNames).To(ContainElement("ETCD_TLS_CA_PATH"))

			// Check volumes
			volumeNames := make([]string, len(job.Spec.Template.Spec.Volumes))
			for i, v := range job.Spec.Template.Spec.Volumes {
				volumeNames[i] = v.Name
			}
			Expect(volumeNames).To(ContainElement("client-certificate"))
			Expect(volumeNames).To(ContainElement("server-trusted-ca-certificate"))
		})
	})

	Context("When OperatorImage is empty", func() {
		It("Should set Failed condition instead of creating Job", func() {
			emptyImageReconciler := &EtcdBackupReconciler{
				Client:        getK8sClient(),
				Scheme:        getK8sClient().Scheme(),
				OperatorImage: "",
			}

			cluster := createTestCluster(ctx, "test-cluster-noimage", nil)
			backup := createTestPVCBackup(ctx, "test-backup-noimage", cluster.Name)

			result, err := emptyImageReconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			// Verify no Job was created
			jobList := &batchv1.JobList{}
			Expect(getK8sClient().List(ctx, jobList,
				client.InNamespace(backup.Namespace),
				client.MatchingLabels{"etcd.aenix.io/etcdbackup-name": backup.Name},
			)).To(Succeed())
			Expect(jobList.Items).To(BeEmpty())

			// Verify Failed phase and condition were set
			Expect(getK8sClient().Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backup)).To(Succeed())
			Expect(backup.Status.Phase).To(Equal(etcdaenixiov1alpha1.EtcdBackupStatusPhaseFailed))
			failedCond := meta.FindStatusCondition(backup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed)
			Expect(failedCond).NotTo(BeNil())
			Expect(failedCond.Reason).To(Equal("ConfigurationError"))
		})
	})
})

func getBackupJob(ctx context.Context, backupName string) *batchv1.Job {
	jobList := &batchv1.JobList{}
	ExpectWithOffset(1, getK8sClient().List(ctx, jobList,
		client.InNamespace(testNamespace),
		client.MatchingLabels{"etcd.aenix.io/etcdbackup-name": backupName},
	)).To(Succeed())
	ExpectWithOffset(1, jobList.Items).To(HaveLen(1))
	return &jobList.Items[0]
}

func createTestCluster(ctx context.Context, name string, security *etcdaenixiov1alpha1.SecuritySpec) *etcdaenixiov1alpha1.EtcdCluster {
	cluster := &etcdaenixiov1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
			Replicas: ptr.To(int32(3)),
			PodTemplate: etcdaenixiov1alpha1.PodTemplate{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "etcd", Image: "quay.io/coreos/etcd:v3.5.12"},
					},
				},
			},
			Storage: etcdaenixiov1alpha1.StorageSpec{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
			Security: security,
		},
	}
	Expect(getK8sClient().Create(ctx, cluster)).To(Succeed())
	return cluster
}

func createTestPVCBackup(ctx context.Context, name, clusterName string) *etcdaenixiov1alpha1.EtcdBackup {
	backup := &etcdaenixiov1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
			ClusterRef: corev1.LocalObjectReference{Name: clusterName},
			Destination: etcdaenixiov1alpha1.BackupDestination{
				PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
					ClaimName: "test-backup-pvc",
				},
			},
		},
	}
	Expect(getK8sClient().Create(ctx, backup)).To(Succeed())
	return backup
}

func createTestS3Backup(ctx context.Context, name, clusterName string) *etcdaenixiov1alpha1.EtcdBackup {
	backup := &etcdaenixiov1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
			ClusterRef: corev1.LocalObjectReference{Name: clusterName},
			Destination: etcdaenixiov1alpha1.BackupDestination{
				S3: &etcdaenixiov1alpha1.S3BackupDestination{
					Endpoint:             "https://s3.amazonaws.com",
					Bucket:               "test-bucket",
					Key:                  "backups/test.db",
					CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
					Region:               "us-east-1",
				},
			},
		},
	}
	Expect(getK8sClient().Create(ctx, backup)).To(Succeed())
	return backup
}
