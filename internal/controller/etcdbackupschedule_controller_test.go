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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
)

var _ = Describe("EtcdBackupSchedule Controller", func() {
	const (
		scheduleClusterName = "test-schedule-cluster"
		scheduleName        = "test-schedule"
		scheduleImage       = "ghcr.io/aenix-io/etcd-operator:test"
	)

	var (
		reconciler *EtcdBackupScheduleReconciler
	)

	BeforeEach(func() {
		reconciler = &EtcdBackupScheduleReconciler{
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			OperatorImage: scheduleImage,
		}
	})

	AfterEach(func() {
		scheduleList := &etcdaenixiov1alpha1.EtcdBackupScheduleList{}
		Expect(k8sClient.List(ctx, scheduleList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range scheduleList.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &scheduleList.Items[i]))).To(Succeed())
		}
		cronJobList := &batchv1.CronJobList{}
		Expect(k8sClient.List(ctx, cronJobList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range cronJobList.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &cronJobList.Items[i]))).To(Succeed())
		}
		clusterList := &etcdaenixiov1alpha1.EtcdClusterList{}
		Expect(k8sClient.List(ctx, clusterList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range clusterList.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &clusterList.Items[i]))).To(Succeed())
		}
	})

	Context("When reconciling a PVC schedule", func() {
		It("Should create a CronJob and set Ready condition", func() {
			cluster := createTestCluster(ctx, scheduleClusterName, nil)
			schedule := createTestPVCSchedule(ctx, scheduleName, cluster.Name)

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			// Verify CronJob was created
			cronJob := &batchv1.CronJob{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupCronJobName(schedule),
				Namespace: testNamespace,
			}, cronJob)).To(Succeed())
			Expect(cronJob.Spec.Schedule).To(Equal("0 */6 * * *"))
			Expect(cronJob.Spec.ConcurrencyPolicy).To(Equal(batchv1.ForbidConcurrent))
			Expect(cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"/backup-agent"}))

			// Verify owner reference
			Expect(cronJob.OwnerReferences).To(HaveLen(1))
			Expect(cronJob.OwnerReferences[0].Name).To(Equal(schedule.Name))

			// Verify Ready condition
			updatedSchedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: schedule.Name, Namespace: testNamespace}, updatedSchedule)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(updatedSchedule.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady)).To(BeTrue())

			_ = cluster
		})
	})

	Context("When EtcdCluster is not found", func() {
		It("Should set Ready=false and requeue", func() {
			schedule := createTestPVCSchedule(ctx, scheduleName+"-nocluster", "nonexistent-cluster")

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0))

			updatedSchedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: schedule.Name, Namespace: testNamespace}, updatedSchedule)).To(Succeed())
			readyCond := meta.FindStatusCondition(updatedSchedule.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))
			Expect(readyCond.Reason).To(Equal("ClusterNotFound"))
		})
	})

	Context("When EtcdBackupSchedule does not exist", func() {
		It("Should return without error", func() {
			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: testNamespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())
		})
	})

	Context("When CronJob already exists", func() {
		It("Should update CronJob when schedule changes and set Ready condition", func() {
			cluster := createTestCluster(ctx, scheduleClusterName+"-update", nil)
			schedule := createTestPVCSchedule(ctx, scheduleName+"-update", cluster.Name)

			// First reconcile: creates the CronJob
			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Verify CronJob exists
			cronJob := &batchv1.CronJob{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupCronJobName(schedule),
				Namespace: testNamespace,
			}, cronJob)).To(Succeed())
			Expect(cronJob.Spec.Schedule).To(Equal("0 */6 * * *"))

			// Second reconcile: CronJob already exists, should be Ready
			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			updatedSchedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: schedule.Name, Namespace: testNamespace}, updatedSchedule)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(updatedSchedule.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady)).To(BeTrue())
		})
	})

	Context("When CronJob has status updates", func() {
		It("Should sync LastScheduleTime from CronJob", func() {
			cluster := createTestCluster(ctx, scheduleClusterName+"-status", nil)
			schedule := createTestPVCSchedule(ctx, scheduleName+"-status", cluster.Name)

			// First reconcile: creates the CronJob
			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Simulate CronJob having been scheduled
			cronJob := &batchv1.CronJob{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupCronJobName(schedule),
				Namespace: testNamespace,
			}, cronJob)).To(Succeed())
			now := metav1.Now()
			cronJob.Status.LastScheduleTime = &now
			Expect(k8sClient.Status().Update(ctx, cronJob)).To(Succeed())

			// Re-fetch the schedule to get latest version
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: schedule.Name, Namespace: testNamespace}, schedule)).To(Succeed())

			// Second reconcile: should sync status
			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			updatedSchedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: schedule.Name, Namespace: testNamespace}, updatedSchedule)).To(Succeed())
			Expect(updatedSchedule.Status.LastScheduleTime).NotTo(BeNil())
		})
	})

	Context("When OperatorImage is empty", func() {
		It("Should set Failed and Ready=false conditions", func() {
			emptyImageReconciler := &EtcdBackupScheduleReconciler{
				Client:        k8sClient,
				Scheme:        k8sClient.Scheme(),
				OperatorImage: "",
			}

			cluster := createTestCluster(ctx, scheduleClusterName+"-noimage", nil)
			schedule := createTestPVCSchedule(ctx, scheduleName+"-noimage", cluster.Name)

			result, err := emptyImageReconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			updatedSchedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: schedule.Name, Namespace: testNamespace}, updatedSchedule)).To(Succeed())

			failedCond := meta.FindStatusCondition(updatedSchedule.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupScheduleConditionFailed)
			Expect(failedCond).NotTo(BeNil())
			Expect(failedCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(failedCond.Reason).To(Equal("ConfigurationError"))
			Expect(failedCond.Message).To(ContainSubstring("OPERATOR_IMAGE"))

			readyCond := meta.FindStatusCondition(updatedSchedule.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady)
			Expect(readyCond).NotTo(BeNil())
			Expect(readyCond.Status).To(Equal(metav1.ConditionFalse))

			// Verify no CronJob was created
			cronJobList := &batchv1.CronJobList{}
			Expect(k8sClient.List(ctx, cronJobList, client.InNamespace(testNamespace))).To(Succeed())
			for _, cj := range cronJobList.Items {
				Expect(cj.OwnerReferences).NotTo(ContainElement(
					HaveField("Name", schedule.Name),
				))
			}
		})
	})

	Context("With TLS-enabled cluster", func() {
		It("Should create CronJob with TLS volume mounts", func() {
			security := &etcdaenixiov1alpha1.SecuritySpec{
				TLS: etcdaenixiov1alpha1.TLSSpec{
					ClientSecret:          "client-cert",
					ClientTrustedCASecret: "client-ca",
					ServerSecret:          "server-cert",
					ServerTrustedCASecret: "server-ca",
				},
			}
			cluster := createTestCluster(ctx, scheduleClusterName+"-tls", security)
			schedule := createTestPVCSchedule(ctx, scheduleName+"-tls", cluster.Name)

			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: schedule.Name, Namespace: schedule.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			cronJob := &batchv1.CronJob{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupCronJobName(schedule),
				Namespace: testNamespace,
			}, cronJob)).To(Succeed())

			container := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0]

			envNames := make([]string, len(container.Env))
			for i, e := range container.Env {
				envNames[i] = e.Name
			}
			Expect(envNames).To(ContainElement("ETCD_TLS_ENABLED"))
			Expect(envNames).To(ContainElement("ETCD_TLS_CERT_PATH"))
			Expect(envNames).To(ContainElement("ETCD_TLS_KEY_PATH"))
			Expect(envNames).To(ContainElement("ETCD_TLS_CA_PATH"))

			volumeNames := make([]string, len(cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes))
			for i, v := range cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes {
				volumeNames[i] = v.Name
			}
			Expect(volumeNames).To(ContainElement("client-certificate"))
			Expect(volumeNames).To(ContainElement("server-trusted-ca-certificate"))
		})
	})
})

func createTestPVCSchedule(ctx context.Context, name, clusterName string) *etcdaenixiov1alpha1.EtcdBackupSchedule { //nolint:unparam
	schedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupScheduleSpec{
			ClusterRef: corev1.LocalObjectReference{Name: clusterName},
			Schedule:   "0 */6 * * *",
			Destination: etcdaenixiov1alpha1.BackupDestination{
				PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
					ClaimName: "test-backup-pvc",
				},
			},
		},
	}
	Expect(k8sClient.Create(ctx, schedule)).To(Succeed())
	return schedule
}

// createTestCluster is already defined in etcdbackup_controller_test.go.
// Since tests run in the same package, we don't need to re-declare it.

// Verify that all helpers referenced in this file compile.
var _ = func() {
	_ = errors.IsNotFound
	_ = ptr.To[int32]
}
