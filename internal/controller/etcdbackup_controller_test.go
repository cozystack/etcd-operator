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
			Client:        k8sClient,
			Scheme:        k8sClient.Scheme(),
			OperatorImage: operatorImage,
		}
	})

	AfterEach(func() {
		// Cleanup resources
		backupList := &etcdaenixiov1alpha1.EtcdBackupList{}
		Expect(k8sClient.List(ctx, backupList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range backupList.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &backupList.Items[i]))).To(Succeed())
		}
		clusterList := &etcdaenixiov1alpha1.EtcdClusterList{}
		Expect(k8sClient.List(ctx, clusterList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range clusterList.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &clusterList.Items[i]))).To(Succeed())
		}
		jobList := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobList, client.InNamespace(testNamespace))).To(Succeed())
		for i := range jobList.Items {
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &jobList.Items[i], client.PropagationPolicy(metav1.DeletePropagationBackground)))).To(Succeed())
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
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupJobName(backup),
				Namespace: testNamespace,
			}, job)).To(Succeed())
			Expect(job.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(job.Spec.Template.Spec.Containers[0].Command).To(Equal([]string{"/backup-agent"}))
			Expect(*job.Spec.BackoffLimit).To(Equal(int32(0)))

			// Verify owner reference
			Expect(job.OwnerReferences).To(HaveLen(1))
			Expect(job.OwnerReferences[0].Name).To(Equal(backup.Name))

			// Verify Started condition
			updatedBackup := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
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

			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupJobName(backup),
				Namespace: testNamespace,
			}, job)).To(Succeed())

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
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
			failedCond := meta.FindStatusCondition(updatedBackup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed)
			Expect(failedCond).NotTo(BeNil())
			Expect(failedCond.Status).To(Equal(metav1.ConditionTrue))
			Expect(failedCond.Reason).To(Equal("ClusterNotFound"))
		})
	})

	Context("When Job succeeds", func() {
		It("Should set Complete condition", func() {
			cluster := createTestCluster(ctx, clusterName+"-success", nil)
			backup := createTestPVCBackup(ctx, backupName+"-success", cluster.Name)

			// First reconcile: creates the Job
			_, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			// Simulate Job success
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupJobName(backup),
				Namespace: testNamespace,
			}, job)).To(Succeed())
			job.Status.Succeeded = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			// Second reconcile: should set Complete
			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			updatedBackup := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(updatedBackup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionComplete)).To(BeTrue())
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
			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupJobName(backup),
				Namespace: testNamespace,
			}, job)).To(Succeed())
			job.Status.Failed = 1
			Expect(k8sClient.Status().Update(ctx, job)).To(Succeed())

			// Second reconcile: should set Failed
			_, err = reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())

			updatedBackup := &etcdaenixiov1alpha1.EtcdBackup{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: testNamespace}, updatedBackup)).To(Succeed())
			Expect(meta.IsStatusConditionTrue(updatedBackup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed)).To(BeTrue())
		})
	})

	Context("When backup is already complete", func() {
		It("Should not create another Job", func() {
			cluster := createTestCluster(ctx, clusterName+"-done", nil)
			backup := createTestPVCBackup(ctx, backupName+"-done", cluster.Name)

			// Set Complete condition directly
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:    etcdaenixiov1alpha1.EtcdBackupConditionComplete,
				Status:  metav1.ConditionTrue,
				Reason:  "JobSucceeded",
				Message: "Backup completed",
			})
			Expect(k8sClient.Status().Update(ctx, backup)).To(Succeed())

			result, err := reconciler.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Requeue).To(BeFalse())

			// Verify no Job was created
			job := &batchv1.Job{}
			err = k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupJobName(backup),
				Namespace: testNamespace,
			}, job)
			Expect(errors.IsNotFound(err)).To(BeTrue())
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

			job := &batchv1.Job{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      factory.GetBackupJobName(backup),
				Namespace: testNamespace,
			}, job)).To(Succeed())

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
				Client:        k8sClient,
				Scheme:        k8sClient.Scheme(),
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
			jobName := factory.GetBackupJobName(backup)
			job := &batchv1.Job{}
			err = k8sClient.Get(ctx, types.NamespacedName{Name: jobName, Namespace: backup.Namespace}, job)
			Expect(errors.IsNotFound(err)).To(BeTrue())

			// Verify Failed condition was set
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, backup)).To(Succeed())
			failedCond := meta.FindStatusCondition(backup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed)
			Expect(failedCond).NotTo(BeNil())
			Expect(failedCond.Reason).To(Equal("ConfigurationError"))
		})
	})
})

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
	Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
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
	Expect(k8sClient.Create(ctx, backup)).To(Succeed())
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
	Expect(k8sClient.Create(ctx, backup)).To(Succeed())
	return backup
}
