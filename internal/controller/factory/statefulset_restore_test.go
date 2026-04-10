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

package factory

import (
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

var _ = Describe("StatefulSet restore init containers", func() {
	var ns *corev1.Namespace

	BeforeEach(func() {
		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-restore-",
			},
		}
		Expect(k8sClient.Create(ctx, ns)).Should(Succeed())
		DeferCleanup(k8sClient.Delete, ns)
	})

	Context("when bootstrap is nil", func() {
		It("should not add init containers", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-no-restore-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).Should(Succeed())
			Eventually(Get(cluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, cluster)

			Expect(CreateOrUpdateStatefulSet(ctx, cluster, k8sClient, "operator:latest")).To(Succeed())

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cluster.GetName(),
					Namespace: ns.GetName(),
				},
			}
			Eventually(Get(sts)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, sts)

			Expect(sts.Spec.Template.Spec.InitContainers).To(BeEmpty())
		})
	})

	Context("when bootstrap.restore with PVC source is set", func() {
		It("should add two init containers and restore volumes", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-pvc-restore-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &etcdaenixiov1alpha1.BootstrapSpec{
						Restore: &etcdaenixiov1alpha1.RestoreSpec{
							Source: etcdaenixiov1alpha1.BackupDestination{
								PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
									ClaimName: "my-backup-pvc",
									SubPath:   "snapshot.db",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).Should(Succeed())
			Eventually(Get(cluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, cluster)

			Expect(CreateOrUpdateStatefulSet(ctx, cluster, k8sClient, "operator:latest")).To(Succeed())

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cluster.GetName(),
					Namespace: ns.GetName(),
				},
			}
			Eventually(Get(sts)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, sts)

			By("Checking init containers count and names")
			Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(2))
			Expect(sts.Spec.Template.Spec.InitContainers[0].Name).To(Equal("restore-agent"))
			Expect(sts.Spec.Template.Spec.InitContainers[1].Name).To(Equal("restore-datadir"))

			By("Checking restore-agent container")
			restoreAgent := sts.Spec.Template.Spec.InitContainers[0]
			Expect(restoreAgent.Image).To(Equal("operator:latest"))
			Expect(restoreAgent.Command).To(Equal([]string{"/restore-agent"}))

			By("Checking restore-agent env vars for PVC")
			envMap := make(map[string]string)
			for _, env := range restoreAgent.Env {
				if env.ValueFrom == nil {
					envMap[env.Name] = env.Value
				}
			}
			Expect(envMap["RESTORE_SOURCE"]).To(Equal("pvc"))
			Expect(envMap["PVC_BACKUP_PATH"]).To(Equal("/backup/data/snapshot.db"))

			By("Checking restore-datadir container")
			restoreDatadir := sts.Spec.Template.Spec.InitContainers[1]
			Expect(restoreDatadir.Image).To(Equal(etcdaenixiov1alpha1.DefaultEtcdImage))
			Expect(restoreDatadir.Command).To(Equal([]string{"/bin/sh", "-c"}))
			Expect(restoreDatadir.Args).To(HaveLen(1))
			Expect(restoreDatadir.Args[0]).To(ContainSubstring("etcdutl snapshot restore"))

			By("Checking restore volumes exist")
			volumeNames := make([]string, 0)
			for _, v := range sts.Spec.Template.Spec.Volumes {
				volumeNames = append(volumeNames, v.Name)
			}
			Expect(volumeNames).To(ContainElement("restore-data"))
			Expect(volumeNames).To(ContainElement("backup-source"))
		})
	})

	Context("when bootstrap.restore with S3 source is set", func() {
		It("should add two init containers with S3 env vars", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-s3-restore-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &etcdaenixiov1alpha1.BootstrapSpec{
						Restore: &etcdaenixiov1alpha1.RestoreSpec{
							Source: etcdaenixiov1alpha1.BackupDestination{
								S3: &etcdaenixiov1alpha1.S3BackupDestination{
									Endpoint: "https://s3.example.com",
									Bucket:   "my-bucket",
									Key:      "backups/snapshot.db",
									CredentialsSecretRef: corev1.LocalObjectReference{
										Name: "s3-credentials",
									},
									Region:         "us-west-2",
									ForcePathStyle: true,
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).Should(Succeed())
			Eventually(Get(cluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, cluster)

			Expect(CreateOrUpdateStatefulSet(ctx, cluster, k8sClient, "operator:v1.0.0")).To(Succeed())

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cluster.GetName(),
					Namespace: ns.GetName(),
				},
			}
			Eventually(Get(sts)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, sts)

			By("Checking init containers count")
			Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(2))

			By("Checking restore-agent S3 env vars")
			restoreAgent := sts.Spec.Template.Spec.InitContainers[0]
			Expect(restoreAgent.Image).To(Equal("operator:v1.0.0"))

			envMap := make(map[string]string)
			var secretRefEnvs []string
			for _, env := range restoreAgent.Env {
				if env.ValueFrom == nil {
					envMap[env.Name] = env.Value
				} else if env.ValueFrom.SecretKeyRef != nil {
					secretRefEnvs = append(secretRefEnvs, env.Name)
				}
			}
			Expect(envMap["RESTORE_SOURCE"]).To(Equal("s3"))
			Expect(envMap["S3_ENDPOINT"]).To(Equal("https://s3.example.com"))
			Expect(envMap["S3_BUCKET"]).To(Equal("my-bucket"))
			Expect(envMap["S3_KEY"]).To(Equal("backups/snapshot.db"))
			Expect(envMap["S3_REGION"]).To(Equal("us-west-2"))
			Expect(envMap["S3_FORCE_PATH_STYLE"]).To(Equal("true"))
			Expect(secretRefEnvs).To(ContainElement("AWS_ACCESS_KEY_ID"))
			Expect(secretRefEnvs).To(ContainElement("AWS_SECRET_ACCESS_KEY"))

			By("Checking no backup-source PVC volume for S3")
			for _, v := range sts.Spec.Template.Spec.Volumes {
				Expect(v.Name).NotTo(Equal("backup-source"))
			}
		})
	})

	Context("when bootstrap.restore with PVC source without SubPath", func() {
		It("should use default snapshot path", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-pvc-nosubpath-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &etcdaenixiov1alpha1.BootstrapSpec{
						Restore: &etcdaenixiov1alpha1.RestoreSpec{
							Source: etcdaenixiov1alpha1.BackupDestination{
								PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
									ClaimName: "my-backup-pvc",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).Should(Succeed())
			Eventually(Get(cluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, cluster)

			Expect(CreateOrUpdateStatefulSet(ctx, cluster, k8sClient, "operator:latest")).To(Succeed())

			sts := &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      cluster.GetName(),
					Namespace: ns.GetName(),
				},
			}
			Eventually(Get(sts)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, sts)

			restoreAgent := sts.Spec.Template.Spec.InitContainers[0]
			envMap := make(map[string]string)
			for _, env := range restoreAgent.Env {
				if env.ValueFrom == nil {
					envMap[env.Name] = env.Value
				}
			}
			Expect(envMap["PVC_BACKUP_PATH"]).To(Equal("/backup/data/snapshot.db"))
		})
	})
})
