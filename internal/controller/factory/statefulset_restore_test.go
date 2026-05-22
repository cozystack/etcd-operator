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

			// Without bootstrap.restore there must be no FSGroup
			// override, since the restore-specific PodSecurityContext
			// only exists to chgrp the freshly-provisioned PVC for the
			// nonroot restore-agent. Asserting this locks the
			// contract: future code that always sets FSGroup would
			// silently change pod startup semantics (and the apparent
			// uid/gid of every file etcd touches) for every cluster.
			// The PodSecurityContext struct itself may be non-nil here
			// because of strategic-merge with cluster.Spec.PodTemplate;
			// what matters is that no FSGroup leaks in.
			if sc := sts.Spec.Template.Spec.SecurityContext; sc != nil {
				Expect(sc.FSGroup).To(BeNil())
			}
		})
	})

	Context("when bootstrap.restore with PVC source is set", func() {
		It("should add restore-agent init container and restore volumes", func() {
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
			// restore-agent now does both download AND etcdutl restore
			// in one Go process. The legacy restore-datadir initContainer
			// used /bin/sh -c which is unavailable in distroless etcd
			// images, so it was dropped entirely.
			Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.InitContainers[0].Name).To(Equal("restore-agent"))

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

			By("Checking restore-agent mounts the etcd data volume so etcdutl Restore() can write member/")
			Expect(restoreAgent.VolumeMounts).To(ContainElement(
				And(HaveField("Name", "data"), HaveField("MountPath", "/var/run/etcd"))))

			By("Checking restore-agent has envFrom cluster-state ConfigMap")
			Expect(restoreAgent.EnvFrom).To(HaveLen(1))
			Expect(restoreAgent.EnvFrom[0].ConfigMapRef).NotTo(BeNil())

			By("Checking restore volumes exist")
			volumeNames := make([]string, 0)
			for _, v := range sts.Spec.Template.Spec.Volumes {
				volumeNames = append(volumeNames, v.Name)
			}
			Expect(volumeNames).To(ContainElement("restore-data"))
			Expect(volumeNames).To(ContainElement("backup-source"))

			By("Checking PodSecurityContext.FSGroup is 65532 for restore")
			// Required so the nonroot restore-agent (uid 65532 from
			// distroless/static:nonroot) can mkdir into a freshly
			// provisioned PVC that kubelet mounts as root:root 0755.
			// Without this the initContainer hard-fails with
			// `mkdir /var/run/etcd/default.etcd: permission denied`
			// on every restore.
			Expect(sts.Spec.Template.Spec.SecurityContext).NotTo(BeNil())
			Expect(sts.Spec.Template.Spec.SecurityContext.FSGroup).NotTo(BeNil())
			Expect(*sts.Spec.Template.Spec.SecurityContext.FSGroup).To(Equal(int64(65532)))
		})
	})

	Context("when bootstrap.restore with S3 source is set", func() {
		It("should add restore-agent init container with S3 env vars", func() {
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
			Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(1))

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

	Context("when bootstrap.restore.source.pvc.subPath escapes the mount", func() {
		It("should refuse to render the StatefulSet", func() {
			// Mirrors the backup-side hardening: the restore SubPath
			// is interpolated into PVC_BACKUP_PATH, so a `..` segment
			// would let the agent read from a sibling of the mounted
			// PVC. Read-only on the restore side and bounded by the
			// pod's own filesystem, but the published spec is the
			// place to reject it — same as backup destinations.
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-pvc-restore-bad-subpath-",
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
									SubPath:   "../escape.db",
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).Should(Succeed())
			Eventually(Get(cluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, cluster)

			err := CreateOrUpdateStatefulSet(ctx, cluster, k8sClient, "operator:latest")
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("pvc.subPath"))
		})
	})
})
