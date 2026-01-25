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
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/types"
)

var _ = Describe("StatefulSet factory", func() {
	var ns *corev1.Namespace

	BeforeEach(func() {
		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-",
			},
		}
		Expect(k8sClient.Create(ctx, ns)).Should(Succeed())
		DeferCleanup(k8sClient.Delete, ns)
	})

	Context("PodLabels", func() {
		It("should return base labels with custom labels merged", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{},
			}

			cluster.Spec.PodTemplate.Labels = map[string]string{
				"custom-label":           "value",
				"app.kubernetes.io/name": "override",
			}

			labels := PodLabels(cluster)
			Expect(labels).To(HaveKeyWithValue("custom-label", "value"))
			Expect(labels).To(HaveKeyWithValue("app.kubernetes.io/name", "override"))
		})

		It("should handle nil custom labels", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
			}
			labels := PodLabels(cluster)
			Expect(labels).Should(HaveLen(3))
		})
	})

	Context("GenerateEtcdArgs", func() {
		It("should correctly fill options to args", func() {
			extraArgs := map[string]string{
				"key1": "value1",
				"key2": "value2",
			}
			etcdcluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Options: extraArgs,
				},
			}

			args := GenerateEtcdArgs(etcdcluster)

			Expect(args).To(ContainElements([]string{
				"--key1=value1",
				"--key2=value2",
			}))
		})
		It("should not override user defined quota-backend-bytes", func() {
			etcdCluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Options: map[string]string{
						"quota-backend-bytes": "2147483648", // 2Gi
					},
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							SizeLimit: ptr.To(resource.MustParse("2Gi")),
						},
					},
				},
			}
			args := GenerateEtcdArgs(etcdCluster)
			Expect(args).To(ContainElement("--quota-backend-bytes=2147483648"))
		})
		It("should set quota-backend-bytes to 0.95 of EmptyDir size", func() {
			etcdCluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							SizeLimit: ptr.To(resource.MustParse("2Gi")),
						},
					},
				},
			}
			args := GenerateEtcdArgs(etcdCluster)
			// 2Gi * 0.95 = 2040109465,6
			Expect(args).To(ContainElement("--quota-backend-bytes=2040109465"))
		})
		It("should set quota-backend-bytes to 0.95 of PVC size", func() {
			etcdCluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Storage: etcdaenixiov1alpha1.StorageSpec{
						VolumeClaimTemplate: etcdaenixiov1alpha1.EmbeddedPersistentVolumeClaim{
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
								StorageClassName: ptr.To("local-path"),
								Resources: corev1.VolumeResourceRequirements{
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceStorage: resource.MustParse("2Gi"),
									},
								},
							},
						},
					},
				},
			}
			args := GenerateEtcdArgs(etcdCluster)
			// 2Gi * 0.95 = 2040109465,6
			Expect(args).To(ContainElement("--quota-backend-bytes=2040109465"))
		})

		It("should handle zero storage size", func() {
			etcdCluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			}
			args := GenerateEtcdArgs(etcdCluster)
			Expect(args).To(HaveLen(10))
		})
	})

	Context("generateVolumes", func() {
		It("should create EmptyDir volume when specified", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			}
			volumes := generateVolumes(cluster)
			Expect(volumes).To(HaveLen(1))
			Expect(volumes[0].EmptyDir).NotTo(BeNil())
		})

		It("should create PVC volume when no EmptyDir", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Storage: etcdaenixiov1alpha1.StorageSpec{
						VolumeClaimTemplate: etcdaenixiov1alpha1.EmbeddedPersistentVolumeClaim{
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
							},
						},
					},
				},
			}
			volumes := generateVolumes(cluster)
			Expect(volumes).To(HaveLen(1))
			Expect(volumes[0].PersistentVolumeClaim).NotTo(BeNil())
			Expect(volumes[0].PersistentVolumeClaim.ClaimName).To(Equal(GetPVCName(cluster)))
		})

		It("should add TLS volumes when security enabled", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Security: &etcdaenixiov1alpha1.SecuritySpec{
						TLS: etcdaenixiov1alpha1.TLSSpec{
							PeerSecret:            "peer-secret",
							PeerTrustedCASecret:   "peer-ca-secret",
							ServerSecret:          "server-secret",
							ClientSecret:          "client-secret",
							ClientTrustedCASecret: "client-ca-secret",
						},
					},
				},
			}
			volumes := generateVolumes(cluster)
			Expect(volumes).To(HaveLen(5)) // data + 4 TLS volumes
		})
	})

	Context("generateContainer", func() {
		It("should create etcd container with correct configuration", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
				},
			}

			container := generateContainer(cluster)

			Expect(container.Name).To(Equal(etcdContainerName))
			Expect(container.Image).To(Equal(etcdaenixiov1alpha1.DefaultEtcdImage))
			Expect(container.Command).To(Equal(GenerateEtcdCommand()))
			Expect(container.Ports).To(HaveLen(2))

			Expect(container.StartupProbe).NotTo(BeNil())
			Expect(container.LivenessProbe).NotTo(BeNil())
			Expect(container.ReadinessProbe).NotTo(BeNil())

			Expect(container.Env).To(HaveLen(2))
			Expect(container.Env[0].Name).To(Equal("POD_NAME"))
			Expect(container.Env[1].Name).To(Equal("POD_NAMESPACE"))
		})

		It("should include ConfigMap envFrom", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-cluster",
				},
			}

			container := generateContainer(cluster)

			Expect(container.EnvFrom).To(HaveLen(1))
			Expect(container.EnvFrom[0].ConfigMapRef).NotTo(BeNil())
			Expect(container.EnvFrom[0].ConfigMapRef.Name).To(Equal(GetClusterStateConfigMapName(cluster)))
		})
	})

	Context("generateVolumeMounts", func() {
		It("should create data volume mount only when no TLS", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{}

			mounts := generateVolumeMounts(cluster)

			Expect(mounts).To(HaveLen(1))
			Expect(mounts[0].Name).To(Equal("data"))
			Expect(mounts[0].MountPath).To(Equal("/var/run/etcd"))
			Expect(mounts[0].ReadOnly).To(BeFalse())
		})

		It("should create all TLS volume mounts when security enabled", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Security: &etcdaenixiov1alpha1.SecuritySpec{
						TLS: etcdaenixiov1alpha1.TLSSpec{
							PeerSecret:            "peer-secret",
							PeerTrustedCASecret:   "peer-ca-secret",
							ServerSecret:          "server-secret",
							ClientSecret:          "client-secret",
							ClientTrustedCASecret: "client-ca-secret",
						},
					},
				},
			}
			mounts := generateVolumeMounts(cluster)
			Expect(mounts).To(HaveLen(5))

			mountInfo := map[string]struct {
				path     string
				readOnly bool
			}{}
			for _, m := range mounts {
				mountInfo[m.Name] = struct {
					path     string
					readOnly bool
				}{m.MountPath, m.ReadOnly}
			}

			Expect(mountInfo).To(HaveKey("data"))
			Expect(mountInfo["data"].path).To(Equal("/var/run/etcd"))
			Expect(mountInfo["data"].readOnly).To(BeFalse())

			Expect(mountInfo).To(HaveKey("peer-trusted-ca-certificate"))
			Expect(mountInfo["peer-trusted-ca-certificate"].path).To(Equal("/etc/etcd/pki/peer/ca"))
			Expect(mountInfo["peer-trusted-ca-certificate"].readOnly).To(BeTrue())

			Expect(mountInfo).To(HaveKey("peer-certificate"))
			Expect(mountInfo["peer-certificate"].path).To(Equal("/etc/etcd/pki/peer/cert"))
			Expect(mountInfo["peer-certificate"].readOnly).To(BeTrue())

			Expect(mountInfo).To(HaveKey("server-certificate"))
			Expect(mountInfo["server-certificate"].path).To(Equal("/etc/etcd/pki/server/cert"))
			Expect(mountInfo["server-certificate"].readOnly).To(BeTrue())

			Expect(mountInfo).To(HaveKey("client-trusted-ca-certificate"))
			Expect(mountInfo["client-trusted-ca-certificate"].path).To(Equal("/etc/etcd/pki/client/ca"))
			Expect(mountInfo["client-trusted-ca-certificate"].readOnly).To(BeTrue())
		})
	})

	Context("GetStatefulSet", func() {
		var etcdcluster etcdaenixiov1alpha1.EtcdCluster

		BeforeEach(func() {
			etcdcluster = etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-etcdcluster-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Options: map[string]string{
						"foo": "bar",
					},
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)
		})

		It("should create StatefulSet with all required fields", func() {
			statefulSetObj, err := GetStatefulSet(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(statefulSetObj).ShouldNot(BeNil())

			Expect(statefulSetObj.Name).To(Equal(etcdcluster.Name))
			Expect(statefulSetObj.Namespace).To(Equal(etcdcluster.Namespace))
			Expect(statefulSetObj.Spec.Replicas).To(Equal(etcdcluster.Spec.Replicas))
			Expect(statefulSetObj.Spec.ServiceName).To(Equal(GetHeadlessServiceName(&etcdcluster)))
			Expect(statefulSetObj.Spec.PodManagementPolicy).To(Equal(appsv1.ParallelPodManagement))

			var hasEtcdContainer bool
			for _, c := range statefulSetObj.Spec.Template.Spec.Containers {
				if c.Name == etcdContainerName {
					hasEtcdContainer = true
					break
				}
			}
			Expect(hasEtcdContainer).To(BeTrue())
		})
	})

	Context("when handling edge cases and errors", func() {
		It("should handle nil storage gracefully", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-etcdcluster-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
				},
			}
			Expect(k8sClient.Create(ctx, cluster)).Should(Succeed())
			defer k8sClient.Delete(ctx, cluster)

			statefulSet, err := GetStatefulSet(ctx, cluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(statefulSet.Spec.VolumeClaimTemplates).To(HaveLen(1))
		})
	})

	/* TODO: all of the following tests validate merging logic, but all merging logic is now handled externally.
		These tests now need a rewrite.

	Context("When getting liveness probe", func() {
		It("should correctly get default values", func() {
			probe := getLivenessProbe(nil)
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/livez",
						Port: intstr.FromInt32(2381),
					},
				},
				PeriodSeconds: 5,
			}))
		})
		It("should correctly override all values", func() {
			probe := getLivenessProbe(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/liveznew",
						Port: intstr.FromInt32(2390),
					},
				},
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			})
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/liveznew",
						Port: intstr.FromInt32(2390),
					},
				},
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			}))
		})
		It("should correctly override partial changes", func() {
			probe := getLivenessProbe(&corev1.Probe{
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			})
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/livez",
						Port: intstr.FromInt32(2381),
					},
				},
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			}))
		})
	})

	Context("When getting startup probe", func() {
		It("should correctly get default values", func() {
			probe := getStartupProbe(nil)
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/readyz?serializable=false",
						Port: intstr.FromInt32(2381),
					},
				},
				PeriodSeconds: 5,
			}))
		})
		It("should correctly override all values", func() {
			probe := getStartupProbe(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/readyz",
						Port: intstr.FromInt32(2390),
					},
				},
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			})
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/readyz",
						Port: intstr.FromInt32(2390),
					},
				},
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			}))
		})
		It("should correctly override partial changes", func() {
			probe := getStartupProbe(&corev1.Probe{
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			})
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/readyz?serializable=false",
						Port: intstr.FromInt32(2381),
					},
				},
				InitialDelaySeconds: 7,
				PeriodSeconds:       3,
			}))
		})
	})

	Context("When getting liveness probe", func() {
		It("should correctly get default values", func() {
			probe := getLivenessProbe(nil)
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/livez",
						Port: intstr.FromInt32(2381),
					},
				},
				PeriodSeconds: 5,
			}))
		})
		It("should correctly override all values", func() {
			probe := getLivenessProbe(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/liveznew",
						Port: intstr.FromInt32(2371),
					},
				},
				InitialDelaySeconds: 11,
				PeriodSeconds:       13,
			})
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/liveznew",
						Port: intstr.FromInt32(2371),
					},
				},
				InitialDelaySeconds: 11,
				PeriodSeconds:       13,
			}))
		})
		It("should correctly override partial changes", func() {
			probe := getLivenessProbe(&corev1.Probe{
				InitialDelaySeconds: 11,
				PeriodSeconds:       13,
			})
			Expect(probe).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/livez",
						Port: intstr.FromInt32(2381),
					},
				},
				InitialDelaySeconds: 11,
				PeriodSeconds:       13,
			}))
		})
	})

	Context("When merge with default probe", func() {
		It("should correctly merge probe with default", func() {
			defaultProbe := corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{
						Path: "/livez",
						Port: intstr.FromInt32(2379),
					},
				},
				PeriodSeconds: 5,
			}
			defaultProbeCopy := defaultProbe.DeepCopy()

			probe := &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{"test"},
					},
				},
				InitialDelaySeconds: 11,
			}
			result := mergeWithDefaultProbe(probe, defaultProbe)
			Expect(result).To(Equal(&corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					Exec: &corev1.ExecAction{
						Command: []string{"test"},
					},
				},
				InitialDelaySeconds: 11,
				PeriodSeconds:       5,
			}))
			By("Shouldn't mutate default probe", func() {
				Expect(defaultProbe).To(Equal(*defaultProbeCopy))
			})
		})
	})

	Context("Generating Pod spec.containers", func() {
		etcdCluster := &etcdaenixiov1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      resourceName,
				Namespace: "default",
				UID:       "test-uid",
			},
			Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
				Replicas: ptr.To(int32(3)),
			},
		}
		etcdCluster.Default()
		It("should generate default containers", func() {
			containers := generateContainers(etcdCluster)
			if Expect(containers).To(HaveLen(1)) {
				Expect(containers[0].StartupProbe).NotTo(BeNil())
				Expect(containers[0].LivenessProbe).NotTo(BeNil())
				Expect(containers[0].ReadinessProbe).NotTo(BeNil())
				if Expect(containers[0].VolumeMounts).To(HaveLen(1)) {
					Expect(containers[0].VolumeMounts[0].Name).To(Equal("data"))
					Expect(containers[0].VolumeMounts[0].MountPath).To(Equal("/var/run/etcd"))
				}
			}
		})
		It("should merge fields without collisions correctly", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodTemplate.Spec.Containers[0].VolumeMounts = append(
				localCluster.Spec.PodTemplate.Spec.Containers[0].VolumeMounts,
				corev1.VolumeMount{
					Name:      "test",
					MountPath: "/test/tmp.txt",
				},
			)
			localCluster.Spec.PodTemplate.Spec.Containers[0].EnvFrom = append(
				localCluster.Spec.PodTemplate.Spec.Containers[0].EnvFrom,
				corev1.EnvFromSource{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test"},
					},
				},
			)
			localCluster.Spec.PodTemplate.Spec.Containers[0].Ports = append(
				localCluster.Spec.PodTemplate.Spec.Containers[0].Ports,
				corev1.ContainerPort{Name: "metrics", ContainerPort: 1111},
			)
			localCluster.Spec.PodTemplate.Spec.Containers = append(
				localCluster.Spec.PodTemplate.Spec.Containers,
				corev1.Container{
					Name:  "exporter",
					Image: "etcd-exporter",
				},
			)

			containers := generateContainers(localCluster)
			if Expect(containers).To(HaveLen(2)) {
				Expect(containers[0].EnvFrom).To(ContainElement(corev1.EnvFromSource{
					ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "test"},
					},
				}))
				Expect(containers[0].Ports).To(ContainElement(corev1.ContainerPort{Name: "metrics", ContainerPort: 1111}))
				if Expect(containers[0].VolumeMounts).To(HaveLen(2)) {
					Expect(containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{
						Name:      "data",
						MountPath: "/var/run/etcd",
					}))
					Expect(containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{
						Name:      "test",
						MountPath: "/test/tmp.txt",
					}))
				}
			}
		})
		It("should override user provided field on collision", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodTemplate.Spec.Containers[0].VolumeMounts = append(
				localCluster.Spec.PodTemplate.Spec.Containers[0].VolumeMounts,
				corev1.VolumeMount{
					Name:      "data",
					MountPath: "/test/tmp.txt",
				},
			)
			localCluster.Spec.PodTemplate.Spec.Containers[0].Ports = append(
				localCluster.Spec.PodTemplate.Spec.Containers[0].Ports,
				corev1.ContainerPort{Name: "client", ContainerPort: 1111},
			)

			containers := generateContainers(localCluster)
			if Expect(containers).To(HaveLen(1)) {
				Expect(containers[0].Ports).NotTo(ContainElement(corev1.ContainerPort{Name: "client", ContainerPort: 1111}))
				if Expect(containers[0].VolumeMounts).To(HaveLen(1)) {
					Expect(containers[0].VolumeMounts).NotTo(ContainElement(corev1.VolumeMount{
						Name:      "data",
						MountPath: "/tmp",
					}))
				}
			}
		})
		It("should generate security volumes mounts", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security = &etcdaenixiov1alpha1.SecuritySpec{
				TLS: etcdaenixiov1alpha1.TLSSpec{
					PeerTrustedCASecret:   "peer-ca-secret",
					PeerSecret:            "peer-cert-secret",
					ServerSecret:          "server-cert-secret",
					ClientTrustedCASecret: "client-ca-secret",
					ClientSecret:          "client-secret",
				},
			}

			containers := generateContainers(localCluster)

			Expect(containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{
				Name:      "peer-trusted-ca-certificate",
				MountPath: "/etc/etcd/pki/peer/ca",
				ReadOnly:  true,
			}))
			Expect(containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{
				Name:      "peer-certificate",
				MountPath: "/etc/etcd/pki/peer/cert",
				ReadOnly:  true,
			}))
			Expect(containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{
				Name:      "server-certificate",
				MountPath: "/etc/etcd/pki/server/cert",
				ReadOnly:  true,
			}))
			Expect(containers[0].VolumeMounts).To(ContainElement(corev1.VolumeMount{
				Name:      "client-trusted-ca-certificate",
				MountPath: "/etc/etcd/pki/client/ca",
				ReadOnly:  true,
			}))
		})

	})
	*/
})
