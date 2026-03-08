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
	"slices"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

var _ = Describe("CreateOrUpdateStatefulSet handler", func() {
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

	Context("when ensuring statefulSet", func() {
		var (
			etcdcluster etcdaenixiov1alpha1.EtcdCluster
			statefulSet appsv1.StatefulSet

			err error
		)

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
						"foo":  "bar",
						"key1": "value1",
						"key2": "value2",
					},
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			statefulSet = appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Name:      etcdcluster.GetName(),
					Namespace: ns.GetName(),
				},
			}
		})

		AfterEach(func() {
			err = Get(&statefulSet)()
			if err == nil {
				Expect(k8sClient.Delete(ctx, &statefulSet)).Should(Succeed())
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
		})

		It("should successfully ensure the statefulSet with empty spec", func() {
			Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient, "")).To(Succeed())
			Eventually(Object(&statefulSet)).Should(
				HaveField("Spec.Replicas", Equal(etcdcluster.Spec.Replicas)),
			)
		})

		It("should successfully ensure the statefulSet with filled spec", func() {
			etcdcluster.Spec.Storage = etcdaenixiov1alpha1.StorageSpec{
				VolumeClaimTemplate: etcdaenixiov1alpha1.EmbeddedPersistentVolumeClaim{
					EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
						Name: "etcd-data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse("1Gi"),
							},
						},
					},
					Status: corev1.PersistentVolumeClaimStatus{},
				},
			}
			etcdcluster.Spec.PodTemplate = etcdaenixiov1alpha1.PodTemplate{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Labels: map[string]string{
						"app": "etcd",
					},
					Annotations: map[string]string{
						"app": "etcd",
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: "etcd-operator",
					ReadinessGates: []corev1.PodReadinessGate{
						{
							// Some custom readiness gate
							ConditionType: "target-health.elbv2.k8s.aws",
						},
					},
					Containers: []corev1.Container{
						{
							Name: "etcd",
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("100m"),
									corev1.ResourceMemory: resource.MustParse("128Mi"),
								},
							},
						},
					},
				},
			}
			etcdcluster.Spec.Security = &etcdaenixiov1alpha1.SecuritySpec{
				TLS: etcdaenixiov1alpha1.TLSSpec{
					PeerTrustedCASecret:   "peer-ca-secret",
					PeerSecret:            "peer-cert-secret",
					ServerSecret:          "server-cert-secret",
					ClientTrustedCASecret: "client-ca-secret",
					ClientSecret:          "client-secret",
				},
			}
			Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient, "")).To(Succeed())
			Eventually(Get(&statefulSet)).Should(Succeed())

			By("Checking the resources", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].Resources.Requests.Cpu()).
					To(Equal(etcdcluster.Spec.PodTemplate.Spec.Containers[0].Resources.Requests.Cpu()))
				Expect(statefulSet.Spec.Template.Spec.Containers[0].Resources.Requests.Memory()).
					To(Equal(etcdcluster.Spec.PodTemplate.Spec.Containers[0].Resources.Requests.Memory()))
			})

			By("Checking the pod metadata", func() {
				Expect(statefulSet.Spec.Template.ObjectMeta.Labels).To(Equal(map[string]string{
					"app.kubernetes.io/name":       "etcd",
					"app.kubernetes.io/instance":   etcdcluster.Name,
					"app.kubernetes.io/managed-by": "etcd-operator",
					"app":                          "etcd",
				}))
				Expect(statefulSet.Spec.Template.ObjectMeta.Annotations).To(Equal(etcdcluster.Spec.PodTemplate.Annotations))
			})

			By("Checking the command", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].Command).To(Equal(generateEtcdCommand()))
			})

			By("Checking the extraArgs", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].Args).To(Equal(generateEtcdArgs(&etcdcluster)))
				By("Checking args are sorted", func() {
					// Check that only the extra args are sorted, which means we need to check elements starting from n,
					// where n is the length of the default args. So we subtract the length of the extra args from the all args.
					// For example: if we have 3 extra args and 10 total args, we need to check elements starting from 10-3 = 7,
					// because the first 7 elements are default args and the elements args[7], args[8], and args[9] are extra args.
					n := len(statefulSet.Spec.Template.Spec.Containers[0].Args) - len(etcdcluster.Spec.Options)
					argsClone := slices.Clone(statefulSet.Spec.Template.Spec.Containers[0].Args[n:])
					slices.Sort(argsClone)
					Expect(statefulSet.Spec.Template.Spec.Containers[0].Args[n:]).To(Equal(argsClone))
				})
			})

			By("Checking the readinessGates", func() {
				Expect(statefulSet.Spec.Template.Spec.ReadinessGates).To(Equal(etcdcluster.Spec.PodTemplate.Spec.ReadinessGates))
			})

			By("Checking the serviceAccountName", func() {
				Expect(statefulSet.Spec.Template.Spec.ServiceAccountName).To(Equal(etcdcluster.Spec.PodTemplate.Spec.ServiceAccountName))
			})

			By("Checking the default startup probe", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].StartupProbe).To(Equal(&corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/readyz?serializable=false",
							Port:   intstr.FromInt32(2381),
							Scheme: corev1.URISchemeHTTP,
						},
					},
					TimeoutSeconds:   1,
					PeriodSeconds:    5,
					SuccessThreshold: 1,
					FailureThreshold: 3,
				}))
			})

			By("Checking the default readiness probe", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].ReadinessProbe).To(Equal(&corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/readyz",
							Port:   intstr.FromInt32(2381),
							Scheme: corev1.URISchemeHTTP,
						},
					},
					TimeoutSeconds:   1,
					PeriodSeconds:    5,
					SuccessThreshold: 1,
					FailureThreshold: 3,
				}))
			})

			By("Checking the default liveness probe", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].LivenessProbe).To(Equal(&corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/livez",
							Port:   intstr.FromInt32(2381),
							Scheme: corev1.URISchemeHTTP,
						},
					},
					TimeoutSeconds:   1,
					PeriodSeconds:    5,
					SuccessThreshold: 1,
					FailureThreshold: 3,
				}))
			})

			By("Checking generated security volumes", func() {
				Expect(statefulSet.Spec.Template.Spec.Volumes).Should(SatisfyAll(
					ContainElements([]corev1.Volume{
						{
							Name: "peer-trusted-ca-certificate",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  "peer-ca-secret",
									DefaultMode: ptr.To(int32(420)),
								},
							},
						},
						{
							Name: "peer-certificate",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  "peer-cert-secret",
									DefaultMode: ptr.To(int32(420)),
								},
							},
						},
						{
							Name: "server-certificate",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  "server-cert-secret",
									DefaultMode: ptr.To(int32(420)),
								},
							},
						},
						{
							Name: "client-trusted-ca-certificate",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName:  "client-ca-secret",
									DefaultMode: ptr.To(int32(420)),
								},
							},
						}},
					),
				))
			})
		})

		It("should successfully override probes", func() {
			etcdcluster.Spec.PodTemplate.Spec = corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "etcd",
						LivenessProbe: &corev1.Probe{
							InitialDelaySeconds: 13,
							PeriodSeconds:       11,
						},
						ReadinessProbe: &corev1.Probe{
							PeriodSeconds: 3,
						},
						StartupProbe: &corev1.Probe{
							PeriodSeconds: 7,
							ProbeHandler: corev1.ProbeHandler{
								HTTPGet: &corev1.HTTPGetAction{
									Path: "/test",
									Port: intstr.FromInt32(2389),
								},
							},
						},
					},
				},
			}
			Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient, "")).To(Succeed())
			Eventually(Get(&statefulSet)).Should(Succeed())

			By("Checking the updated startup probe", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].StartupProbe).To(Equal(&corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/test",
							Port:   intstr.FromInt32(2389),
							Scheme: corev1.URISchemeHTTP,
						},
					},
					TimeoutSeconds:   1,
					PeriodSeconds:    7,
					SuccessThreshold: 1,
					FailureThreshold: 3,
				}))
			})

			By("Checking the updated readiness probe", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].ReadinessProbe).To(Equal(&corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/readyz",
							Port:   intstr.FromInt32(2381),
							Scheme: corev1.URISchemeHTTP,
						},
					},
					TimeoutSeconds:   1,
					PeriodSeconds:    3,
					SuccessThreshold: 1,
					FailureThreshold: 3,
				}))
			})

			By("Checking the updated liveness probe", func() {
				Expect(statefulSet.Spec.Template.Spec.Containers[0].LivenessProbe).To(Equal(&corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						HTTPGet: &corev1.HTTPGetAction{
							Path:   "/livez",
							Port:   intstr.FromInt32(2381),
							Scheme: corev1.URISchemeHTTP,
						},
					},
					InitialDelaySeconds: 13,
					TimeoutSeconds:      1,
					PeriodSeconds:       11,
					SuccessThreshold:    1,
					FailureThreshold:    3,
				}))
			})
		})

		It("should successfully create statefulSet with emptyDir", func() {
			size := resource.MustParse("1Gi")
			etcdcluster.Spec.Storage = etcdaenixiov1alpha1.StorageSpec{
				EmptyDir: &corev1.EmptyDirVolumeSource{
					SizeLimit: ptr.To(size),
				},
			}
			Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient, "")).To(Succeed())
			Eventually(Get(&statefulSet)).Should(Succeed())

			By("Checking the emptyDir", func() {
				Expect(statefulSet.Spec.Template.Spec.Volumes[0].VolumeSource.EmptyDir.SizeLimit.String()).To(Equal(size.String()))
			})
		})

		It("should fail on creating the statefulset with invalid owner reference", func() {
			Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, clientWithEmptyScheme, "")).NotTo(Succeed())
		})
	})

	Context("When generating a etcd command", func() {
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

			args := generateEtcdArgs(etcdcluster)

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
			args := generateEtcdArgs(etcdCluster)
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
			args := generateEtcdArgs(etcdCluster)
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
			args := generateEtcdArgs(etcdCluster)
			// 2Gi * 0.95 = 2040109465,6
			Expect(args).To(ContainElement("--quota-backend-bytes=2040109465"))
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
