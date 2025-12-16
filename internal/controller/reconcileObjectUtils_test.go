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
	"slices"

	"github.com/google/uuid"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
	appsv1 "k8s.io/api/apps/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var _ = Describe("CreateOrUpdateConditionalClusterObjects handlers", func() {
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

	Context("when ensuring a configMap", ensureConfigMapFunc(ns))

	Context(
		"should successfully create pod disruption budget for etcd cluster", 
		ensurePdbFunc(ns))

	Context("when ensuring statefulSet", ensureStatefulSet(ns))

	Context("when ensuring cluster services", ensureService(ns))
})

func ensureConfigMapFunc(ns *corev1.Namespace) func() {
	return func(){
		var (
			etcdcluster etcdaenixiov1alpha1.EtcdCluster
			configMap   corev1.ConfigMap

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
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			configMap = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      factory.GetClusterStateConfigMapName(&etcdcluster),
				},
			}
		})

		AfterEach(func() {
			err = Get(&configMap)()
			if err == nil {
				Expect(k8sClient.Delete(ctx, &configMap)).Should(Succeed())
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
		})

		It("should successfully create the configmap", func() {
			By("processing new etcd cluster", func() {
				Expect(CreateOrUpdateClusterStateConfigMap(ctx, &etcdcluster, k8sClient)).To(Succeed())
				Eventually(Get(&configMap)).Should(Succeed())
				Expect(configMap.Data["ETCD_INITIAL_CLUSTER_STATE"]).To(Equal("new"))
			})
		})

		It("should successfully ensure the configmap", func() {
			var configMapUID types.UID
			By("processing new etcd cluster", func() {
				Expect(CreateOrUpdateClusterStateConfigMap(ctx, &etcdcluster, k8sClient)).To(Succeed())
				Eventually(Get(&configMap)).Should(Succeed())
				Expect(configMap.Data["ETCD_INITIAL_CLUSTER_STATE"]).To(Equal("new"))
				configMapUID = configMap.GetUID()
			})

			// test move to reconcileObjectUtilsTest
			By("processing ready etcd cluster", func() {
				meta.SetStatusCondition(
					&etcdcluster.Status.Conditions,
					metav1.Condition{
						Type:   etcdaenixiov1alpha1.EtcdConditionReady,
						Status: metav1.ConditionTrue,
						Reason: string(etcdaenixiov1alpha1.EtcdCondTypeStatefulSetReady),
					},
				)
				Expect(CreateOrUpdateClusterStateConfigMap(ctx, &etcdcluster, k8sClient)).To(Succeed())
				Eventually(Object(&configMap)).Should(HaveField("ObjectMeta.UID", Equal(configMapUID)))
				Expect(configMap.Data["ETCD_INITIAL_CLUSTER_STATE"]).To(Equal("existing"))
			})

			By("updating the configmap for updated cluster", func() {
				meta.SetStatusCondition(
					&etcdcluster.Status.Conditions,
					metav1.Condition{
						Type:   etcdaenixiov1alpha1.EtcdConditionReady,
						Status: metav1.ConditionTrue,
						Reason: string(etcdaenixiov1alpha1.EtcdCondTypeWaitingForFirstQuorum),
					},
				)
				Expect(CreateOrUpdateClusterStateConfigMap(ctx, &etcdcluster, k8sClient)).To(Succeed())
				Eventually(Object(&configMap)).Should(HaveField("ObjectMeta.UID", Equal(configMapUID)))
				Expect(configMap.Data["ETCD_INITIAL_CLUSTER_STATE"]).To(Equal("new"))
			})
		})

		It("should fail to create the configmap with invalid owner reference", func() {
			Expect(CreateOrUpdateClusterStateConfigMap(ctx, &etcdcluster, k8sClient)).NotTo(Succeed())
		})
	}
}

func ensurePdbFunc(ns *corev1.Namespace) func() {
	return func(){
		var (
			etcdcluster         etcdaenixiov1alpha1.EtcdCluster
			podDisruptionBudget policyv1.PodDisruptionBudget

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
					Replicas:                    ptr.To(int32(3)),
					PodDisruptionBudgetTemplate: &etcdaenixiov1alpha1.EmbeddedPodDisruptionBudget{},
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			podDisruptionBudget = policyv1.PodDisruptionBudget{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      etcdcluster.GetName(),
				},
			}
		})

		AfterEach(func() {
			err = Get(&podDisruptionBudget)()
			if err == nil {
				Expect(k8sClient.Delete(ctx, &podDisruptionBudget)).Should(Succeed())
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
		})

		It("should create PDB with pre-filled data", func() {
			etcdcluster.Spec.PodDisruptionBudgetTemplate.Spec.MinAvailable = ptr.To(intstr.FromInt32(int32(3)))
			Expect(CreateOrUpdatePdb(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Get(&podDisruptionBudget)).Should(Succeed())
			Expect(podDisruptionBudget.Spec.MinAvailable).NotTo(BeNil())
			Expect(podDisruptionBudget.Spec.MinAvailable.IntValue()).To(Equal(3))
			Expect(podDisruptionBudget.Spec.MaxUnavailable).To(BeNil())
		})

		It("should create PDB with empty data", func() {
			Expect(CreateOrUpdatePdb(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Get(&podDisruptionBudget)).Should(Succeed())
			Expect(etcdcluster.Spec.PodDisruptionBudgetTemplate.Spec.MinAvailable).To(BeNil())
			Expect(podDisruptionBudget.Spec.MinAvailable.IntValue()).To(Equal(2))
			Expect(podDisruptionBudget.Spec.MaxUnavailable).To(BeNil())
		})

		It("should skip deletion of PDB if not filled and not exist", func() {
			etcdcluster.Spec.PodDisruptionBudgetTemplate = nil
			Expect(CreateOrUpdatePdb(ctx, &etcdcluster, k8sClient)).NotTo(HaveOccurred())
		})

		It("should delete created PDB after updating CR", func() {
			Expect(CreateOrUpdatePdb(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Get(&podDisruptionBudget)).Should(Succeed())
			etcdcluster.Spec.PodDisruptionBudgetTemplate = nil
			Expect(CreateOrUpdatePdb(ctx, &etcdcluster, k8sClient)).NotTo(HaveOccurred())
			err = Get(&podDisruptionBudget)()
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	}
}

func ensureStatefulSet(ns *corev1.Namespace) func() {
	return func (){
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
				Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient)).To(Succeed())
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
				Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient)).To(Succeed())
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
					Expect(statefulSet.Spec.Template.Spec.Containers[0].Command).
					To(Equal(factory.GenerateEtcdCommand()))
				})

				By("Checking the extraArgs", func() {
					Expect(statefulSet.Spec.Template.Spec.Containers[0].Args).To(Equal(factory.GenerateEtcdArgs(&etcdcluster)))
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
				Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient)).To(Succeed())
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
				Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, k8sClient)).To(Succeed())
				Eventually(Get(&statefulSet)).Should(Succeed())

				By("Checking the emptyDir", func() {
					Expect(statefulSet.Spec.Template.Spec.Volumes[0].VolumeSource.EmptyDir.SizeLimit.String()).To(Equal(size.String()))
				})
			})

			It("should fail on creating the statefulset with invalid owner reference", func() {
				Expect(CreateOrUpdateStatefulSet(ctx, &etcdcluster, clientWithEmptyScheme)).NotTo(Succeed())
		})
	}
}

func ensureService(ns *corev1.Namespace) func(){
	return func() {
		var (
			etcdcluster     etcdaenixiov1alpha1.EtcdCluster
			headlessService corev1.Service
			clientService   corev1.Service

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
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			headlessService = corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      factory.GetHeadlessServiceName(&etcdcluster),
				},
			}
			clientService = corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      factory.GetServiceName(&etcdcluster),
				},
			}
		})

		AfterEach(func() {
			err = Get(&headlessService)()
			if err == nil {
				Expect(k8sClient.Delete(ctx, &headlessService)).Should(Succeed())
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
			err = Get(&clientService)()
			if err == nil {
				Expect(k8sClient.Delete(ctx, &clientService)).Should(Succeed())
			} else {
				Expect(apierrors.IsNotFound(err)).To(BeTrue())
			}
		})

		It("should successfully ensure headless service", func() {
			Expect(CreateOrUpdateHeadlessService(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Object(&headlessService)).Should(SatisfyAll(
				HaveField("Spec.Type", Equal(corev1.ServiceTypeClusterIP)),
				HaveField("Spec.ClusterIP", Equal(corev1.ClusterIPNone)),
			))
		})

		It("should successfully ensure headless service with custom metadata", func() {
			etcdcluster.Spec.HeadlessServiceTemplate = &etcdaenixiov1alpha1.EmbeddedMetadataResource{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Name:        "headless-name",
					Labels:      map[string]string{"label": "value"},
					Annotations: map[string]string{"annotation": "value"},
				},
			}
			svc := headlessService.DeepCopy()
			svc.Name = etcdcluster.Spec.HeadlessServiceTemplate.Name

			Expect(CreateOrUpdateHeadlessService(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Object(svc)).Should(SatisfyAll(
				HaveField("ObjectMeta.Name", Equal(etcdcluster.Spec.HeadlessServiceTemplate.Name)),
				HaveField("ObjectMeta.Labels", SatisfyAll(
					HaveKeyWithValue("label", "value"),
					HaveKeyWithValue("app.kubernetes.io/name", "etcd"),
				)),
				HaveField("ObjectMeta.Annotations", Equal(etcdcluster.Spec.HeadlessServiceTemplate.Annotations)),
			))
			// We need to manually cleanup here because we changed the name of the service
			Expect(k8sClient.Delete(ctx, svc)).Should(Succeed())
		})

		It("should successfully ensure client service", func() {
			Expect(CreateOrUpdateClientService(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Object(&clientService)).Should(SatisfyAll(
				HaveField("Spec.Type", Equal(corev1.ServiceTypeClusterIP)),
				HaveField("Spec.ClusterIP", Not(Equal(corev1.ClusterIPNone))),
			))
		})

		It("should successfully ensure client service with custom metadata", func() {
			etcdcluster.Spec.ServiceTemplate = &etcdaenixiov1alpha1.EmbeddedService{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Name:        "client-name",
					Labels:      map[string]string{"label": "value"},
					Annotations: map[string]string{"annotation": "value"},
				},
			}
			svc := clientService.DeepCopy()
			svc.Name = etcdcluster.Spec.ServiceTemplate.Name

			Expect(CreateOrUpdateClientService(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Object(svc)).Should(SatisfyAll(
				HaveField("ObjectMeta.Name", Equal(etcdcluster.Spec.ServiceTemplate.Name)),
				HaveField("ObjectMeta.Labels", SatisfyAll(
					HaveKeyWithValue("label", "value"),
					HaveKeyWithValue("app.kubernetes.io/name", "etcd"),
				)),
				HaveField("ObjectMeta.Annotations", Equal(etcdcluster.Spec.ServiceTemplate.Annotations)),
			))
			// We need to manually cleanup here because we changed the name of the service
			Expect(k8sClient.Delete(ctx, svc)).Should(Succeed())
		})

		It("should successfully ensure client service with custom spec", func() {
			etcdcluster.Spec.ServiceTemplate = &etcdaenixiov1alpha1.EmbeddedService{
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
					Ports: []corev1.ServicePort{
						{
							Name:     "client",
							Port:     2379,
							Protocol: corev1.ProtocolUDP,
						},
					},
					LoadBalancerClass: ptr.To("someClass"),
				},
			}

			Expect(CreateOrUpdateClientService(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Object(&clientService)).Should(SatisfyAll(
				HaveField("Spec.Type", Equal(corev1.ServiceTypeLoadBalancer)),
				HaveField("Spec.LoadBalancerClass", Equal(ptr.To("someClass"))),
				HaveField("Spec.Ports", SatisfyAll(
					HaveLen(1),
					HaveEach(SatisfyAll(
						HaveField("Name", Equal("client")),
						HaveField("Port", Equal(int32(2379))),
						HaveField("Protocol", Equal(corev1.ProtocolUDP)),
					)),
				)),
			))
		})

		It("should fail on creating the client service with invalid owner reference", func() {
			Expect(CreateOrUpdateHeadlessService(ctx, &etcdcluster, clientWithEmptyScheme)).NotTo(Succeed())
			Expect(CreateOrUpdateClientService(ctx, &etcdcluster, clientWithEmptyScheme)).NotTo(Succeed())
		})
	}
}