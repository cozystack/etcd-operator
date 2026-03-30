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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
)

var _ = Describe("EtcdCluster Controller", func() {
	var (
		reconciler *EtcdClusterReconciler
		ns         *corev1.Namespace
	)

	BeforeEach(func() {
		reconciler = &EtcdClusterReconciler{
			Client: getK8sClient(),
			Scheme: getK8sClient().Scheme(),
		}

		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "test-",
			},
		}
		Expect(getK8sClient().Create(ctx, ns)).Should(Succeed())
		DeferCleanup(getK8sClient().Delete, ns)
	})

	Context("When running getEtcdEndpoints", func() {
		It("Should get etcd empty string slice if etcd has 0 replicas", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: new(int32(0)),
				},
			}
			Expect(getEndpointsSlice(cluster)).To(BeEmpty())
			Expect(getEndpointsSlice(cluster)).To(Equal([]string{}))

		})

		It("Should get etcd correct string slice if etcd has 3 replicas", func() {
			cluster := &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "etcd-test",
					Namespace: "ns-test",
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: new(int32(3)),
				},
			}
			Expect(getEndpointsSlice(cluster)).To(Equal([]string{
				"http://etcd-test-0.etcd-test-headless.ns-test.svc:2379",
				"http://etcd-test-1.etcd-test-headless.ns-test.svc:2379",
				"http://etcd-test-2.etcd-test-headless.ns-test.svc:2379",
			}))
		})

	})

	Context("When reconciling the EtcdCluster", func() {
		var (
			etcdcluster     etcdaenixiov1alpha1.EtcdCluster
			configMap       corev1.ConfigMap
			headlessService corev1.Service
			service         corev1.Service
			statefulSet     appsv1.StatefulSet

			err error
		)

		BeforeEach(func() {
			etcdcluster = etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-etcdcluster-",
					Namespace:    ns.GetName(),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: new(int32(3)),
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			}
			Expect(getK8sClient().Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(getK8sClient().Delete, &etcdcluster)

			configMap = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      factory.GetClusterStateConfigMapName(&etcdcluster),
				},
			}
			DeferCleanup(getK8sClient().Delete, &configMap)
			headlessService = corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      factory.GetHeadlessServiceName(&etcdcluster),
				},
			}
			DeferCleanup(getK8sClient().Delete, &headlessService)
			service = corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      factory.GetServiceName(&etcdcluster),
				},
			}
			DeferCleanup(getK8sClient().Delete, &service)
			statefulSet = appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      etcdcluster.GetName(),
				},
			}
			DeferCleanup(getK8sClient().Delete, &statefulSet)
		})

		It("should reconcile a new EtcdCluster", func() {
			By("reconciling the EtcdCluster", func() {
				_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&etcdcluster)})
				Expect(err).ToNot(HaveOccurred())
				Eventually(Get(&etcdcluster)).Should(Succeed())
				Expect(etcdcluster.Status.Conditions).To(HaveLen(2))
				Expect(etcdcluster.Status.Conditions[0].Type).To(Equal(etcdaenixiov1alpha1.EtcdConditionInitialized))
				Expect(etcdcluster.Status.Conditions[0].Status).To(Equal(metav1.ConditionStatus("True")))
				Expect(etcdcluster.Status.Conditions[1].Type).To(Equal(etcdaenixiov1alpha1.EtcdConditionReady))
				Expect(etcdcluster.Status.Conditions[1].Status).To(Equal(metav1.ConditionStatus("False")))
			})

			By("reconciling owned ConfigMap", func() {
				Eventually(Get(&configMap)).Should(Succeed())
				Expect(configMap.Data).Should(HaveKeyWithValue("ETCD_INITIAL_CLUSTER_STATE", "new"))
			})

			By("reconciling owned headless Service", func() {
				Eventually(Get(&headlessService)).Should(Succeed())
				Expect(headlessService.Spec.ClusterIP).Should(Equal("None"))
			})

			By("reconciling owned Service", func() {
				Eventually(Get(&service)).Should(Succeed())
				Expect(service.Spec.ClusterIP).ShouldNot(Equal("None"))
			})

			By("reconciling owned StatefulSet", func() {
				Eventually(Get(&statefulSet)).Should(Succeed())
			})
		})

		It("should successfully reconcile the resource twice and mark as ready", func() {
			Skip("Skipped because it is checked in e2e tests. Waiting for wrapper interface implementation from @ArtemBortnikov")
			By("reconciling the EtcdCluster", func() {
				_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&etcdcluster)})
				Expect(err).ToNot(HaveOccurred())
			})

			By("setting owned StatefulSet to ready state", func() {
				Eventually(Get(&statefulSet)).Should(Succeed())
				Eventually(UpdateStatus(&statefulSet, func() {
					statefulSet.Status.ReadyReplicas = *etcdcluster.Spec.Replicas
					statefulSet.Status.Replicas = *etcdcluster.Spec.Replicas
				})).Should(Succeed())
			})

			By("reconciling the EtcdCluster after owned StatefulSet is ready", func() {
				_, err = reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&etcdcluster)})
				Expect(err).ToNot(HaveOccurred())
				Eventually(Get(&etcdcluster)).Should(Succeed())
				Expect(etcdcluster.Status.Conditions[1].Type).To(Equal(etcdaenixiov1alpha1.EtcdConditionReady))
				Expect(string(etcdcluster.Status.Conditions[1].Status)).To(Equal("True"))
			})
		})
	})
})
