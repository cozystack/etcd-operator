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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

var _ = Describe("CreateOrUpdateClusterStateConfigMap handlers", func() {
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

	Context("when ensuring a configMap", func() {
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
					Replicas: new(int32(3)),
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			configMap = corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      GetClusterStateConfigMapName(&etcdcluster),
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

		It("should successfully ensure the configmap", func() {
			var configMapUID types.UID
			By("processing new etcd cluster", func() {
				Expect(CreateOrUpdateClusterStateConfigMap(ctx, &etcdcluster, k8sClient)).To(Succeed())
				Eventually(Get(&configMap)).Should(Succeed())
				Expect(configMap.Data["ETCD_INITIAL_CLUSTER_STATE"]).To(Equal("new"))
				configMapUID = configMap.GetUID()
			})

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
			Expect(CreateOrUpdateClusterStateConfigMap(ctx, &etcdcluster, clientWithEmptyScheme)).NotTo(Succeed())
		})
	})
})
