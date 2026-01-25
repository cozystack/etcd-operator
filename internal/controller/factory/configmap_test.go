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
	"time"

	"github.com/google/uuid"
	"k8s.io/utils/ptr"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

var _ = Describe("ClusterStateConfigMap factory", func() {
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
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)
		})

		It("should successfully create the configmap object", func() {
			By("processing new etcd cluster", func() {
				configMapObj, err := GetClusterStateConfigMap(ctx, &etcdcluster, k8sClient)
				Expect(err).ShouldNot(HaveOccurred())
				Expect(configMapObj).NotTo(BeNil())
				Expect(configMapObj.Data["ETCD_INITIAL_CLUSTER_STATE"]).To(Equal("new"))
			})
		})
	})

	Context("when ensuring a configMap for existing cluster", func() {
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
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			etcdcluster.Status = etcdaenixiov1alpha1.EtcdClusterStatus{
				Conditions: []metav1.Condition{
					{
						Type:   etcdaenixiov1alpha1.EtcdConditionReady,
						Reason: string(etcdaenixiov1alpha1.EtcdCondTypeStatefulSetReady),
						Status: metav1.ConditionTrue,
						LastTransitionTime: metav1.Time{
							Time: time.Now(),
						},
					},
				},
			}
			Expect(k8sClient.Status().Update(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)
		})

		It("should generate configmap with state 'existing' for ready cluster", func() {
			configMapObj, err := GetClusterStateConfigMap(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(configMapObj).NotTo(BeNil())
			Expect(configMapObj.Data["ETCD_INITIAL_CLUSTER_STATE"]).To(Equal("existing"))
		})
	})
})
