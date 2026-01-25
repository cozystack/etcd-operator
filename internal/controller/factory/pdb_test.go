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
	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
)

var _ = Describe("Pdb factory", func() {
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

	Context("should successfully create pod disruption budget for etcd cluster", func() {
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
			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj).ShouldNot(BeNil())
		})

		It("should create PDB with empty data", func() {
			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj).ShouldNot(BeNil())
		})
	})

	Context("when calculating quorum-based MinAvailable", func() {
		var (
			etcdcluster etcdaenixiov1alpha1.EtcdCluster
		)

		BeforeEach(func() {
			etcdcluster = etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-etcdcluster-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					PodDisruptionBudgetTemplate: &etcdaenixiov1alpha1.EmbeddedPodDisruptionBudget{},
				},
			}
		})
		AfterEach(func() {
			err := Get(&etcdcluster)()
			if err == nil {
				Expect(k8sClient.Delete(ctx, &etcdcluster)).Should(Succeed())
			}
		})

		It("should calculate MinAvailable=1 for 1 replica", func() {
			etcdcluster.Spec.Replicas = ptr.To(int32(1))
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj.Spec.MinAvailable).To(Equal(ptr.To(intstr.FromInt32(1))))
		})

		It("should calculate MinAvailable=2 for 3 replicas", func() {
			etcdcluster.Spec.Replicas = ptr.To(int32(3))
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj.Spec.MinAvailable).To(Equal(ptr.To(intstr.FromInt32(2)))) // quorum of 3 is 2
		})

		It("should calculate MinAvailable=3 for 5 replicas", func() {
			etcdcluster.Spec.Replicas = ptr.To(int32(5))
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj.Spec.MinAvailable).To(Equal(ptr.To(intstr.FromInt32(3)))) // quorum of 5 is 3
		})

		It("should prioritize user-provided MinAvailable over quorum calculation", func() {
			etcdcluster.Spec.Replicas = ptr.To(int32(3))
			etcdcluster.Spec.PodDisruptionBudgetTemplate.Spec.MinAvailable = ptr.To(intstr.FromInt32(1))
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj.Spec.MinAvailable).To(Equal(ptr.To(intstr.FromInt32(1)))) // User value, not quorum
			Expect(pdbObj.Spec.MaxUnavailable).To(BeNil())
		})

		It("should use MaxUnavailable when provided", func() {
			etcdcluster.Spec.Replicas = ptr.To(int32(3))
			etcdcluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(1))
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj.Spec.MaxUnavailable).To(Equal(ptr.To(intstr.FromInt32(1))))
			Expect(pdbObj.Spec.MinAvailable).To(BeNil()) // Should be nil when MaxUnavailable is set
		})

		It("should have correct selector for etcd pods", func() {
			etcdcluster.Spec.Replicas = ptr.To(int32(3))
			etcdcluster.Name = "test-cluster"
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(pdbObj.Spec.Selector).NotTo(BeNil())
			Expect(pdbObj.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/name", "etcd"))
			Expect(pdbObj.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/instance", "test-cluster"))
			Expect(pdbObj.Spec.Selector.MatchLabels).To(HaveKeyWithValue("app.kubernetes.io/managed-by", "etcd-operator"))
		})

		It("should always set UnhealthyPodEvictionPolicy to IfHealthyBudget", func() {
			etcdcluster.Spec.Replicas = ptr.To(int32(3))
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj.Spec.UnhealthyPodEvictionPolicy).To(Equal(ptr.To(policyv1.IfHealthyBudget)))
		})
	})

	Context("when handling edge cases", func() {
		It("should handle nil PodDisruptionBudgetTemplate", func() {
			etcdcluster := etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-etcdcluster-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					// PodDisruptionBudgetTemplate is nil
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			_, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).Should(HaveOccurred())
		})

		It("should handle 0 replicas gracefully", func() {
			etcdcluster := etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: "test-etcdcluster-",
					Namespace:    ns.GetName(),
					UID:          types.UID(uuid.NewString()),
				},
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Replicas:                    ptr.To(int32(0)),
					PodDisruptionBudgetTemplate: &etcdaenixiov1alpha1.EmbeddedPodDisruptionBudget{},
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			pdbObj, err := GetPdb(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(pdbObj.Spec.MinAvailable).To(Equal(ptr.To(intstr.FromInt32(1))))
		})
	})
})
