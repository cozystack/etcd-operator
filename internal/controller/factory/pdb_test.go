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
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"
)

var _ = Describe("CreateOrUpdatePdb handlers", func() {
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
					Replicas:                    new(int32(3)),
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
			etcdcluster.Spec.PodDisruptionBudgetTemplate.Spec.MinAvailable = new(intstr.FromInt32(int32(3)))
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
	})
})
