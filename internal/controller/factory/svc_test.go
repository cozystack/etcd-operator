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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

var _ = Describe("CreateOrUpdateService handlers", func() {
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

	Context("when ensuring cluster services", func() {
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
					Replicas: new(int32(3)),
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)

			headlessService = corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      GetHeadlessServiceName(&etcdcluster),
				},
			}
			clientService = corev1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns.GetName(),
					Name:      GetServiceName(&etcdcluster),
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
							Name:     "customport",
							Port:     1234,
							Protocol: corev1.ProtocolUDP,
						},
					},
					LoadBalancerClass: new("someClass"),
				},
			}

			Expect(CreateOrUpdateClientService(ctx, &etcdcluster, k8sClient)).To(Succeed())
			Eventually(Object(&clientService)).Should(SatisfyAll(
				HaveField("Spec.Type", Equal(corev1.ServiceTypeLoadBalancer)),
				HaveField("Spec.LoadBalancerClass", Equal(new("someClass"))),
				HaveField("Spec.Ports", SatisfyAll(
					ContainElements(SatisfyAll(
						HaveField("Name", Equal("client")),
						HaveField("Port", Equal(int32(2379))),
						HaveField("Protocol", Equal(corev1.ProtocolTCP)),
					)),
					ContainElements(SatisfyAll(
						HaveField("Name", Equal("metrics")),
						HaveField("Port", Equal(int32(2381))),
						HaveField("Protocol", Equal(corev1.ProtocolTCP)),
					)),
					ContainElements(SatisfyAll(
						HaveField("Name", Equal("customport")),
						HaveField("Port", Equal(int32(1234))),
						HaveField("Protocol", Equal(corev1.ProtocolUDP)),
					)),
				)),
			))
		})

		It("should fail on creating the client service with invalid owner reference", func() {
			Expect(CreateOrUpdateHeadlessService(ctx, &etcdcluster, clientWithEmptyScheme)).NotTo(Succeed())
			Expect(CreateOrUpdateClientService(ctx, &etcdcluster, clientWithEmptyScheme)).NotTo(Succeed())
		})
	})
})
