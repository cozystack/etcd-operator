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
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	. "sigs.k8s.io/controller-runtime/pkg/envtest/komega"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

var _ = Describe("Create services handlers", func() {
var _ = Describe("Create services handlers", func() {
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

	Context("Service name functions", func() {
		var cluster *etcdaenixiov1alpha1.EtcdCluster

		BeforeEach(func() {
			cluster = &etcdaenixiov1alpha1.EtcdCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: ns.GetName(),
				},
			}
		})

		It("should return cluster name when no service template", func() {
			Expect(GetServiceName(cluster)).To(Equal("test-cluster"))
		})

		It("should return custom name from service template", func() {
			cluster.Spec.ServiceTemplate = &etcdaenixiov1alpha1.EmbeddedService{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Name: "custom-service",
				},
			}
			Expect(GetServiceName(cluster)).To(Equal("custom-service"))
		})

		It("should return cluster name when template has empty name", func() {
			cluster.Spec.ServiceTemplate = &etcdaenixiov1alpha1.EmbeddedService{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Name: "",
				},
			}
			Expect(GetServiceName(cluster)).To(Equal("test-cluster"))
		})

		It("should return cluster name with -headless suffix", func() {
			Expect(GetHeadlessServiceName(cluster)).To(Equal("test-cluster-headless"))
		})

		It("should return custom name from headless service template", func() {
			cluster.Spec.HeadlessServiceTemplate = &etcdaenixiov1alpha1.EmbeddedMetadataResource{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Name: "custom-headless",
				},
			}
			Expect(GetHeadlessServiceName(cluster)).To(Equal("custom-headless"))
		})
	})

	Context("Basic service creation", func() {
		var (
			etcdcluster     etcdaenixiov1alpha1.EtcdCluster
			headlessService corev1.Service
			clientService   corev1.Service
			err             error
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

		It("should successfully create headless service object", func() {
			headlessSvcObj, err := GetHeadlessService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(headlessSvcObj).ShouldNot(BeNil())
			Expect(headlessSvcObj).Should(SatisfyAll(
				HaveField("Spec.Type", Equal(corev1.ServiceTypeClusterIP)),
				HaveField("Spec.ClusterIP", Equal(corev1.ClusterIPNone)),
				HaveField("Spec.PublishNotReadyAddresses", BeTrue()),
			))
		})

		It("should successfully create client service object", func() {
			clientSvcObj, err := GetClientService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(clientSvcObj).ShouldNot(BeNil())
			Expect(clientSvcObj).Should(SatisfyAll(
				HaveField("Spec.Type", Equal(corev1.ServiceTypeClusterIP)),
				HaveField("Spec.ClusterIP", Not(Equal(corev1.ClusterIPNone))),
				HaveField("Spec.PublishNotReadyAddresses", BeFalse()),
			))
		})

		It("should have correct ports in headless service", func() {
			headlessSvcObj, err := GetHeadlessService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(headlessSvcObj.Spec.Ports).Should(HaveLen(2))
			Expect(headlessSvcObj.Spec.Ports).Should(ContainElements(
				SatisfyAll(
					HaveField("Name", Equal("peer")),
					HaveField("Port", Equal(int32(2380))),
					HaveField("TargetPort", Equal(intstr.FromInt32(2380))),
					HaveField("Protocol", Equal(corev1.ProtocolTCP)),
				),
				SatisfyAll(
					HaveField("Name", Equal("client")),
					HaveField("Port", Equal(int32(2379))),
					HaveField("TargetPort", Equal(intstr.FromInt32(2379))),
					HaveField("Protocol", Equal(corev1.ProtocolTCP)),
				),
			))
		})

		It("should have correct ports in client service", func() {
			clientSvcObj, err := GetClientService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(clientSvcObj.Spec.Ports).Should(HaveLen(1))
			Expect(clientSvcObj.Spec.Ports[0]).Should(SatisfyAll(
				HaveField("Name", Equal("client")),
				HaveField("Port", Equal(int32(2379))),
				HaveField("TargetPort", Equal(intstr.FromInt32(2379))),
				HaveField("Protocol", Equal(corev1.ProtocolTCP)),
			))
		})

		It("should have correct owner references", func() {
			headlessSvcObj, err := GetHeadlessService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			clientSvcObj, err := GetClientService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(headlessSvcObj.OwnerReferences).Should(HaveLen(1))
			Expect(clientSvcObj.OwnerReferences).Should(HaveLen(1))

			headlessOwner := headlessSvcObj.OwnerReferences[0]
			clientOwner := clientSvcObj.OwnerReferences[0]

			Expect(headlessOwner.Name).To(Equal(etcdcluster.Name))
			Expect(clientOwner.Name).To(Equal(etcdcluster.Name))
			Expect(headlessOwner.UID).To(Equal(etcdcluster.UID))
			Expect(clientOwner.UID).To(Equal(etcdcluster.UID))
			Expect(headlessOwner.Controller).NotTo(BeNil())
			Expect(*headlessOwner.Controller).To(BeTrue())
			Expect(clientOwner.Controller).NotTo(BeNil())
			Expect(*clientOwner.Controller).To(BeTrue())
		})

		It("should have same selectors for headless and client services", func() {
			headlessSvc, err := GetHeadlessService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			clientSvc, err := GetClientService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(headlessSvc.Spec.Selector).To(Equal(clientSvc.Spec.Selector))
		})
	})

	Context("Service creation with templates", func() {
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
					Replicas: ptr.To(int32(3)),
				},
			}
			Expect(k8sClient.Create(ctx, &etcdcluster)).Should(Succeed())
			Eventually(Get(&etcdcluster)).Should(Succeed())
			DeferCleanup(k8sClient.Delete, &etcdcluster)
		})

		It("should merge client service template metadata and spec correctly", func() {
			etcdcluster.Spec.ServiceTemplate = &etcdaenixiov1alpha1.EmbeddedService{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Name: "custom-client",
					Labels: map[string]string{
						"custom-label": "value",
					},
					Annotations: map[string]string{
						"custom-annotation": "value",
					},
				},
				Spec: corev1.ServiceSpec{
					Type: corev1.ServiceTypeLoadBalancer,
					Ports: []corev1.ServicePort{
						{
							Name:       "etcd-client",
							Port:       2379,
							TargetPort: intstr.FromInt32(2379),
						},
						{
							Name:       "metrics",
							Port:       8080,
							TargetPort: intstr.FromInt32(8080),
						},
					},
				},
			}
			Expect(k8sClient.Update(ctx, &etcdcluster)).Should(Succeed())

			svc, err := GetClientService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Expect(svc.Name).To(Equal("custom-client"))
			Expect(svc.Labels).To(HaveKeyWithValue("custom-label", "value"))
			Expect(svc.Annotations).To(HaveKeyWithValue("custom-annotation", "value"))
			Expect(svc.Spec.Type).To(Equal(corev1.ServiceTypeLoadBalancer))
			Expect(svc.Spec.Ports).To(HaveLen(2))

			var portNames []string
			for _, port := range svc.Spec.Ports {
				portNames = append(portNames, port.Name)
			}
			Expect(portNames).To(ContainElements("etcd-client", "metrics"))
			Expect(portNames).NotTo(ContainElement("client"))
		})

		It("should merge partial client service template correctly", func() {
			etcdcluster.Spec.ServiceTemplate = &etcdaenixiov1alpha1.EmbeddedService{
				Spec: corev1.ServiceSpec{
					SessionAffinity: corev1.ServiceAffinityClientIP,
					Ports: []corev1.ServicePort{
						{
							Name:     "customport",
							Port:     1234,
							Protocol: corev1.ProtocolUDP,
						},
					},
				},
			}
			Expect(k8sClient.Update(ctx, &etcdcluster)).Should(Succeed())

			svc, err := GetClientService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())

			Eventually(Object(svc)).Should(SatisfyAll(
				HaveField("Spec.Type", Equal(corev1.ServiceTypeLoadBalancer)),
				HaveField("Spec.LoadBalancerClass", Equal(ptr.To("someClass"))),
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

		It("should use cluster name when template has empty name", func() {
			etcdcluster.Spec.ServiceTemplate = &etcdaenixiov1alpha1.EmbeddedService{
				EmbeddedObjectMetadata: etcdaenixiov1alpha1.EmbeddedObjectMetadata{
					Name: "",
				},
			}
			Expect(k8sClient.Update(ctx, &etcdcluster)).Should(Succeed())

			svc, err := GetClientService(ctx, &etcdcluster, k8sClient)
			Expect(err).ShouldNot(HaveOccurred())
			Expect(svc.Name).To(Equal(etcdcluster.Name))
		})
	})
})
