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
	"context"

	"k8s.io/apimachinery/pkg/api/resource"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	storagev1 "k8s.io/api/storage/v1"
)

var _ = Describe("UpdatePersistentVolumeClaims", func() {
	var (
		ns         *corev1.Namespace
		ctx        context.Context
		cluster    *etcdaenixiov1alpha1.EtcdCluster
		fakeClient client.Client
	)

	BeforeEach(func() {
		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-namespace",
			},
		}
		ctx = context.TODO()
		cluster = &etcdaenixiov1alpha1.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-cluster",
				Namespace: ns.Name,
			},
			Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
				Storage: etcdaenixiov1alpha1.StorageSpec{
					VolumeClaimTemplate: etcdaenixiov1alpha1.EmbeddedPersistentVolumeClaim{
						Spec: corev1.PersistentVolumeClaimSpec{
							Resources: corev1.VolumeResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceStorage: resource.MustParse("10Gi"),
								},
							},
						},
					},
				},
			},
		}

		// Setting up the fake client
		fakeClient = fake.NewClientBuilder().WithObjects(ns, cluster).Build()
	})

	Context("when updating PVC sizes", func() {
		It("should handle no PVCs found correctly", func() {
			Expect(UpdatePersistentVolumeClaims(ctx, cluster, fakeClient)).Should(Succeed())
		})

		It("should update PVC if the desired size is larger", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "data-test-cluster-0",
					Namespace: ns.Name,
					Labels: map[string]string{
						"app.kubernetes.io/instance":   cluster.Name,
						"app.kubernetes.io/managed-by": etcdOperatorName,
						"app.kubernetes.io/name":       "etcd",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: stringPointer("test-storage-class"),
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pvc)).Should(Succeed())

			sc := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-storage-class",
				},
				AllowVolumeExpansion: boolPointer(true),
			}
			Expect(fakeClient.Create(ctx, sc)).Should(Succeed())

			Expect(UpdatePersistentVolumeClaims(ctx, cluster, fakeClient)).Should(Succeed())

			updatedPVC := &corev1.PersistentVolumeClaim{}
			Expect(fakeClient.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: ns.Name}, updatedPVC)).Should(Succeed())
			Expect(updatedPVC.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("10Gi")))
		})

		It("should skip updating PVC if StorageClass does not allow expansion", func() {
			pvc := &corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "data-test-cluster-0",
					Namespace: ns.Name,
					Labels: map[string]string{
						"app.kubernetes.io/instance":   cluster.Name,
						"app.kubernetes.io/managed-by": etcdOperatorName,
						"app.kubernetes.io/name":       "etcd",
					},
				},
				Spec: corev1.PersistentVolumeClaimSpec{
					StorageClassName: stringPointer("non-expandable-storage-class"),
					Resources: corev1.VolumeResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceStorage: resource.MustParse("5Gi"),
						},
					},
				},
			}
			Expect(fakeClient.Create(ctx, pvc)).Should(Succeed())

			sc := &storagev1.StorageClass{
				ObjectMeta: metav1.ObjectMeta{
					Name: "non-expandable-storage-class",
				},
				AllowVolumeExpansion: boolPointer(false),
			}
			Expect(fakeClient.Create(ctx, sc)).Should(Succeed())

			Expect(UpdatePersistentVolumeClaims(ctx, cluster, fakeClient)).Should(Succeed())
			unchangedPVC := &corev1.PersistentVolumeClaim{}
			Expect(fakeClient.Get(ctx, types.NamespacedName{Name: pvc.Name, Namespace: ns.Name}, unchangedPVC)).Should(Succeed())
			Expect(unchangedPVC.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("5Gi")))
		})
	})
})

func stringPointer(s string) *string {
	return &s
}

func boolPointer(b bool) *bool {
	return &b
}
