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

package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

var _ = Describe("EtcdCluster Webhook", func() {

	Context("When creating EtcdCluster under Defaulting Webhook", func() {
		It("Should fill in the default value if a required field is empty", func() {
			etcdCluster := &EtcdCluster{}
			etcdCluster.Default()
			Expect(etcdCluster.Spec.Replicas).To(BeNil(), "User should have an opportunity to create cluster with 0 replicas")
			Expect(etcdCluster.Spec.Storage.EmptyDir).To(BeNil())
			storage := etcdCluster.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests.Storage()
			if Expect(storage).NotTo(BeNil()) {
				Expect(*storage).To(Equal(resource.MustParse("4Gi")))
			}
		})

		It("Should not override fields with default values if not empty", func() {
			etcdCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(5)),
					PodDisruptionBudgetTemplate: &EmbeddedPodDisruptionBudget{
						Spec: PodDisruptionBudgetSpec{
							MaxUnavailable: ptr.To(intstr.FromInt32(int32(2))),
						},
					},
					Storage: StorageSpec{
						VolumeClaimTemplate: EmbeddedPersistentVolumeClaim{
							Spec: corev1.PersistentVolumeClaimSpec{
								AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
								StorageClassName: ptr.To("local-path"),
								Resources: corev1.VolumeResourceRequirements{
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceStorage: resource.MustParse("10Gi"),
									},
								},
							},
						},
					},
				},
			}
			etcdCluster.Default()
			Expect(*etcdCluster.Spec.Replicas).To(Equal(int32(5)))
			Expect(etcdCluster.Spec.PodDisruptionBudgetTemplate).NotTo(BeNil())
			Expect(etcdCluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable.IntValue()).To(Equal(2))
			Expect(etcdCluster.Spec.Storage.EmptyDir).To(BeNil())
			storage := etcdCluster.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests.Storage()
			if Expect(storage).NotTo(BeNil()) {
				Expect(*storage).To(Equal(resource.MustParse("10Gi")))
			}
		})
	})

	Context("When creating EtcdCluster under Validating Webhook", func() {
		It("Should admit if all required fields are provided", func() {
			etcdCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(1)),
				},
			}
			w, err := etcdCluster.ValidateCreate()
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})
	})

	Context("When updating EtcdCluster under Validating Webhook", func() {
		It("Should reject changing storage type", func() {
			etcdCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(1)),
					Storage:  StorageSpec{EmptyDir: &corev1.EmptyDirVolumeSource{}},
				},
			}
			oldCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(1)),
					Storage:  StorageSpec{EmptyDir: nil},
				},
			}
			_, err := etcdCluster.ValidateUpdate(oldCluster)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("field is immutable"))
			}
		})

		It("Should reject decreasing storage size", func() {
			etcdCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(1)),
					Storage: StorageSpec{
						VolumeClaimTemplate: EmbeddedPersistentVolumeClaim{
							Spec: corev1.PersistentVolumeClaimSpec{
								Resources: corev1.VolumeResourceRequirements{
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceStorage: resource.MustParse("5Gi"),
									},
								},
							},
						},
					},
				},
			}
			oldCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(1)),
					Storage: StorageSpec{
						VolumeClaimTemplate: EmbeddedPersistentVolumeClaim{
							Spec: corev1.PersistentVolumeClaimSpec{
								Resources: corev1.VolumeResourceRequirements{
									Requests: map[corev1.ResourceName]resource.Quantity{
										corev1.ResourceStorage: resource.MustParse("10Gi"),
									},
								},
							},
						},
					},
				},
			}
			_, err := etcdCluster.ValidateUpdate(oldCluster)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("decreasing storage size is not allowed"))
			}
		})

		It("Should allow changing emptydir size", func() {
			etcdCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(1)),
					Storage:  StorageSpec{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr.To(resource.MustParse("4Gi"))}},
				},
			}
			oldCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(1)),
					Storage:  StorageSpec{EmptyDir: &corev1.EmptyDirVolumeSource{SizeLimit: ptr.To(resource.MustParse("10Gi"))}},
				},
			}
			_, err := etcdCluster.ValidateUpdate(oldCluster)
			Expect(err).To(Succeed())
		})
	})

	Context("Validate Security", func() {
		etcdCluster := &EtcdCluster{
			Spec: EtcdClusterSpec{
				Replicas: ptr.To(int32(3)),
				Security: &SecuritySpec{},
			},
		}
		It("Should admit enabled empty security", func() {
			localCluster := etcdCluster.DeepCopy()
			err := localCluster.validateSecurity()
			Expect(err).To(BeNil())
		})

		It("Should reject if only one peer secret is defined", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security.TLS = TLSSpec{
				PeerTrustedCASecret: "test-peer-ca-cert",
			}
			err := localCluster.validateSecurity()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "security", "tls"),
					localCluster.Spec.Security.TLS,
					"both spec.security.tls.peerSecret and spec.security.tls.peerTrustedCASecret must be filled or empty",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})

		It("Should reject if only one peer secret is defined", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security.TLS = TLSSpec{
				PeerSecret: "test-peer-cert",
			}
			err := localCluster.validateSecurity()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "security", "tls"),
					localCluster.Spec.Security.TLS,
					"both spec.security.tls.peerSecret and spec.security.tls.peerTrustedCASecret must be filled or empty",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})

		It("Should reject if only one client secret is defined", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security.TLS = TLSSpec{
				ClientTrustedCASecret: "test-client-ca-cert",
			}
			err := localCluster.validateSecurity()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "security", "tls"),
					localCluster.Spec.Security.TLS,
					"both spec.security.tls.clientSecret and spec.security.tls.clientTrustedCASecret must be filled or empty",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})

		It("Should reject if only one client secret is defined", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security.TLS = TLSSpec{
				ClientTrustedCASecret: "test-client-cert",
			}
			err := localCluster.validateSecurity()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "security", "tls"),
					localCluster.Spec.Security.TLS,
					"both spec.security.tls.clientSecret and spec.security.tls.clientTrustedCASecret must be filled or empty",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})

		It("Shouldn't reject if auth is enabled and security client certs are defined", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security = &SecuritySpec{
				EnableAuth: true,
				TLS: TLSSpec{
					ClientTrustedCASecret: "test-client-trusted-ca-cert",
					ClientSecret:          "test-client-cert",
					ServerSecret:          "test-server-cert",
				},
			}
			err := localCluster.validateSecurity()
			Expect(err).To(BeNil())
		})

		It("Should reject if auth is enabled and one of client and server certs is defined", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security = &SecuritySpec{
				EnableAuth: true,
				TLS: TLSSpec{
					ClientSecret:          "test-client-cert",
					ClientTrustedCASecret: "test-client-trusted-ca-cert",
				},
			}
			err := localCluster.validateSecurity()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "security"),
					localCluster.Spec.Security.TLS,
					"if auth is enabled, client secret and server secret must be provided",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}

		})

		It("Should reject if auth is enabled and one of client and server certs is defined", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Security = &SecuritySpec{
				EnableAuth: true,
				TLS: TLSSpec{
					ServerSecret: "test-server-cert",
				},
			}
			err := localCluster.validateSecurity()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "security"),
					localCluster.Spec.Security.TLS,
					"if auth is enabled, client secret and server secret must be provided",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}

		})

	})

	Context("Validate Bootstrap", func() {
		It("Should admit cluster without bootstrap", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
				},
			}
			errs := cluster.validateBootstrap()
			Expect(errs).To(BeEmpty())
		})

		It("Should admit valid bootstrap with PVC source", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &BootstrapSpec{
						Restore: &RestoreSpec{
							Source: BackupDestination{
								PVC: &PVCBackupDestination{
									ClaimName: "my-pvc",
									SubPath:   "backup.db",
								},
							},
						},
					},
				},
			}
			errs := cluster.validateBootstrap()
			Expect(errs).To(BeEmpty())
		})

		It("Should admit valid bootstrap with S3 source", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &BootstrapSpec{
						Restore: &RestoreSpec{
							Source: BackupDestination{
								S3: &S3BackupDestination{
									Endpoint:             "https://s3.example.com",
									Bucket:               "my-bucket",
									Key:                  "backup.db",
									CredentialsSecretRef: corev1.LocalObjectReference{Name: "creds"},
								},
							},
						},
					},
				},
			}
			errs := cluster.validateBootstrap()
			Expect(errs).To(BeEmpty())
		})

		It("Should reject bootstrap with both S3 and PVC", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &BootstrapSpec{
						Restore: &RestoreSpec{
							Source: BackupDestination{
								S3:  &S3BackupDestination{Endpoint: "e", Bucket: "b", Key: "k", CredentialsSecretRef: corev1.LocalObjectReference{Name: "c"}},
								PVC: &PVCBackupDestination{ClaimName: "pvc"},
							},
						},
					},
				},
			}
			errs := cluster.validateBootstrap()
			Expect(errs).NotTo(BeEmpty())
		})

		It("Should reject bootstrap with empty source", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &BootstrapSpec{
						Restore: &RestoreSpec{
							Source: BackupDestination{},
						},
					},
				},
			}
			errs := cluster.validateBootstrap()
			Expect(errs).NotTo(BeEmpty())
		})

		It("Should reject update changing bootstrap", func() {
			newCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &BootstrapSpec{
						Restore: &RestoreSpec{
							Source: BackupDestination{
								PVC: &PVCBackupDestination{ClaimName: "new-pvc"},
							},
						},
					},
				},
			}
			oldCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas: ptr.To(int32(3)),
					Bootstrap: &BootstrapSpec{
						Restore: &RestoreSpec{
							Source: BackupDestination{
								PVC: &PVCBackupDestination{ClaimName: "old-pvc"},
							},
						},
					},
				},
			}
			_, err := newCluster.ValidateUpdate(oldCluster)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("immutable"))
			}
		})

		It("Should allow update not changing bootstrap", func() {
			bootstrap := &BootstrapSpec{
				Restore: &RestoreSpec{
					Source: BackupDestination{
						PVC: &PVCBackupDestination{ClaimName: "same-pvc"},
					},
				},
			}
			newCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas:  ptr.To(int32(3)),
					Bootstrap: bootstrap.DeepCopy(),
				},
			}
			oldCluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Replicas:  ptr.To(int32(3)),
					Bootstrap: bootstrap.DeepCopy(),
				},
			}
			_, err := newCluster.ValidateUpdate(oldCluster)
			Expect(err).To(Succeed())
		})
	})

	Context("Validate PDB", func() {
		etcdCluster := &EtcdCluster{
			Spec: EtcdClusterSpec{
				Replicas:                    ptr.To(int32(3)),
				PodDisruptionBudgetTemplate: &EmbeddedPodDisruptionBudget{},
			},
		}
		It("Should admit enabled empty PDB", func() {
			localCluster := etcdCluster.DeepCopy()
			w, err := localCluster.validatePdb()
			Expect(err).To(BeNil())
			Expect(w).To(BeEmpty())
		})
		It("Should reject if negative spec.podDisruptionBudget.minAvailable", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MinAvailable = ptr.To(intstr.FromInt32(int32(-1)))
			_, err := localCluster.validatePdb()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "podDisruptionBudget", "minAvailable"),
					-1,
					"value cannot be less than zero",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})
		It("Should reject if negative spec.podDisruptionBudget.maxUnavailable", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(int32(-1)))
			_, err := localCluster.validatePdb()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "podDisruptionBudget", "maxUnavailable"),
					-1,
					"value cannot be less than zero",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})
		It("Should reject if min available field larger than replicas", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Replicas = ptr.To(int32(1))
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MinAvailable = ptr.To(intstr.FromInt32(int32(2)))
			_, err := localCluster.validatePdb()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "podDisruptionBudget", "minAvailable"),
					2,
					"value cannot be larger than number of replicas",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})
		It("Should reject if max unavailable field larger than replicas", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Replicas = ptr.To(int32(1))
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(int32(2)))
			_, err := localCluster.validatePdb()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "podDisruptionBudget", "maxUnavailable"),
					2,
					"value cannot be larger than number of replicas",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})
		It("should accept correct percentage value for minAvailable", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.Replicas = ptr.To(int32(4))
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MinAvailable = ptr.To(intstr.FromString("50%"))
			warnings, err := localCluster.validatePdb()
			Expect(err).To(BeNil())
			Expect(warnings).To(ContainElement("current number of spec.podDisruptionBudget.minAvailable can lead to loss of quorum"))
		})
		It("should accept correct percentage value for maxUnavailable", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable = ptr.To(intstr.FromString("50%"))
			warnings, err := localCluster.validatePdb()
			Expect(err).To(BeNil())
			Expect(warnings).To(ContainElement("current number of spec.podDisruptionBudget.maxUnavailable can lead to loss of quorum"))
		})
		It("Should reject incorrect value for maxUnavailable", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable = ptr.To(intstr.FromString("50$"))
			_, err := localCluster.validatePdb()
			if Expect(err).NotTo(BeNil()) {
				expectedFieldErr := field.Invalid(
					field.NewPath("spec", "podDisruptionBudget", "maxUnavailable"),
					"50$",
					"invalid percentage value",
				)
				if Expect(err).To(HaveLen(1)) {
					Expect(*(err[0])).To(Equal(*expectedFieldErr))
				}
			}
		})
		It("should correctly use zero numeric value for maxUnavailable PDB", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable = ptr.To(intstr.FromInt32(int32(0)))
			_, err := localCluster.validatePdb()
			Expect(err).To(BeNil())
		})
		It("should correctly use zero string value for PDB", func() {
			localCluster := etcdCluster.DeepCopy()
			localCluster.Spec.PodDisruptionBudgetTemplate.Spec.MaxUnavailable = ptr.To(intstr.FromString("0"))
			_, err := localCluster.validatePdb()
			Expect(err).To(BeNil())
		})
	})
})
