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
)

var _ = Describe("EtcdBackup Webhook", func() {

	Context("When creating EtcdBackup under Validating Webhook", func() {
		It("Should admit a valid S3 backup", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						S3: &S3BackupDestination{
							Endpoint:             "https://s3.amazonaws.com",
							Bucket:               "my-bucket",
							Key:                  "backups/snapshot.db",
							CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
						},
					},
				},
			}
			w, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})

		It("Should admit a valid PVC backup", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			w, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})

		It("Should reject if both S3 and PVC are set", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						S3: &S3BackupDestination{
							Endpoint:             "https://s3.amazonaws.com",
							Bucket:               "my-bucket",
							Key:                  "backups/snapshot.db",
							CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
						},
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("exactly one of s3 or pvc must be specified"))
			}
		})

		It("Should reject if neither S3 nor PVC is set", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("exactly one of s3 or pvc must be specified"))
			}
		})

		It("Should reject if clusterRef.name is empty", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: ""},
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("clusterRef.name"))
			}
		})

		It("Should reject S3 with invalid endpoint URL", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						S3: &S3BackupDestination{
							Endpoint:             "not-a-url",
							Bucket:               "my-bucket",
							Key:                  "backups/snapshot.db",
							CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
						},
					},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("endpoint must start with http:// or https://"))
			}
		})

		It("Should reject S3 with empty required fields", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						S3: &S3BackupDestination{},
					},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("endpoint"))
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("bucket"))
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("credentialsSecretRef"))
			}
		})

		It("Should reject PVC with path traversal in subPath", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
							SubPath:   "../../etc/shadow",
						},
					},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("subPath"))
			}
		})

		It("Should reject PVC with absolute subPath", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
							SubPath:   "/etc/shadow",
						},
					},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("subPath"))
			}
		})

		It("Should reject PVC with empty claimName", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{},
					},
				},
			}
			_, err := etcdBackupValidator.ValidateCreate(ctx, backup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("claimName"))
			}
		})

	})

	Context("When updating EtcdBackup under Validating Webhook", func() {
		It("Should allow status-only updates (spec unchanged)", func() {
			backup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{ClaimName: "backup-pvc"},
					},
				},
			}
			oldBackup := backup.DeepCopy()
			w, err := etcdBackupValidator.ValidateUpdate(ctx, oldBackup, backup)
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})

		It("Should reject spec changes", func() {
			oldBackup := &EtcdBackup{
				Spec: EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{ClaimName: "backup-pvc"},
					},
				},
			}
			newBackup := oldBackup.DeepCopy()
			newBackup.Spec.ClusterRef.Name = "other-cluster"
			_, err := etcdBackupValidator.ValidateUpdate(ctx, oldBackup, newBackup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("immutable"))
			}
		})
	})

	Context("When deleting EtcdBackup under Validating Webhook", func() {
		It("Should always allow deletion", func() {
			backup := &EtcdBackup{}
			w, err := etcdBackupValidator.ValidateDelete(ctx, backup)
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})
	})
})
