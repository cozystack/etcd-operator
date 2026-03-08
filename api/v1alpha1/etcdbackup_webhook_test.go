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
			w, err := backup.ValidateCreate()
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
			w, err := backup.ValidateCreate()
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
			_, err := backup.ValidateCreate()
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
			_, err := backup.ValidateCreate()
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
			_, err := backup.ValidateCreate()
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
			_, err := backup.ValidateCreate()
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
			_, err := backup.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("endpoint"))
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("bucket"))
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("key"))
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
			_, err := backup.ValidateCreate()
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
			_, err := backup.ValidateCreate()
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
			_, err := backup.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("claimName"))
			}
		})

		It("Should reject name exceeding 56 characters", func() {
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
			backup.Name = "this-name-is-way-too-long-for-a-backup-job-name-suffix-exceeds"
			_, err := backup.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("name must be at most 56 characters"))
			}
		})

		It("Should admit name at exactly 56 characters", func() {
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
			backup.Name = "abcdefghijklmnopqrstuvwxyz12345678901234567890123456" // exactly 56 chars
			w, err := backup.ValidateCreate()
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
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
			w, err := backup.ValidateUpdate(oldBackup)
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
			_, err := newBackup.ValidateUpdate(oldBackup)
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("immutable"))
			}
		})
	})

	Context("When deleting EtcdBackup under Validating Webhook", func() {
		It("Should always allow deletion", func() {
			backup := &EtcdBackup{}
			w, err := backup.ValidateDelete()
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})
	})
})
