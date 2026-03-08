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

var _ = Describe("EtcdBackupSchedule Webhook", func() {

	Context("When creating EtcdBackupSchedule under Validating Webhook", func() {
		It("Should admit a valid schedule with PVC destination", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:    "0 */6 * * *",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			w, err := schedule.ValidateCreate()
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})

		It("Should admit a valid schedule with S3 destination", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "@daily",
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
			w, err := schedule.ValidateCreate()
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})

		It("Should reject if clusterRef.name is empty", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: ""},
					Schedule:   "0 0 * * *",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			_, err := schedule.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("clusterRef.name"))
			}
		})

		It("Should reject if schedule is empty", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			_, err := schedule.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("schedule"))
			}
		})

		It("Should reject an invalid cron expression", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "not-a-cron",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			_, err := schedule.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("schedule"))
			}
		})

		It("Should reject if neither S3 nor PVC is set", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef:  corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:    "0 0 * * *",
					Destination: BackupDestination{},
				},
			}
			_, err := schedule.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("exactly one of s3 or pvc must be specified"))
			}
		})

		It("Should reject if both S3 and PVC are set", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "0 0 * * *",
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
			_, err := schedule.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("exactly one of s3 or pvc must be specified"))
			}
		})

		It("Should reject PVC with path traversal in subPath", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "0 0 * * *",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
							SubPath:   "../../etc/shadow",
						},
					},
				},
			}
			_, err := schedule.ValidateCreate()
			if Expect(err).To(HaveOccurred()) {
				statusErr := err.(*errors.StatusError)
				Expect(statusErr.ErrStatus.Message).To(ContainSubstring("subPath"))
			}
		})

		It("Should admit cron shorthand @hourly", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "@hourly",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{
							ClaimName: "backup-pvc",
						},
					},
				},
			}
			w, err := schedule.ValidateCreate()
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})
	})

	Context("When updating EtcdBackupSchedule under Validating Webhook", func() {
		It("Should allow valid spec changes (schedules are mutable)", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "0 0 * * *",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{ClaimName: "backup-pvc"},
					},
				},
			}
			oldSchedule := schedule.DeepCopy()
			schedule.Spec.Schedule = "0 */12 * * *"
			w, err := schedule.ValidateUpdate(oldSchedule)
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})

		It("Should reject update with invalid cron expression", func() {
			schedule := &EtcdBackupSchedule{
				Spec: EtcdBackupScheduleSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-cluster"},
					Schedule:   "invalid",
					Destination: BackupDestination{
						PVC: &PVCBackupDestination{ClaimName: "backup-pvc"},
					},
				},
			}
			oldSchedule := schedule.DeepCopy()
			oldSchedule.Spec.Schedule = "0 0 * * *"
			_, err := schedule.ValidateUpdate(oldSchedule)
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When deleting EtcdBackupSchedule under Validating Webhook", func() {
		It("Should always allow deletion", func() {
			schedule := &EtcdBackupSchedule{}
			w, err := schedule.ValidateDelete()
			Expect(err).To(Succeed())
			Expect(w).To(BeEmpty())
		})
	})
})
