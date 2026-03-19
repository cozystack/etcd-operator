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
	"testing"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
)

const awsCreds = "aws-creds"

func TestCreateBackupCronJob_PVC(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = etcdaenixiov1alpha1.AddToScheme(scheme)

	cluster := &etcdaenixiov1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-etcd",
			Namespace: "default",
		},
		Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
			Replicas: ptr.To(int32(3)),
			Storage: etcdaenixiov1alpha1.StorageSpec{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	schedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-schedule",
			Namespace: "default",
			UID:       "test-uid",
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupScheduleSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
			Schedule:   "0 */6 * * *",
			Destination: etcdaenixiov1alpha1.BackupDestination{
				PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
					ClaimName: "backup-pvc",
				},
			},
		},
	}

	cronJob, err := CreateBackupCronJob(schedule, cluster, "test-image:latest", scheme)
	if err != nil {
		t.Fatalf("CreateBackupCronJob failed: %v", err)
	}

	if cronJob.Name != "my-schedule-scheduled-backup" {
		t.Errorf("expected cronjob name 'my-schedule-scheduled-backup', got %q", cronJob.Name)
	}
	if cronJob.Namespace != "default" {
		t.Errorf("expected namespace 'default', got %q", cronJob.Namespace)
	}
	if cronJob.Spec.Schedule != "0 */6 * * *" {
		t.Errorf("expected schedule '0 */6 * * *', got %q", cronJob.Spec.Schedule)
	}
	if cronJob.Spec.ConcurrencyPolicy != "Forbid" {
		t.Errorf("expected ConcurrencyPolicy 'Forbid', got %q", cronJob.Spec.ConcurrencyPolicy)
	}

	container := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0]
	if container.Name != "backup-agent" {
		t.Errorf("expected container name 'backup-agent', got %q", container.Name)
	}
	if container.Image != "test-image:latest" {
		t.Errorf("expected image 'test-image:latest', got %q", container.Image)
	}
	if len(container.Command) != 1 || container.Command[0] != "/backup-agent" {
		t.Errorf("expected command [/backup-agent], got %v", container.Command)
	}

	// Check PVC volume
	foundBackupVolume := false
	for _, v := range cronJob.Spec.JobTemplate.Spec.Template.Spec.Volumes {
		if v.Name == backupData {
			foundBackupVolume = true
			if v.VolumeSource.PersistentVolumeClaim.ClaimName != "backup-pvc" {
				t.Errorf("expected PVC claim 'backup-pvc', got %q", v.VolumeSource.PersistentVolumeClaim.ClaimName)
			}
		}
	}
	if !foundBackupVolume {
		t.Error("backup-data volume not found")
	}

	// Check owner reference
	if len(cronJob.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(cronJob.OwnerReferences))
	}
	if cronJob.OwnerReferences[0].Name != "my-schedule" {
		t.Errorf("expected owner name 'my-schedule', got %q", cronJob.OwnerReferences[0].Name)
	}

	// Check labels
	if cronJob.Labels["etcd.aenix.io/etcdbackupschedule-name"] != "my-schedule" {
		t.Errorf("expected label etcd.aenix.io/etcdbackupschedule-name=my-schedule, got %q", cronJob.Labels["etcd.aenix.io/etcdbackupschedule-name"])
	}
	if cronJob.Labels["app.kubernetes.io/managed-by"] != etcdOperatorName {
		t.Errorf("expected managed-by label, got %q", cronJob.Labels["app.kubernetes.io/managed-by"])
	}
}

func TestCreateBackupCronJob_S3(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = etcdaenixiov1alpha1.AddToScheme(scheme)

	cluster := &etcdaenixiov1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-etcd",
			Namespace: "default",
		},
		Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
			Replicas: ptr.To(int32(3)),
			Storage: etcdaenixiov1alpha1.StorageSpec{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	schedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s3-schedule",
			Namespace: "default",
			UID:       "test-uid-s3",
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupScheduleSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
			Schedule:   "@daily",
			Destination: etcdaenixiov1alpha1.BackupDestination{
				S3: &etcdaenixiov1alpha1.S3BackupDestination{
					Endpoint:             "https://s3.example.com",
					Bucket:               "backups",
					Key:                  "etcd/snap.db",
					CredentialsSecretRef: corev1.LocalObjectReference{Name: awsCreds},
					Region:               "eu-west-1",
				},
			},
		},
	}

	cronJob, err := CreateBackupCronJob(schedule, cluster, "test-image:latest", scheme)
	if err != nil {
		t.Fatalf("CreateBackupCronJob failed: %v", err)
	}

	container := cronJob.Spec.JobTemplate.Spec.Template.Spec.Containers[0]

	envMap := make(map[string]corev1.EnvVar)
	for _, e := range container.Env {
		envMap[e.Name] = e
	}

	if envMap["BACKUP_DESTINATION"].Value != "s3" {
		t.Errorf("expected BACKUP_DESTINATION=s3, got %q", envMap["BACKUP_DESTINATION"].Value)
	}
	if envMap["S3_BUCKET"].Value != "backups" {
		t.Errorf("expected S3_BUCKET=backups, got %q", envMap["S3_BUCKET"].Value)
	}
	if envMap["S3_KEY"].Value != "etcd/snap.db" {
		t.Errorf("expected S3_KEY=etcd/snap.db, got %q", envMap["S3_KEY"].Value)
	}

	awsAccessKey := envMap["AWS_ACCESS_KEY_ID"]
	if awsAccessKey.ValueFrom == nil || awsAccessKey.ValueFrom.SecretKeyRef.Name != awsCreds {
		t.Error("AWS_ACCESS_KEY_ID should reference aws-creds secret")
	}
}

func TestCreateBackupCronJob_HistoryLimits(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = etcdaenixiov1alpha1.AddToScheme(scheme)

	cluster := &etcdaenixiov1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-etcd",
			Namespace: "default",
		},
		Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
			Replicas: ptr.To(int32(1)),
			Storage: etcdaenixiov1alpha1.StorageSpec{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}

	schedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "limit-schedule",
			Namespace: "default",
			UID:       "test-uid-limits",
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupScheduleSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
			Schedule:   "0 0 * * *",
			Destination: etcdaenixiov1alpha1.BackupDestination{
				PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
					ClaimName: "backup-pvc",
				},
			},
			SuccessfulJobsHistoryLimit: ptr.To(int32(5)),
			FailedJobsHistoryLimit:     ptr.To(int32(2)),
		},
	}

	cronJob, err := CreateBackupCronJob(schedule, cluster, "test-image:latest", scheme)
	if err != nil {
		t.Fatalf("CreateBackupCronJob failed: %v", err)
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit == nil || *cronJob.Spec.SuccessfulJobsHistoryLimit != 5 {
		t.Errorf("expected SuccessfulJobsHistoryLimit=5, got %v", cronJob.Spec.SuccessfulJobsHistoryLimit)
	}
	if cronJob.Spec.FailedJobsHistoryLimit == nil || *cronJob.Spec.FailedJobsHistoryLimit != 2 {
		t.Errorf("expected FailedJobsHistoryLimit=2, got %v", cronJob.Spec.FailedJobsHistoryLimit)
	}
}

func TestGetBackupCronJobName(t *testing.T) {
	schedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{
		ObjectMeta: metav1.ObjectMeta{Name: "my-schedule"},
	}
	expected := "my-schedule-scheduled-backup"
	if got := GetBackupCronJobName(schedule); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}
