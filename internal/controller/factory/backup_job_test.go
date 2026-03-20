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
	"strings"
	"testing"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
)

func TestCreateBackupJob_PVC(t *testing.T) {
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

	backup := &etcdaenixiov1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-backup",
			Namespace: "default",
			UID:       "test-uid",
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
			Destination: etcdaenixiov1alpha1.BackupDestination{
				PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
					ClaimName: "backup-pvc",
				},
			},
		},
	}

	job, err := CreateBackupJob(backup, cluster, "test-image:latest", scheme)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	if job.Name != "my-backup-backup" {
		t.Errorf("expected job name 'my-backup-backup', got %q", job.Name)
	}
	if job.Namespace != "default" {
		t.Errorf("expected namespace 'default', got %q", job.Namespace)
	}
	if *job.Spec.BackoffLimit != 0 {
		t.Errorf("expected backoffLimit 0, got %d", *job.Spec.BackoffLimit)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 600 {
		t.Errorf("expected TTLSecondsAfterFinished 600, got %v", job.Spec.TTLSecondsAfterFinished)
	}
	if job.Spec.Template.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("expected RestartPolicyNever, got %s", job.Spec.Template.Spec.RestartPolicy)
	}

	container := job.Spec.Template.Spec.Containers[0]
	if container.Name != "backup-agent" {
		t.Errorf("expected container name 'backup-agent', got %q", container.Name)
	}
	if container.Image != "test-image:latest" {
		t.Errorf("expected image 'test-image:latest', got %q", container.Image)
	}
	if len(container.Command) != 1 || container.Command[0] != "/backup-agent" {
		t.Errorf("expected command [/backup-agent], got %v", container.Command)
	}

	// Check PVC volume mount
	foundBackupVolume := false
	for _, v := range job.Spec.Template.Spec.Volumes {
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
	if len(job.OwnerReferences) != 1 {
		t.Fatalf("expected 1 owner reference, got %d", len(job.OwnerReferences))
	}
	if job.OwnerReferences[0].Name != "my-backup" {
		t.Errorf("expected owner name 'my-backup', got %q", job.OwnerReferences[0].Name)
	}

	// Check labels
	if job.Labels["etcd.aenix.io/etcdbackup-name"] != "my-backup" {
		t.Errorf("expected label etcd.aenix.io/etcdbackup-name=my-backup, got %q", job.Labels["etcd.aenix.io/etcdbackup-name"])
	}
	if job.Labels["app.kubernetes.io/managed-by"] != etcdOperatorName {
		t.Errorf("expected managed-by label, got %q", job.Labels["app.kubernetes.io/managed-by"])
	}
}

func TestCreateBackupJob_S3(t *testing.T) {
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

	backup := &etcdaenixiov1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "s3-backup",
			Namespace: "default",
			UID:       "test-uid-s3",
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
			Destination: etcdaenixiov1alpha1.BackupDestination{
				S3: &etcdaenixiov1alpha1.S3BackupDestination{
					Endpoint:             "https://s3.example.com",
					Bucket:               "backups",
					Key:                  "etcd/snap.db",
					CredentialsSecretRef: corev1.LocalObjectReference{Name: "aws-creds"},
					Region:               "eu-west-1",
				},
			},
		},
	}

	job, err := CreateBackupJob(backup, cluster, "test-image:latest", scheme)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]

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
	if envMap["S3_REGION"].Value != "eu-west-1" {
		t.Errorf("expected S3_REGION=eu-west-1, got %q", envMap["S3_REGION"].Value)
	}

	// Check credentials secret refs
	awsAccessKey := envMap["AWS_ACCESS_KEY_ID"]
	if awsAccessKey.ValueFrom == nil || awsAccessKey.ValueFrom.SecretKeyRef.Name != "aws-creds" {
		t.Error("AWS_ACCESS_KEY_ID should reference aws-creds secret")
	}
	awsSecretKey := envMap["AWS_SECRET_ACCESS_KEY"]
	if awsSecretKey.ValueFrom == nil || awsSecretKey.ValueFrom.SecretKeyRef.Name != "aws-creds" {
		t.Error("AWS_SECRET_ACCESS_KEY should reference aws-creds secret")
	}

	// S3 backup should have no PVC volumes
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Name == backupData {
			t.Error("S3 backup should not have backup-data volume")
		}
	}
}

func TestCreateBackupJob_WithTLS(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = etcdaenixiov1alpha1.AddToScheme(scheme)

	cluster := &etcdaenixiov1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-etcd",
			Namespace: "default",
		},
		Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
			Replicas: ptr.To(int32(3)),
			Storage: etcdaenixiov1alpha1.StorageSpec{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
			Security: &etcdaenixiov1alpha1.SecuritySpec{
				TLS: etcdaenixiov1alpha1.TLSSpec{
					ClientSecret:          "client-cert",
					ClientTrustedCASecret: "client-ca",
					ServerSecret:          "server-cert",
					ServerTrustedCASecret: "server-ca",
				},
			},
		},
	}

	backup := &etcdaenixiov1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "tls-backup",
			Namespace: "default",
			UID:       "test-uid-tls",
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "tls-etcd"},
			Destination: etcdaenixiov1alpha1.BackupDestination{
				PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
					ClaimName: "backup-pvc",
				},
			},
		},
	}

	job, err := CreateBackupJob(backup, cluster, "test-image:latest", scheme)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]
	envMap := make(map[string]corev1.EnvVar)
	for _, e := range container.Env {
		envMap[e.Name] = e
	}

	if envMap["ETCD_TLS_ENABLED"].Value != "true" {
		t.Error("expected ETCD_TLS_ENABLED=true")
	}
	if envMap["ETCD_TLS_CERT_PATH"].Value != "/etc/etcd/pki/client/cert/tls.crt" {
		t.Errorf("unexpected ETCD_TLS_CERT_PATH: %q", envMap["ETCD_TLS_CERT_PATH"].Value)
	}
	if envMap["ETCD_TLS_KEY_PATH"].Value != "/etc/etcd/pki/client/cert/tls.key" {
		t.Errorf("unexpected ETCD_TLS_KEY_PATH: %q", envMap["ETCD_TLS_KEY_PATH"].Value)
	}
	if envMap["ETCD_TLS_CA_PATH"].Value != "/etc/etcd/pki/server/ca/ca.crt" {
		t.Errorf("unexpected ETCD_TLS_CA_PATH: %q", envMap["ETCD_TLS_CA_PATH"].Value)
	}

	// Check volumes
	volumeMap := make(map[string]corev1.Volume)
	for _, v := range job.Spec.Template.Spec.Volumes {
		volumeMap[v.Name] = v
	}
	if v, ok := volumeMap["client-certificate"]; !ok {
		t.Error("client-certificate volume not found")
	} else if v.VolumeSource.Secret.SecretName != "client-cert" {
		t.Errorf("expected secret 'client-cert', got %q", v.VolumeSource.Secret.SecretName)
	}
	if v, ok := volumeMap["server-trusted-ca-certificate"]; !ok {
		t.Error("server-trusted-ca-certificate volume not found")
	} else if v.VolumeSource.Secret.SecretName != "server-ca" {
		t.Errorf("expected secret 'server-ca', got %q", v.VolumeSource.Secret.SecretName)
	}

	// Check volume mounts
	mountMap := make(map[string]corev1.VolumeMount)
	for _, m := range container.VolumeMounts {
		mountMap[m.Name] = m
	}
	if _, ok := mountMap["client-certificate"]; !ok {
		t.Error("client-certificate volume mount not found")
	}
	if _, ok := mountMap["server-trusted-ca-certificate"]; !ok {
		t.Error("server-trusted-ca-certificate volume mount not found")
	}

	// Endpoints should use https
	if envMap["ETCD_ENDPOINTS"].Value == "" {
		t.Error("ETCD_ENDPOINTS should not be empty")
	}
	etcdEndpointsSlice := strings.Split(envMap["ETCD_ENDPOINTS"].Value, ",")
	for _, endpoint := range etcdEndpointsSlice {
		if !strings.HasPrefix(endpoint, "https://") {
			t.Errorf("expected endpoint to start with https://, got %q", endpoint)
		}
	}
}

func TestCreateBackupJob_PVCSubPath(t *testing.T) {
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

	backup := &etcdaenixiov1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "subpath-backup",
			Namespace: "default",
			UID:       "test-uid-subpath",
		},
		Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
			ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
			Destination: etcdaenixiov1alpha1.BackupDestination{
				PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
					ClaimName: "backup-pvc",
					SubPath:   "daily/snap-2024.db",
				},
			},
		},
	}

	job, err := CreateBackupJob(backup, cluster, "test-image:latest", scheme)
	if err != nil {
		t.Fatalf("CreateBackupJob failed: %v", err)
	}

	container := job.Spec.Template.Spec.Containers[0]
	envMap := make(map[string]corev1.EnvVar)
	for _, e := range container.Env {
		envMap[e.Name] = e
	}

	expected := "/backup/data/daily/snap-2024.db"
	if envMap["PVC_BACKUP_PATH"].Value != expected {
		t.Errorf("expected PVC_BACKUP_PATH=%q, got %q", expected, envMap["PVC_BACKUP_PATH"].Value)
	}
}

func TestGetBackupJobName(t *testing.T) {
	backup := &etcdaenixiov1alpha1.EtcdBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "my-backup"},
	}
	expected := "my-backup-backup"
	if got := GetBackupJobName(backup); got != expected {
		t.Errorf("expected %q, got %q", expected, got)
	}
}
