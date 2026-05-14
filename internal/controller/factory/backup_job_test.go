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
	"errors"
	"strings"
	"testing"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
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

	if job.GenerateName != "my-backup-" {
		t.Errorf("expected job generateName 'my-backup-', got %q", job.GenerateName)
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
	// Pin ImagePullPolicy=IfNotPresent: kubernetes defaults
	// :latest tags to Always, which would short-circuit a node's
	// kind-loaded image and re-pull from the registry — silently
	// running an OLDER operator binary as the backup-agent than
	// what the freshly-deployed manager pod runs. That divergence
	// is fatal for the marker parser (a pre-this-PR agent emits
	// "snapshot written successfully (N bytes)" which the
	// terminal-marker regex does not recognise) and turns the e2e
	// "With PVC backup" spec into a check of whatever was last
	// published under :latest. Mirroring the manager pod's policy
	// (config/manager/manager.yaml) keeps the two pods using the
	// SAME image bytes at install/upgrade time.
	if container.ImagePullPolicy != corev1.PullIfNotPresent {
		t.Errorf("expected ImagePullPolicy=IfNotPresent (must match manager pod's policy so the backup-agent runs the same image bytes as the just-deployed manager); got %q", container.ImagePullPolicy)
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

	// Check ephemeral storage (EmptyDir without SizeLimit → etcd default 2Gi)
	expectedEphemeral := resource.NewQuantity(2*1024*1024*1024, resource.BinarySI)
	if req, ok := container.Resources.Requests[corev1.ResourceEphemeralStorage]; !ok {
		t.Error("ephemeral-storage request not set")
	} else if req.Cmp(*expectedEphemeral) != 0 {
		t.Errorf("expected ephemeral-storage request %s, got %s", expectedEphemeral.String(), req.String())
	}
	if lim, ok := container.Resources.Limits[corev1.ResourceEphemeralStorage]; !ok {
		t.Error("ephemeral-storage limit not set")
	} else if lim.Cmp(*expectedEphemeral) != 0 {
		t.Errorf("expected ephemeral-storage limit %s, got %s", expectedEphemeral.String(), lim.String())
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
					Key:                  "etcd/backups",
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
	if envMap["S3_KEY"].Value != "etcd/backups/s3-backup.db" {
		t.Errorf("expected S3_KEY=etcd/backups/s3-backup.db, got %q", envMap["S3_KEY"].Value)
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
					SubPath:   "daily",
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

	expected := "/backup/data/daily/subpath-backup.db"
	if envMap["PVC_BACKUP_PATH"].Value != expected {
		t.Errorf("expected PVC_BACKUP_PATH=%q, got %q", expected, envMap["PVC_BACKUP_PATH"].Value)
	}
}

// TestCreateBackupJob_RejectsTraversalSubPath pins the SubPath
// hardening: a user-supplied PVC SubPath containing path-escape
// patterns (../, leading /, empty components, backslashes) must
// fail at factory time so the agent never sees PVC_BACKUP_PATH
// that escapes /backup/data — and the URI we report into
// status.snapshot can't advertise an out-of-mount location.
func TestCreateBackupJob_RejectsTraversalSubPath(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = etcdaenixiov1alpha1.AddToScheme(scheme)

	cluster := &etcdaenixiov1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-etcd", Namespace: "default"},
		Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
			Replicas: ptr.To(int32(1)),
			Storage:  etcdaenixiov1alpha1.StorageSpec{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}

	cases := []struct {
		name    string
		subPath string
	}{
		{"parent traversal", "../../etc"},
		{"leading parent", "../escape"},
		{"hidden parent in middle", "good/../bad"},
		{"single dot dot", ".."},
		{"single dot segment", "good/./bad"},
		{"lone dot", "."},
		{"leading dot segment", "./bad"},
		{"trailing dot segment", "good/."},
		{"absolute path", "/etc"},
		{"empty segment leading", "/leading"},
		{"empty segment trailing", "trailing/"},
		{"double slash", "good//bad"},
		{"backslash", `bad\path`},
		{"null byte", "good\x00bad"},
		// C0 controls + DEL: must be rejected so the agent's
		// terminal "snapshot written: …" marker line cannot be
		// torn by an embedded LF / CR / TAB in PVC_BACKUP_PATH
		// (the controller parses one marker line, the URI cell is
		// %q-quoted but a downstream tool that strips quotes would
		// resplit on these). NUL is the C-string-truncation
		// footgun; the rest are shell-active.
		{"newline (LF)", "good\nbad"},
		{"carriage return (CR)", "good\rbad"},
		{"tab", "good\tbad"},
		{"low control byte", "good\x01bad"},
		{"escape control byte", "good\x1bbad"},
		{"DEL", "good\x7fbad"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			backup := &etcdaenixiov1alpha1.EtcdBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "trav-backup",
					Namespace: "default",
					UID:       "trav-uid",
				},
				Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
					Destination: etcdaenixiov1alpha1.BackupDestination{
						PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
							ClaimName: "backup-pvc",
							SubPath:   tc.subPath,
						},
					},
				},
			}
			_, err := CreateBackupJob(backup, cluster, "test-image:latest", scheme)
			if err == nil {
				t.Fatalf("CreateBackupJob accepted SubPath=%q; want rejection", tc.subPath)
			}
			// The controller branches on this sentinel to surface
			// a terminal Phase=Failed condition; unwrapping breaks
			// the user-facing error reporting path.
			if !errors.Is(err, ErrInvalidSpec) {
				t.Fatalf("err is %v; want errors.Is(_, ErrInvalidSpec) so controllers can surface as terminal Failed", err)
			}
		})
	}
}

// TestCreateBackupJob_AcceptsValidSubPath sanity-checks that the
// hardening doesn't over-reject legitimate sub-paths (multi-segment,
// dots within a segment, hyphens, numbers).
func TestCreateBackupJob_AcceptsValidSubPath(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = etcdaenixiov1alpha1.AddToScheme(scheme)

	cluster := &etcdaenixiov1alpha1.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "my-etcd", Namespace: "default"},
		Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
			Replicas: ptr.To(int32(1)),
			Storage:  etcdaenixiov1alpha1.StorageSpec{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		},
	}

	cases := []string{
		"daily",
		"prod/etcd",
		"backups-2025/april",
		"v3.5.12/rev42",
	}
	for _, sp := range cases {
		t.Run(sp, func(t *testing.T) {
			backup := &etcdaenixiov1alpha1.EtcdBackup{
				ObjectMeta: metav1.ObjectMeta{
					Name: "ok-backup", Namespace: "default", UID: "ok-uid",
				},
				Spec: etcdaenixiov1alpha1.EtcdBackupSpec{
					ClusterRef: corev1.LocalObjectReference{Name: "my-etcd"},
					Destination: etcdaenixiov1alpha1.BackupDestination{
						PVC: &etcdaenixiov1alpha1.PVCBackupDestination{
							ClaimName: "backup-pvc",
							SubPath:   sp,
						},
					},
				},
			}
			if _, err := CreateBackupJob(backup, cluster, "test-image:latest", scheme); err != nil {
				t.Fatalf("CreateBackupJob rejected legitimate SubPath=%q: %v", sp, err)
			}
		})
	}
}

func TestGetEffectiveDBQuota(t *testing.T) {
	tests := []struct {
		name     string
		cluster  *etcdaenixiov1alpha1.EtcdCluster
		expected int64
	}{
		{
			name: "explicit quota-backend-bytes",
			cluster: &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Options: map[string]string{"quota-backend-bytes": "8589934592"}, // 8Gi
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			expected: 8589934592,
		},
		{
			name: "derived from EmptyDir SizeLimit",
			cluster: &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							SizeLimit: ptr.To(resource.MustParse("4Gi")),
						},
					},
				},
			},
			expected: 4 * 1024 * 1024 * 1024, // 4Gi
		},
		{
			name: "derived from PVC storage request",
			cluster: &etcdaenixiov1alpha1.EtcdCluster{
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
			},
			expected: 10 * 1024 * 1024 * 1024, // 10Gi
		},
		{
			name: "EmptyDir without SizeLimit falls back to etcd default",
			cluster: &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			expected: 2 * 1024 * 1024 * 1024, // 2Gi
		},
		{
			name: "invalid quota-backend-bytes falls through to storage size",
			cluster: &etcdaenixiov1alpha1.EtcdCluster{
				Spec: etcdaenixiov1alpha1.EtcdClusterSpec{
					Options: map[string]string{"quota-backend-bytes": "not-a-number"},
					Storage: etcdaenixiov1alpha1.StorageSpec{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							SizeLimit: ptr.To(resource.MustParse("4Gi")),
						},
					},
				},
			},
			expected: 4 * 1024 * 1024 * 1024, // 4Gi
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := getEffectiveDBQuota(tt.cluster)
			expectedQ := resource.NewQuantity(tt.expected, resource.BinarySI)
			if got.Cmp(*expectedQ) != 0 {
				t.Errorf("expected %s (%d bytes), got %s (%d bytes)",
					expectedQ.String(), tt.expected, got.String(), got.Value())
			}
		})
	}
}
