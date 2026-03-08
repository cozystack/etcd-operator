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
	"fmt"
	"strings"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
)

// GetBackupJobName returns the deterministic Job name for a given EtcdBackup.
func GetBackupJobName(backup *etcdaenixiov1alpha1.EtcdBackup) string {
	return fmt.Sprintf("%s-backup", backup.Name)
}

// CreateBackupJob builds a Job that runs the backup-agent to take an etcd snapshot
// and store it to the configured destination.
func CreateBackupJob(
	backup *etcdaenixiov1alpha1.EtcdBackup,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	operatorImage string,
	scheme *runtime.Scheme,
) (*batchv1.Job, error) {
	labels := NewLabelsBuilder().WithName().WithInstance(cluster.Name).WithManagedBy()
	labels["etcd.aenix.io/etcdbackup-name"] = backup.Name

	var backoffLimit int32
	var ttl int32 = 600
	var activeDeadline int64 = 900 // 15 minutes; safety net if backup-agent hangs
	container, volumes := buildBackupContainer(backup.Name, backup.Spec.Destination, cluster, operatorImage)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetBackupJobName(backup),
			Namespace: backup.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoffLimit,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
						RunAsUser:    ptr.To(int64(65532)),
						RunAsGroup:   ptr.To(int64(65532)),
						FSGroup:      ptr.To(int64(65532)),
					},
					Containers:    []corev1.Container{container},
					Volumes:       volumes,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(backup, job, scheme); err != nil {
		return nil, fmt.Errorf("cannot set controller reference: %w", err)
	}

	return job, nil
}

// buildBackupContainer constructs the backup-agent container and associated volumes
// for a given backup destination and cluster. This is shared between Job and CronJob creation.
func buildBackupContainer(
	backupName string,
	destination etcdaenixiov1alpha1.BackupDestination,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	operatorImage string,
) (corev1.Container, []corev1.Volume) {
	endpoints := buildEndpoints(cluster)

	envVars := []corev1.EnvVar{
		{Name: "ETCD_ENDPOINTS", Value: endpoints},
	}

	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	if cluster.IsClientSecurityEnabled() || cluster.IsServerSecurityEnabled() || cluster.IsServerTrustedCADefined() {
		envVars = append(envVars, corev1.EnvVar{Name: "ETCD_TLS_ENABLED", Value: "true"})
	}

	if cluster.IsClientSecurityEnabled() {
		envVars = append(envVars,
			corev1.EnvVar{Name: "ETCD_TLS_CERT_PATH", Value: "/etc/etcd/pki/client/cert/tls.crt"},
			corev1.EnvVar{Name: "ETCD_TLS_KEY_PATH", Value: "/etc/etcd/pki/client/cert/tls.key"},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "client-certificate",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cluster.Spec.Security.TLS.ClientSecret,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "client-certificate",
			ReadOnly:  true,
			MountPath: "/etc/etcd/pki/client/cert",
		})
	}

	if cluster.IsServerTrustedCADefined() {
		envVars = append(envVars,
			corev1.EnvVar{Name: "ETCD_TLS_CA_PATH", Value: "/etc/etcd/pki/server/ca/ca.crt"},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "server-trusted-ca-certificate",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cluster.Spec.Security.TLS.ServerTrustedCASecret,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "server-trusted-ca-certificate",
			ReadOnly:  true,
			MountPath: "/etc/etcd/pki/server/ca",
		})
	}

	if s3 := destination.S3; s3 != nil {
		forcePathStyle := "false"
		if s3.ForcePathStyle {
			forcePathStyle = "true"
		}
		envVars = append(envVars,
			corev1.EnvVar{Name: "BACKUP_DESTINATION", Value: "s3"},
			corev1.EnvVar{Name: "S3_ENDPOINT", Value: s3.Endpoint},
			corev1.EnvVar{Name: "S3_BUCKET", Value: s3.Bucket},
			corev1.EnvVar{Name: "S3_KEY", Value: s3.Key},
			corev1.EnvVar{Name: "S3_REGION", Value: s3.Region},
			corev1.EnvVar{Name: "S3_FORCE_PATH_STYLE", Value: forcePathStyle},
			corev1.EnvVar{
				Name: "AWS_ACCESS_KEY_ID",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: s3.CredentialsSecretRef.Name},
						Key:                  "AWS_ACCESS_KEY_ID",
					},
				},
			},
			corev1.EnvVar{
				Name: "AWS_SECRET_ACCESS_KEY",
				ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{
						LocalObjectReference: corev1.LocalObjectReference{Name: s3.CredentialsSecretRef.Name},
						Key:                  "AWS_SECRET_ACCESS_KEY",
					},
				},
			},
		)
	}

	if pvc := destination.PVC; pvc != nil {
		backupPath := fmt.Sprintf("/backup/data/%s.db", backupName)
		if pvc.SubPath != "" {
			backupPath = fmt.Sprintf("/backup/data/%s", pvc.SubPath)
		}
		envVars = append(envVars,
			corev1.EnvVar{Name: "BACKUP_DESTINATION", Value: "pvc"},
			corev1.EnvVar{Name: "PVC_BACKUP_PATH", Value: backupPath},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "backup-data",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.ClaimName,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "backup-data",
			MountPath: "/backup/data",
		})
	}

	container := corev1.Container{
		Name:         "backup-agent",
		Image:        operatorImage,
		Command:      []string{"/backup-agent"},
		Env:          envVars,
		VolumeMounts: volumeMounts,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
	}

	return container, volumes
}

func buildEndpoints(cluster *etcdaenixiov1alpha1.EtcdCluster) string {
	protocol := GetServerProtocol(cluster)
	headlessSvc := GetHeadlessServiceName(cluster)
	replicas := 1
	if cluster.Spec.Replicas != nil {
		replicas = int(*cluster.Spec.Replicas)
	}
	eps := make([]string, 0, replicas)
	for i := 0; i < replicas; i++ {
		eps = append(eps, fmt.Sprintf("%s%s-%d.%s.%s.svc:2379",
			protocol, cluster.Name, i, headlessSvc, cluster.Namespace))
	}
	return strings.Join(eps, ",")
}
