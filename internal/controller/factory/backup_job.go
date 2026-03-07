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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	endpoints := buildEndpoints(cluster)

	envVars := []corev1.EnvVar{
		{Name: "ETCD_ENDPOINTS", Value: endpoints},
	}

	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount

	if cluster.IsClientSecurityEnabled() || cluster.IsServerSecurityEnabled() {
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

	if cluster.IsServerSecurityEnabled() {
		// ServerSecret contains ca.crt alongside tls.crt and tls.key.
		// This matches the convention in etcd_client.go (configFromCluster)
		// where Root CA is extracted from ServerSecret via parseTLSSecretCA.
		envVars = append(envVars,
			corev1.EnvVar{Name: "ETCD_TLS_CA_PATH", Value: "/etc/etcd/pki/server/cert/ca.crt"},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "server-certificate",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: cluster.Spec.Security.TLS.ServerSecret,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "server-certificate",
			ReadOnly:  true,
			MountPath: "/etc/etcd/pki/server/cert",
		})
	}

	if s3 := backup.Spec.Destination.S3; s3 != nil {
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

	if pvc := backup.Spec.Destination.PVC; pvc != nil {
		backupPath := fmt.Sprintf("/backup/data/%s.db", backup.Name)
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

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      GetBackupJobName(backup),
			Namespace: backup.Namespace,
			Labels:    labels,
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:         "backup-agent",
							Image:        operatorImage,
							Command:      []string{"/backup-agent"},
							Env:          envVars,
							VolumeMounts: volumeMounts,
						},
					},
					Volumes: volumes,
				},
			},
		},
	}

	if err := ctrl.SetControllerReference(backup, job, scheme); err != nil {
		return nil, fmt.Errorf("cannot set controller reference: %w", err)
	}

	return job, nil
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
