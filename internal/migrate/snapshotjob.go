/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package migrate

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

// Mount paths inside the snapshot Job, mirroring the operator's own
// snapshot-job layout (controllers/snapshot_job.go).
const (
	snapshotCAMountPath     = "/etc/etcd/pki/ca"
	snapshotClientMountPath = "/etc/etcd/pki/client"
	snapshotPVCMountPath    = "/snapshot/data"

	snapshotJobTTLSeconds            int32 = 600
	snapshotJobBackoffLim            int32 = 3
	snapshotJobActiveDeadlineSeconds int64 = 1800
)

// SnapshotName is the SNAPSHOT_NAME the migration Job stores the artifact
// under. Namespaced so clusters sharing one S3 bucket/prefix don't collide.
func SnapshotName(namespace, cluster string) string {
	return namespace + "-" + cluster + "-migration"
}

// SnapshotJobName names the one-off Job the tool creates per cluster.
func SnapshotJobName(cluster string) string {
	return cluster + "-migration-snapshot"
}

// LegacyClientEndpoint is the legacy operator's client Service URL — the
// Service is named after the cluster (the new operator's "-client" suffix
// does not exist yet at snapshot time). https iff the legacy cluster serves
// TLS on the client port.
func LegacyClientEndpoint(name, namespace string, spec legacy.EtcdClusterSpec) string {
	scheme := "http"
	if spec.Security != nil && spec.Security.TLS.ServerSecret != "" {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s.%s.svc:2379", scheme, name, namespace)
}

// BuildSnapshotJob constructs the one-off Job that snapshots a LEGACY
// cluster with the new operator's snapshot agent (the agent is env-driven
// and needs no Kubernetes API access, so it works with both controllers
// stopped). Mirrors the operator's buildSnapshotJob with two deltas:
//
//   - endpoints point at the legacy client Service;
//   - the TLS material comes from the legacy secret layout, where the CA
//     lives in a SEPARATE secret (serverTrustedCASecret) and the client
//     identity is the legacy operator's clientSecret.
//
// No ETCD_USERNAME/PASSWORD: the legacy root user is NoPassword (cert-only),
// and when the cluster had auth enabled the tool disables it before this Job
// runs, so the dial is anonymous either way.
func BuildSnapshotJob(name, namespace, clusterUID string, spec legacy.EtcdClusterSpec, dest lll.SnapshotLocation, agentImage string) *batchv1.Job {
	env := []corev1.EnvVar{
		{Name: "ETCD_ENDPOINTS", Value: LegacyClientEndpoint(name, namespace, spec)},
		{Name: "SNAPSHOT_NAME", Value: SnapshotName(namespace, name)},
		// Stamped onto the S3 object so a re-run recognizes its own prior
		// upload instead of failing the agent's overwrite guard.
		{Name: "SNAPSHOT_UID", Value: clusterUID},
	}

	var volumes []corev1.Volume
	var mounts []corev1.VolumeMount

	if spec.Security != nil && spec.Security.TLS.ServerSecret != "" {
		t := spec.Security.TLS
		// Legacy keeps the client-plane CA in serverTrustedCASecret; fall
		// back to the server secret's own ca.crt when no separate CA secret
		// is set (the post-merge layout the migration asks users for).
		caSecret := t.ServerTrustedCASecret
		if caSecret == "" {
			caSecret = t.ServerSecret
		}
		volumes = append(volumes, corev1.Volume{
			Name:         "etcd-ca",
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: caSecret}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "etcd-ca", MountPath: snapshotCAMountPath, ReadOnly: true})
		env = append(env, corev1.EnvVar{Name: "ETCD_TLS_CA_PATH", Value: snapshotCAMountPath + "/ca.crt"})

		if t.ClientSecret != "" {
			volumes = append(volumes, corev1.Volume{
				Name:         "etcd-client",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: t.ClientSecret}},
			})
			mounts = append(mounts, corev1.VolumeMount{Name: "etcd-client", MountPath: snapshotClientMountPath, ReadOnly: true})
			env = append(env,
				corev1.EnvVar{Name: "ETCD_TLS_CERT_PATH", Value: snapshotClientMountPath + "/tls.crt"},
				corev1.EnvVar{Name: "ETCD_TLS_KEY_PATH", Value: snapshotClientMountPath + "/tls.key"},
			)
		}
	}

	switch {
	case dest.S3 != nil:
		s3 := dest.S3
		env = append(env,
			corev1.EnvVar{Name: "SNAPSHOT_DEST_KIND", Value: "s3"},
			corev1.EnvVar{Name: "S3_ENDPOINT", Value: s3.Endpoint},
			corev1.EnvVar{Name: "S3_BUCKET", Value: s3.Bucket},
			corev1.EnvVar{Name: "S3_KEY", Value: s3.Key},
			corev1.EnvVar{Name: "S3_REGION", Value: s3.Region},
			corev1.EnvVar{Name: "S3_FORCE_PATH_STYLE", Value: fmt.Sprintf("%t", s3.ForcePathStyle)},
			corev1.EnvVar{Name: "AWS_ACCESS_KEY_ID", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: s3.CredentialsSecretRef, Key: "AWS_ACCESS_KEY_ID",
			}}},
			corev1.EnvVar{Name: "AWS_SECRET_ACCESS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: s3.CredentialsSecretRef, Key: "AWS_SECRET_ACCESS_KEY",
			}}},
		)
	case dest.PVC != nil:
		env = append(env,
			corev1.EnvVar{Name: "SNAPSHOT_DEST_KIND", Value: "pvc"},
			corev1.EnvVar{Name: "PVC_MOUNT_PATH", Value: snapshotPVCMountPath},
			corev1.EnvVar{Name: "PVC_SUBPATH", Value: dest.PVC.SubPath},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "snapshot-data",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: dest.PVC.ClaimName,
			}},
		})
		mounts = append(mounts, corev1.VolumeMount{Name: "snapshot-data", MountPath: snapshotPVCMountPath})
	}

	ttl := snapshotJobTTLSeconds
	backoff := snapshotJobBackoffLim
	activeDeadline := snapshotJobActiveDeadlineSeconds
	notRoot := true
	user := int64(65532)
	noAutomount := false
	noEscalation := false

	return &batchv1.Job{
		TypeMeta: metav1.TypeMeta{APIVersion: "batch/v1", Kind: "Job"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      SnapshotJobName(name),
			Namespace: namespace,
			Labels:    map[string]string{"app.kubernetes.io/created-by": "etcd-migrate"},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   &activeDeadline,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					AutomountServiceAccountToken: &noAutomount,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot:   &notRoot,
						RunAsUser:      &user,
						RunAsGroup:     &user,
						FSGroup:        &user,
						SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
					},
					Containers: []corev1.Container{{
						Name:    "snapshot-agent",
						Image:   agentImage,
						Command: []string{"/manager", "snapshot-agent"},
						Env:     env,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: &noEscalation,
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: mounts,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("50m"),
								corev1.ResourceMemory: resource.MustParse("64Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
					}},
					Volumes: volumes,
				},
			},
		},
	}
}
