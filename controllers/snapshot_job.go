/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// agentResources sets modest requests/limits for the BACKUP agent container.
// Without requests it runs BestEffort (first to be evicted under memory
// pressure) and is rejected outright where a LimitRange demands requests. The
// snapshot agent only streams the snapshot to its destination, so a 256Mi ceiling
// is ample. No CPU limit, so streaming a large snapshot isn't throttled.
//
// The RESTORE init container uses restoreAgentResources instead — see why there.
func agentResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
}

// restoreAgentResources sets the resources for the restore init container. It
// keeps the same requests as agentResources (so the container is Burstable, not
// BestEffort — it gates bootstrap and must not be first to evict) but sets NO
// memory limit. Unlike snapshot's streaming copy, etcdutl snapshot.Restore
// rebuilds a bbolt database whose working set scales with the snapshot/keyspace
// size; a fixed low ceiling (the snapshot agent's 256Mi) would get a large
// restore OOM-killed, bricking bootstrap with an opaque OOMKilled — the exact
// failure class the restore path is built to avoid. Leaving the limit unset
// lets the rebuild use up to node capacity.
func restoreAgentResources() corev1.ResourceRequirements {
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("50m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
	}
}

const (
	snapshotJobTTLSeconds int32 = 600
	snapshotJobBackoffLim int32 = 3
	// snapshotJobActiveDeadlineSeconds bounds a single snapshot Pod's wall-clock
	// run. BackoffLimit only counts Pods that EXIT non-zero; a Pod that HANGS —
	// e.g. the agent dialing a parked (replicas:0) cluster or a client Service
	// with no ready endpoints, where the clientv3 Snapshot RPC blocks — never
	// increments it, so without this the Job (and the EtcdSnapshot, stuck in
	// Started) would requeue forever. On deadline the kubelet kills the Pod and
	// the Job gets a Failed/DeadlineExceeded condition, which jobFailed() already
	// turns into a terminal EtcdSnapshot failure. 30 min is generous for a large
	// snapshot + upload while still bounding a true hang.
	snapshotJobActiveDeadlineSeconds int64 = 1800
	snapshotCAMountPath                    = "/etc/etcd/pki/ca"
	snapshotClientMountPath                = "/etc/etcd/pki/client"
	snapshotPVCMountPath                   = "/snapshot/data"
)

// snapshotJobName is the deterministic, owned Job name for an EtcdSnapshot.
func snapshotJobName(snapshot *lll.EtcdSnapshot) string {
	return snapshot.Name + "-snapshot"
}

// clientServiceEndpoint returns the stable client-service URL the agent dials.
func clientServiceEndpoint(cluster *lll.EtcdCluster) string {
	return fmt.Sprintf("%s://%s-client.%s.svc:2379",
		clusterClientScheme(cluster), cluster.Name, cluster.Namespace)
}

// buildSnapshotJob constructs the snapshot Job: the operator image run as
// `manager snapshot-agent`, configured entirely via env. operatorImage is the
// operator's own image (the agent lives in the same binary).
func buildSnapshotJob(snapshot *lll.EtcdSnapshot, cluster *lll.EtcdCluster, operatorImage string) *batchv1.Job {
	dest := snapshot.Spec.Destination

	env := []corev1.EnvVar{
		{Name: "ETCD_ENDPOINTS", Value: clientServiceEndpoint(cluster)},
		{Name: "SNAPSHOT_NAME", Value: snapshot.Name},
		// Stamped onto the S3 object so a Job retry recognizes its own prior
		// upload instead of failing the overwrite guard.
		{Name: "SNAPSHOT_UID", Value: string(snapshot.UID)},
	}

	var volumes []corev1.Volume
	var mounts []corev1.VolumeMount

	// TLS: mount the server CA (verify) and, on mTLS, the operator-client cert.
	if scheme := clusterClientScheme(cluster); scheme == "https" {
		if name := serverSecretName(cluster); name != "" {
			volumes = append(volumes, corev1.Volume{
				Name:         "etcd-ca",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: name}},
			})
			mounts = append(mounts, corev1.VolumeMount{Name: "etcd-ca", MountPath: snapshotCAMountPath, ReadOnly: true})
			env = append(env, corev1.EnvVar{Name: "ETCD_TLS_CA_PATH", Value: snapshotCAMountPath + "/ca.crt"})
		}
		if name := operatorClientSecretName(cluster); name != "" {
			volumes = append(volumes, corev1.Volume{
				Name:         "etcd-client",
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: name}},
			})
			mounts = append(mounts, corev1.VolumeMount{Name: "etcd-client", MountPath: snapshotClientMountPath, ReadOnly: true})
			env = append(env,
				corev1.EnvVar{Name: "ETCD_TLS_CERT_PATH", Value: snapshotClientMountPath + "/tls.crt"},
				corev1.EnvVar{Name: "ETCD_TLS_KEY_PATH", Value: snapshotClientMountPath + "/tls.key"},
			)
		}
	}

	// Auth: once auth is enabled, dial as root with the password from the
	// cluster's root-credentials Secret (injected via env, not read here).
	if cluster.Status.AuthEnabled && cluster.Spec.Auth != nil && cluster.Spec.Auth.RootCredentialsSecretRef != nil {
		env = append(env,
			corev1.EnvVar{Name: "ETCD_USERNAME", Value: "root"},
			corev1.EnvVar{Name: "ETCD_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: *cluster.Spec.Auth.RootCredentialsSecretRef,
				Key:                  corev1.BasicAuthPasswordKey,
			}}},
		)
	}

	// Destination.
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

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      snapshotJobName(snapshot),
			Namespace: snapshot.Namespace,
			Labels:    map[string]string{LabelCluster: cluster.Name},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            &backoff,
			TTLSecondsAfterFinished: &ttl,
			ActiveDeadlineSeconds:   &activeDeadline,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{LabelCluster: cluster.Name}},
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
						Image:   operatorImage,
						Command: []string{"/manager", "snapshot-agent"},
						Env:     env,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: ptrBool(false),
							Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
						},
						VolumeMounts: mounts,
						Resources:    agentResources(),
					}},
					Volumes: volumes,
				},
			},
		},
	}
}
