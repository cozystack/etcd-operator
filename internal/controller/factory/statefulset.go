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
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/k8sutils"
	"github.com/aenix-io/etcd-operator/internal/log"
)

const (
	etcdContainerName                = "etcd"
	defaultBackendQuotaBytesFraction = 0.95
)

// TODO!
func TemplateStatefulSet() *appsv1.StatefulSet {
	panic("not yet implemented")
}

func PodLabels(cluster *etcdaenixiov1alpha1.EtcdCluster) map[string]string {
	labels := NewLabelsBuilder().WithName().WithInstance(cluster.Name).WithManagedBy()

	if cluster.Spec.PodTemplate.Labels != nil {
		for key, value := range cluster.Spec.PodTemplate.Labels {
			labels[key] = value
		}
	}

	return labels
}

func CreateOrUpdateStatefulSet(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
	operatorImage string,
) error {
	podMetadata := metav1.ObjectMeta{
		Labels: PodLabels(cluster),
	}

	if cluster.Spec.PodTemplate.Annotations != nil {
		podMetadata.Annotations = cluster.Spec.PodTemplate.Annotations
	}

	volumeClaimTemplates := make([]corev1.PersistentVolumeClaim, 0)
	if cluster.Spec.Storage.EmptyDir == nil {
		volumeClaimTemplates = append(volumeClaimTemplates, corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:        GetPVCName(cluster),
				Labels:      cluster.Spec.Storage.VolumeClaimTemplate.Labels,
				Annotations: cluster.Spec.Storage.VolumeClaimTemplate.Annotations,
			},
			Spec:   cluster.Spec.Storage.VolumeClaimTemplate.Spec,
			Status: cluster.Spec.Storage.VolumeClaimTemplate.Status,
		})
	}

	volumes := generateVolumes(cluster)

	var initContainers []corev1.Container
	if cluster.Spec.Bootstrap != nil && cluster.Spec.Bootstrap.Restore != nil {
		if operatorImage == "" {
			return fmt.Errorf("OPERATOR_IMAGE is required for bootstrap restore but not set")
		}
		restoreInitContainers, restoreVolumes, err := generateRestoreInitContainers(cluster, operatorImage)
		if err != nil {
			return err
		}
		initContainers = restoreInitContainers
		volumes = append(volumes, restoreVolumes...)
	}

	basePodSpec := corev1.PodSpec{
		InitContainers: initContainers,
		Containers:     []corev1.Container{generateContainer(cluster)},
		Volumes:        volumes,
	}
	// Restore-agent ships in the operator image (distroless/static:nonroot,
	// uid 65532) but writes into /var/run/etcd, which is the StatefulSet's
	// PVC. On a freshly-provisioned PVC the mountpoint belongs to root:root
	// 0755, so the agent fails with `mkdir /var/run/etcd/default.etcd:
	// permission denied`. FSGroup tells the kubelet to recursively chgrp
	// the mount to gid 65532 and add g+s, after which the nonroot agent
	// can create the data dir. Same pattern is already used for the
	// backup Job/CronJob pods in this package.
	if cluster.Spec.Bootstrap != nil && cluster.Spec.Bootstrap.Restore != nil {
		basePodSpec.SecurityContext = &corev1.PodSecurityContext{
			FSGroup: ptr.To(int64(65532)),
		}
	}
	if cluster.Spec.PodTemplate.Spec.Containers == nil {
		cluster.Spec.PodTemplate.Spec.Containers = make([]corev1.Container, 0)
	}
	finalPodSpec, err := k8sutils.StrategicMerge(basePodSpec, cluster.Spec.PodTemplate.Spec)
	if err != nil {
		return fmt.Errorf("cannot strategic-merge base podspec with podTemplate.spec: %w", err)
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: cluster.Namespace,
			Name:      cluster.Name,
		},
		Spec: appsv1.StatefulSetSpec{
			// initialize static fields that cannot be changed across updates.
			Replicas:            cluster.Spec.Replicas,
			ServiceName:         GetHeadlessServiceName(cluster),
			PodManagementPolicy: appsv1.ParallelPodManagement,
			Selector: &metav1.LabelSelector{
				MatchLabels: NewLabelsBuilder().WithName().WithInstance(cluster.Name).WithManagedBy(),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: podMetadata,
				Spec:       finalPodSpec,
			},
			VolumeClaimTemplates: volumeClaimTemplates,
		},
	}
	ctx, err = contextWithGVK(ctx, statefulSet, rclient.Scheme())
	if err != nil {
		return err
	}
	log.Debug(ctx, "statefulset spec generated", "spec", statefulSet.Spec)

	if err = ctrl.SetControllerReference(cluster, statefulSet, rclient.Scheme()); err != nil {
		return fmt.Errorf("cannot set controller reference: %w", err)
	}

	return reconcileOwnedResource(ctx, rclient, statefulSet)
}

func generateVolumes(cluster *etcdaenixiov1alpha1.EtcdCluster) []corev1.Volume {
	volumes := []corev1.Volume{}

	var dataVolumeSource corev1.VolumeSource

	if cluster.Spec.Storage.EmptyDir != nil {
		dataVolumeSource = corev1.VolumeSource{EmptyDir: cluster.Spec.Storage.EmptyDir}
	} else {
		dataVolumeSource = corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: GetPVCName(cluster),
			},
		}
	}

	volumes = append(
		volumes,

		corev1.Volume{
			Name:         "data",
			VolumeSource: dataVolumeSource,
		},
	)

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.PeerSecret != "" {
		volumes = append(volumes,
			[]corev1.Volume{
				{
					Name: "peer-trusted-ca-certificate",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: cluster.Spec.Security.TLS.PeerTrustedCASecret,
						},
					},
				},
				{
					Name: "peer-certificate",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: cluster.Spec.Security.TLS.PeerSecret,
						},
					},
				},
			}...)
	}

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.ServerSecret != "" {
		volumes = append(volumes,
			[]corev1.Volume{
				{
					Name: "server-certificate",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: cluster.Spec.Security.TLS.ServerSecret,
						},
					},
				},
			}...)
	}

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.ClientSecret != "" {
		volumes = append(volumes,
			[]corev1.Volume{
				{
					Name: "client-trusted-ca-certificate",
					VolumeSource: corev1.VolumeSource{
						Secret: &corev1.SecretVolumeSource{
							SecretName: cluster.Spec.Security.TLS.ClientTrustedCASecret,
						},
					},
				},
			}...)
	}

	return volumes

}

func generateVolumeMounts(cluster *etcdaenixiov1alpha1.EtcdCluster) []corev1.VolumeMount {

	volumeMounts := []corev1.VolumeMount{}

	volumeMounts = append(volumeMounts, corev1.VolumeMount{
		Name:      "data",
		ReadOnly:  false,
		MountPath: "/var/run/etcd",
	})

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.PeerSecret != "" {
		volumeMounts = append(volumeMounts, []corev1.VolumeMount{
			{
				Name:      "peer-trusted-ca-certificate",
				ReadOnly:  true,
				MountPath: "/etc/etcd/pki/peer/ca",
			},
			{
				Name:      "peer-certificate",
				ReadOnly:  true,
				MountPath: "/etc/etcd/pki/peer/cert",
			},
		}...)
	}

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.ServerSecret != "" {
		volumeMounts = append(volumeMounts, []corev1.VolumeMount{
			{
				Name:      "server-certificate",
				ReadOnly:  true,
				MountPath: "/etc/etcd/pki/server/cert",
			},
		}...)
	}

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.ClientSecret != "" {

		volumeMounts = append(volumeMounts, []corev1.VolumeMount{
			{
				Name:      "client-trusted-ca-certificate",
				ReadOnly:  true,
				MountPath: "/etc/etcd/pki/client/ca",
			},
		}...)
	}

	return volumeMounts
}

func generateEtcdCommand() []string {
	return []string{
		"etcd",
	}
}

func generateEtcdArgs(cluster *etcdaenixiov1alpha1.EtcdCluster) []string {
	args := []string{}

	peerTlsSettings := []string{"--peer-auto-tls"}

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.PeerSecret != "" {
		peerTlsSettings = []string{
			"--peer-trusted-ca-file=/etc/etcd/pki/peer/ca/ca.crt",
			"--peer-cert-file=/etc/etcd/pki/peer/cert/tls.crt",
			"--peer-key-file=/etc/etcd/pki/peer/cert/tls.key",
			"--peer-client-cert-auth",
		}
	}

	serverTlsSettings := []string{}

	if cluster.Spec.Security != nil && cluster.Spec.Security.TLS.ServerSecret != "" {
		serverTlsSettings = []string{
			"--cert-file=/etc/etcd/pki/server/cert/tls.crt",
			"--key-file=/etc/etcd/pki/server/cert/tls.key",
		}
	}

	clientTlsSettings := []string{}

	if cluster.IsClientSecurityEnabled() {
		clientTlsSettings = []string{
			"--trusted-ca-file=/etc/etcd/pki/client/ca/ca.crt",
			"--client-cert-auth",
		}
	}

	autoCompactionSettings := []string{
		"--auto-compaction-retention=5m",
		"--snapshot-count=10000",
	}

	args = append(args, []string{
		"--name=$(POD_NAME)",
		"--listen-metrics-urls=http://0.0.0.0:2381",
		"--listen-peer-urls=https://0.0.0.0:2380",
		fmt.Sprintf("--listen-client-urls=%s0.0.0.0:2379", GetServerProtocol(cluster)),
		fmt.Sprintf("--initial-advertise-peer-urls=https://$(POD_NAME).%s.$(POD_NAMESPACE).svc:2380", GetHeadlessServiceName(cluster)),
		"--data-dir=/var/run/etcd/default.etcd",
		fmt.Sprintf("--advertise-client-urls=%s$(POD_NAME).%s.$(POD_NAMESPACE).svc:2379", GetServerProtocol(cluster), GetHeadlessServiceName(cluster)),
	}...)

	args = append(args, peerTlsSettings...)
	args = append(args, serverTlsSettings...)
	args = append(args, clientTlsSettings...)
	args = append(args, autoCompactionSettings...)

	extraArgs := []string{}

	if value, ok := cluster.Spec.Options["quota-backend-bytes"]; !ok || value == "" {
		var size resource.Quantity
		if cluster.Spec.Storage.EmptyDir != nil {
			if cluster.Spec.Storage.EmptyDir.SizeLimit != nil {
				size = *cluster.Spec.Storage.EmptyDir.SizeLimit
			}
		} else {
			size = *cluster.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests.Storage()
		}
		quota := float64(size.Value()) * defaultBackendQuotaBytesFraction
		quota = math.Floor(quota)
		if quota > 0 {
			if cluster.Spec.Options == nil {
				cluster.Spec.Options = make(map[string]string, 1)
			}
			cluster.Spec.Options["quota-backend-bytes"] = strconv.FormatInt(int64(quota), 10)
		}
	}

	for name, value := range cluster.Spec.Options {
		flag := "--" + name
		if len(value) == 0 {
			extraArgs = append(extraArgs, flag)

			continue
		}

		extraArgs = append(extraArgs, fmt.Sprintf("%s=%s", flag, value))
	}

	// Sort the extra args to ensure a deterministic order
	slices.Sort(extraArgs)
	args = append(args, extraArgs...)

	return args
}

func generateContainer(cluster *etcdaenixiov1alpha1.EtcdCluster) corev1.Container {
	podEnv := []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
	}

	c := corev1.Container{}
	c.Name = etcdContainerName
	c.Image = etcdaenixiov1alpha1.DefaultEtcdImage
	c.Command = generateEtcdCommand()
	c.Args = generateEtcdArgs(cluster)
	c.Ports = []corev1.ContainerPort{
		{Name: "peer", ContainerPort: 2380},
		{Name: "client", ContainerPort: 2379},
	}
	clusterStateConfigMapName := GetClusterStateConfigMapName(cluster)
	c.EnvFrom = []corev1.EnvFromSource{
		{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: clusterStateConfigMapName,
				},
			},
		},
	}
	c.StartupProbe = getStartupProbe()
	c.LivenessProbe = getLivenessProbe()
	c.ReadinessProbe = getReadinessProbe()
	c.Env = podEnv
	c.VolumeMounts = generateVolumeMounts(cluster)

	return c
}

func getStartupProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/readyz?serializable=false",
				Port: intstr.FromInt32(2381),
			},
		},
		PeriodSeconds: 5,
	}
}

func getReadinessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/readyz",
				Port: intstr.FromInt32(2381),
			},
		},
		PeriodSeconds: 5,
	}
}

func getLivenessProbe() *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path: "/livez",
				Port: intstr.FromInt32(2381),
			},
		},
		PeriodSeconds: 5,
	}
}

func generateRestoreInitContainers(
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	operatorImage string,
) ([]corev1.Container, []corev1.Volume, error) {
	restore := cluster.Spec.Bootstrap.Restore
	headlessSvc := GetHeadlessServiceName(cluster)

	podEnv := []corev1.EnvVar{
		{
			Name: "POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
			},
		},
		{
			Name: "POD_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		},
	}

	// Env vars for the restore-agent init container. restore-agent now
	// runs etcdutl snapshot.Restore() in-process after downloading the
	// snapshot, so it consumes POD_NAME/POD_NAMESPACE and the
	// cluster-state values (ETCD_INITIAL_CLUSTER / token) that the
	// dropped restore-datadir container used to read.
	restoreAgentEnv := append([]corev1.EnvVar{}, podEnv...)
	restoreAgentEnv = append(restoreAgentEnv,
		corev1.EnvVar{Name: "ETCD_DATA_DIR", Value: "/var/run/etcd/default.etcd"},
		corev1.EnvVar{Name: "HEADLESS_SVC", Value: headlessSvc},
	)
	restoreAgentVolumeMounts := []corev1.VolumeMount{
		{Name: "data", MountPath: "/var/run/etcd"},
		{Name: "restore-data", MountPath: "/restore"},
	}

	var extraVolumes []corev1.Volume

	if s3 := restore.Source.S3; s3 != nil {
		forcePathStyle := "false"
		if s3.ForcePathStyle {
			forcePathStyle = "true"
		}
		restoreAgentEnv = append(restoreAgentEnv,
			corev1.EnvVar{Name: "RESTORE_SOURCE", Value: "s3"},
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

	if pvc := restore.Source.PVC; pvc != nil {
		// Mirror the backup-side hardening (see backup_job.go's
		// validatePVCSubPath): the restore SubPath is interpolated into
		// PVC_BACKUP_PATH that the agent feeds straight into os.Open.
		// The restore PVC is read-only and the value points back into
		// the same already-mounted volume, so the blast radius is
		// smaller than on the backup side — but a `..` or empty segment
		// still produces a path the agent cannot meaningfully open and
		// a status message the user cannot recover from in-place.
		// Apply the same rules; ErrInvalidSpec wrapping lets the
		// caller surface the failure as a terminal spec error.
		if err := validatePVCSubPath(pvc.SubPath); err != nil {
			return nil, nil, err
		}
		backupPath := "/backup/data/snapshot.db"
		if pvc.SubPath != "" {
			backupPath = fmt.Sprintf("/backup/data/%s", pvc.SubPath)
		}
		restoreAgentEnv = append(restoreAgentEnv,
			corev1.EnvVar{Name: "RESTORE_SOURCE", Value: "pvc"},
			corev1.EnvVar{Name: "PVC_BACKUP_PATH", Value: backupPath},
		)
		extraVolumes = append(extraVolumes, corev1.Volume{
			Name: "backup-source",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.ClaimName,
				},
			},
		})
		restoreAgentVolumeMounts = append(restoreAgentVolumeMounts, corev1.VolumeMount{
			Name:      "backup-source",
			ReadOnly:  true,
			MountPath: "/backup/data",
		})
	}

	// restore-data emptyDir scratch space for the downloaded snapshot
	extraVolumes = append(extraVolumes, corev1.Volume{
		Name:         "restore-data",
		VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
	})

	// Single init container: restore-agent. Downloads the snapshot and
	// runs etcdutl snapshot.Restore() in-process via the Go API. The
	// legacy restore-datadir initContainer ran `/bin/sh -c "etcdutl
	// snapshot restore ..."` from the upstream distroless etcd image,
	// which has no /bin/sh, so it crash-looped on every restore.
	clusterStateConfigMapName := GetClusterStateConfigMapName(cluster)
	restoreAgentContainer := corev1.Container{
		Name:    "restore-agent",
		Image:   operatorImage,
		Command: []string{"/restore-agent"},
		Env:     restoreAgentEnv,
		EnvFrom: []corev1.EnvFromSource{
			{
				ConfigMapRef: &corev1.ConfigMapEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: clusterStateConfigMapName,
					},
				},
			},
		},
		VolumeMounts: restoreAgentVolumeMounts,
		// Memory limit covers etcdutl snapshot.Restore() running
		// in-process: the restore materializes the entire on-disk
		// btree in memory while replaying the WAL, so the previous
		// 512Mi limit (sized for download-only) OOM-killed on
		// multi-GB snapshots that etcd reaches in practice. 2Gi
		// covers the typical operational ceiling; clusters with
		// larger snapshots should raise this via a future
		// EtcdCluster.spec.bootstrap.restore.resources override.
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("256Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("2Gi"),
			},
		},
	}

	return []corev1.Container{restoreAgentContainer}, extraVolumes, nil
}

func GetServerProtocol(cluster *etcdaenixiov1alpha1.EtcdCluster) string {
	serverProtocol := "http://"
	if cluster.IsServerSecurityEnabled() {
		serverProtocol = "https://"
	}
	return serverProtocol
}
