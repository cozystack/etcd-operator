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

func GetStatefulSet(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) (*appsv1.StatefulSet, error) {
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

	basePodSpec := corev1.PodSpec{
		Containers: []corev1.Container{generateContainer(cluster)},
		Volumes:    volumes,
	}
	if cluster.Spec.PodTemplate.Spec.Containers == nil {
		cluster.Spec.PodTemplate.Spec.Containers = make([]corev1.Container, 0)
	}
	finalPodSpec, err := k8sutils.StrategicMerge(basePodSpec, cluster.Spec.PodTemplate.Spec)
	if err != nil {
		return nil, fmt.Errorf("cannot strategic-merge base podspec with podTemplate.spec: %w", err)
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
				//should we get labels from pod template because user can override base ones?
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
		return nil, err
	}
	log.Debug(ctx, "statefulset spec generated", "spec", statefulSet.Spec)

	if err = ctrl.SetControllerReference(cluster, statefulSet, rclient.Scheme()); err != nil {
		return nil, fmt.Errorf("cannot set controller reference: %w", err)
	}

	return statefulSet, nil
}

func PodLabels(cluster *etcdaenixiov1alpha1.EtcdCluster) map[string]string {
	labels := NewLabelsBuilder().WithName().WithInstance(cluster.Name).WithManagedBy()

	if cluster.Spec.PodTemplate.Labels != nil {
		//is it valid that user labels can override base ones?
		for key, value := range cluster.Spec.PodTemplate.Labels {
			labels[key] = value
		}
	}

	return labels
}

func GenerateEtcdCommand() []string {
	return []string{
		"etcd",
	}
}

func GenerateEtcdArgs(cluster *etcdaenixiov1alpha1.EtcdCluster) []string {
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

func GetServerProtocol(cluster *etcdaenixiov1alpha1.EtcdCluster) string {
	serverProtocol := "http://"
	if cluster.IsServerSecurityEnabled() {
		serverProtocol = "https://"
	}
	return serverProtocol
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
	c.Command = GenerateEtcdCommand()
	c.Args = GenerateEtcdArgs(cluster)
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
