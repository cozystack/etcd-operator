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
	"fmt"
	"strconv"
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

const (
	stringTrue = "true"
	backupData = "backup-data"

	// defaultEtcdQuotaBytes is etcd's default backend quota (2 GiB).
	defaultEtcdQuotaBytes int64 = 2 * 1024 * 1024 * 1024
)

// ErrInvalidSpec is the sentinel wrap for user-input validation
// failures surfaced by CreateBackupJob / CreateBackupCronJob (and
// any future builder in this package). The wrap lets controllers
// distinguish "this EtcdBackup/EtcdBackupSchedule will NEVER
// succeed without user action" from generic transient build
// failures (apiserver hiccup, controller-reference fault) — the
// former must be reported as a terminal Failed status condition
// so the user can see and fix it; the latter must be retried by
// the workqueue. Callers branch with `errors.Is(err,
// factory.ErrInvalidSpec)`. Use `fmt.Errorf("%w: …", ErrInvalidSpec, …)`
// to wrap at the validation boundary so the underlying message is
// preserved for the Failed condition's user-facing Message field.
var ErrInvalidSpec = errors.New("invalid backup spec")

// getEffectiveDBQuota returns the maximum etcd DB size for the given cluster,
// used to set ephemeral-storage requests/limits on backup Jobs.
// It checks, in order: explicit quota-backend-bytes option, the full cluster
// storage size, and falls back to etcd's default 2 GiB quota.
func getEffectiveDBQuota(cluster *etcdaenixiov1alpha1.EtcdCluster) resource.Quantity {
	// we'll use an explicit quota-backend-bytes option if set.
	if v, ok := cluster.Spec.Options["quota-backend-bytes"]; ok && v != "" {
		if parsed, err := strconv.ParseInt(v, 10, 64); err == nil && parsed > 0 {
			return *resource.NewQuantity(parsed, resource.BinarySI)
		}
	}

	// if a quota-backend-bytes option is not set, use the full cluster storage size
	if cluster.Spec.Storage.EmptyDir != nil {
		if cluster.Spec.Storage.EmptyDir.SizeLimit != nil {
			return *cluster.Spec.Storage.EmptyDir.SizeLimit
		}
	} else if req := cluster.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests.Storage(); req != nil {
		return *req
	}

	// otherwise we'll fall back to etcd's default quota.
	return *resource.NewQuantity(defaultEtcdQuotaBytes, resource.BinarySI)
}

// validatePVCSubPath rejects user-supplied SubPath values that would
// let the agent's /backup/data/<subPath>/<file>.db join escape the
// in-container backup mount, OR produce a URI that does not match
// the actual on-disk path the agent writes to. The agent runs as
// non-root inside its own pod (no other writeable host paths) so
// escape is contained to the PVC, but a URI we publish to
// status.snapshot.uri that disagrees with where the file actually
// lands misleads out-of-band tooling and any future RestoreSpec
// consumer.
//
// All rejections are wrapped with ErrInvalidSpec so the controller
// can surface them as a terminal Failed status condition (these are
// user-input errors that no amount of reconciler retry can fix).
//
// Rules:
//   - empty → allowed (the "no sub-path" case).
//   - leading "/" → absolute paths are not sub-paths; reject.
//   - any segment == ".." → escapes the mount; reject.
//   - any segment == "." → does not escape, but the agent's path
//     concatenation preserves the literal "." while the on-disk
//     join collapses it, so the published URI disagrees with the
//     filesystem reality. Reject.
//   - empty segment (consecutive or leading/trailing slash) → would
//     produce a malformed agent path; reject.
//   - backslash → forbidden so a future agent build on a filesystem
//     that honors `\` as a separator cannot be tricked.
//   - any C0 control byte (0x00–0x1f, including LF/CR/TAB) or DEL
//     (0x7f) → forbidden. NUL is the C-string-truncation footgun;
//     LF/CR would split the agent's terminal-marker line that the
//     controller parses (breaking the regex's `^…` anchor and the
//     marker's "one line wins" contract); the rest are shell-active
//     in downstream tooling that may consume the published URI.
//     Easier to forbid the whole C0 + DEL range than to enumerate.
func validatePVCSubPath(subPath string) error {
	if subPath == "" {
		return nil
	}
	if strings.HasPrefix(subPath, "/") {
		return fmt.Errorf("%w: pvc.subPath %q must not be absolute", ErrInvalidSpec, subPath)
	}
	if strings.ContainsRune(subPath, '\\') {
		return fmt.Errorf("%w: pvc.subPath %q must not contain backslashes", ErrInvalidSpec, subPath)
	}
	if i := strings.IndexFunc(subPath, func(r rune) bool {
		return r <= 0x1f || r == 0x7f
	}); i >= 0 {
		return fmt.Errorf("%w: pvc.subPath %q must not contain control characters (offset %d)", ErrInvalidSpec, subPath, i)
	}
	for _, seg := range strings.Split(subPath, "/") {
		if seg == "" {
			return fmt.Errorf("%w: pvc.subPath %q must not contain empty path segments", ErrInvalidSpec, subPath)
		}
		if seg == ".." || seg == "." {
			return fmt.Errorf("%w: pvc.subPath %q must not contain '.' or '..' segments", ErrInvalidSpec, subPath)
		}
	}
	return nil
}

// CreateBackupJob builds a Job that runs the backup-agent to take an etcd snapshot
// and store it to the configured destination.
func CreateBackupJob(
	backup *etcdaenixiov1alpha1.EtcdBackup,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	operatorImage string,
	scheme *runtime.Scheme,
) (*batchv1.Job, error) {
	if pvc := backup.Spec.Destination.PVC; pvc != nil {
		if err := validatePVCSubPath(pvc.SubPath); err != nil {
			return nil, err
		}
	}
	labels := NewLabelsBuilder().WithName().WithInstance(cluster.Name).WithManagedBy()
	labels["etcd.aenix.io/etcdbackup-name"] = backup.Name

	var backoffLimit int32
	var ttl int32 = 600
	var activeDeadline int64 = 900 // 15 minutes; safety net if backup-agent hangs
	container, volumes := buildBackupContainer(backup.Name, backup.Spec.Destination, cluster, operatorImage)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: backup.Name + "-",
			Namespace:    backup.Namespace,
			Labels:       labels,
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
					Containers: []corev1.Container{container},
					Volumes:    volumes,
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
		envVars = append(envVars, corev1.EnvVar{Name: "ETCD_TLS_ENABLED", Value: stringTrue})
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
			forcePathStyle = stringTrue
		}
		s3Key := fmt.Sprintf("%s.db", backupName)
		if s3.Key != "" {
			s3Key = fmt.Sprintf("%s/%s.db", strings.TrimRight(s3.Key, "/"), backupName)
		}
		envVars = append(envVars,
			corev1.EnvVar{Name: "BACKUP_DESTINATION", Value: "s3"},
			corev1.EnvVar{Name: "S3_ENDPOINT", Value: s3.Endpoint},
			corev1.EnvVar{Name: "S3_BUCKET", Value: s3.Bucket},
			corev1.EnvVar{Name: "S3_KEY", Value: s3Key},
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
			backupPath = fmt.Sprintf("/backup/data/%s/%s.db", pvc.SubPath, backupName)
		}
		envVars = append(envVars,
			corev1.EnvVar{Name: "BACKUP_DESTINATION", Value: "pvc"},
			corev1.EnvVar{Name: "PVC_BACKUP_PATH", Value: backupPath},
		)
		volumes = append(volumes, corev1.Volume{
			Name: backupData,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.ClaimName,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      backupData,
			MountPath: "/backup/data",
		})
	}

	envVars = append(envVars, corev1.EnvVar{Name: "BACKUP_INCLUDE_REVISION", Value: stringTrue})

	ephemeralStorage := getEffectiveDBQuota(cluster)
	container := corev1.Container{
		Name: "backup-agent",
		// Pin pull policy to IfNotPresent so the backup-agent
		// container runs the SAME image bytes the manager pod
		// runs — i.e. whatever was loaded onto the node at
		// install/upgrade time, mirroring the manager pod's own
		// imagePullPolicy in config/manager/manager.yaml. Without
		// this, kubernetes defaults to "Always" whenever the image
		// tag is "latest" (or absent), which then short-circuits
		// the node's local image and re-pulls from the registry
		// — silently substituting the running operator's binary
		// with whatever the registry currently serves under that
		// tag. That divergence is fatal for the marker parser: a
		// pre-marker agent build emits "snapshot written
		// successfully (N bytes)" which the controller's terminal-
		// marker regex does not recognise, leading to
		// status.snapshot=nil for a backup that otherwise
		// completed cleanly. It is also load-bearing for e2e: kind
		// loads the freshly-built image onto its nodes, and the
		// test expects the agent to be the just-built binary; an
		// implicit "Always" turns the test into a check of the
		// last-published "latest" tag instead.
		ImagePullPolicy: corev1.PullIfNotPresent,
		Image:           operatorImage,
		Command:         []string{"/backup-agent"},
		Env:             envVars,
		VolumeMounts:    volumeMounts,
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse("100m"),
				corev1.ResourceMemory:           resource.MustParse("128Mi"),
				corev1.ResourceEphemeralStorage: ephemeralStorage,
			},
			Limits: corev1.ResourceList{
				corev1.ResourceMemory:           resource.MustParse("512Mi"),
				corev1.ResourceEphemeralStorage: ephemeralStorage,
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
