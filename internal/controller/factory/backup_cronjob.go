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

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
)

// CreateBackupCronJob builds a CronJob that runs the backup-agent on a schedule.
func CreateBackupCronJob(
	schedule *etcdaenixiov1alpha1.EtcdBackupSchedule,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	operatorImage string,
	scheme *runtime.Scheme,
) (*batchv1.CronJob, error) {
	labels := NewLabelsBuilder().WithName().WithInstance(cluster.Name).WithManagedBy()
	labels["etcd.aenix.io/etcdbackupschedule-name"] = schedule.Name

	container, volumes := buildBackupContainer(schedule.Name, schedule.Spec.Destination, cluster, operatorImage)
	// Enable timestamp injection so each scheduled backup gets a unique filename.
	container.Env = append(container.Env, corev1.EnvVar{Name: "BACKUP_TIMESTAMP", Value: "true"})

	var backoffLimit int32
	var ttl int32 = 600
	var activeDeadline int64 = 900 // 15 minutes; safety net if backup-agent hangs
	cronJob := &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: schedule.Name + "-scheduled-backup-",
			Namespace:    schedule.Namespace,
			Labels:       labels,
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   schedule.Spec.Schedule,
			ConcurrencyPolicy:          batchv1.ForbidConcurrent,
			SuccessfulJobsHistoryLimit: schedule.Spec.SuccessfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     schedule.Spec.FailedJobsHistoryLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
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
			},
		},
	}

	if err := ctrl.SetControllerReference(schedule, cronJob, scheme); err != nil {
		return nil, fmt.Errorf("cannot set controller reference: %w", err)
	}

	return cronJob, nil
}
