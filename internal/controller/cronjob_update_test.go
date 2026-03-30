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

package controller

import (
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func baseCronJob() *batchv1.CronJob {
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{"app": "backup"},
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                   "0 */6 * * *",
			SuccessfulJobsHistoryLimit: new(int32(3)),
			FailedJobsHistoryLimit:     new(int32(1)),
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:    "backup-agent",
									Image:   "operator:v1",
									Command: []string{"/backup-agent"},
									Env: []corev1.EnvVar{
										{Name: "BACKUP_DESTINATION", Value: "s3"},
									},
									VolumeMounts: []corev1.VolumeMount{
										{Name: "data", MountPath: "/data"},
									},
								},
							},
							Volumes: []corev1.Volume{
								{
									Name: "data",
									VolumeSource: corev1.VolumeSource{
										EmptyDir: &corev1.EmptyDirVolumeSource{},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestCronJobNeedsUpdate(t *testing.T) {
	tests := []struct {
		name     string
		mutate   func(cj *batchv1.CronJob)
		expected bool
	}{
		{
			name:     "identical CronJobs",
			mutate:   func(_ *batchv1.CronJob) {},
			expected: false,
		},
		{
			name: "schedule changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.Schedule = "0 */12 * * *"
			},
			expected: true,
		},
		{
			name: "successful history limit changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.SuccessfulJobsHistoryLimit = new(int32(5))
			},
			expected: true,
		},
		{
			name: "failed history limit changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.FailedJobsHistoryLimit = new(int32(3))
			},
			expected: true,
		},
		{
			name: "label changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Labels["new-label"] = "value"
			},
			expected: true,
		},
		{
			name: "container image changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image = "operator:v2"
			},
			expected: true,
		},
		{
			name: "container env changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env = append(
					cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Env,
					corev1.EnvVar{Name: "NEW_VAR", Value: "val"},
				)
			},
			expected: true,
		},
		{
			name: "container command changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Command = []string{"/new-agent"}
			},
			expected: true,
		},
		{
			name: "volume mount changed",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts = append(
					cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].VolumeMounts,
					corev1.VolumeMount{Name: "extra", MountPath: "/extra"},
				)
			},
			expected: true,
		},
		{
			name: "volume added",
			mutate: func(cj *batchv1.CronJob) {
				cj.Spec.JobTemplate.Spec.Template.Spec.Volumes = append(
					cj.Spec.JobTemplate.Spec.Template.Spec.Volumes,
					corev1.Volume{Name: "extra"},
				)
			},
			expected: true,
		},
		{
			name: "API server defaults do not trigger update",
			mutate: func(cj *batchv1.CronJob) {
				// Simulate API server adding defaults to the existing CronJob
				cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].TerminationMessagePath = "/dev/termination-log"
				cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].TerminationMessagePolicy = corev1.TerminationMessageReadFile
				cj.Spec.JobTemplate.Spec.Template.Spec.Containers[0].ImagePullPolicy = corev1.PullIfNotPresent
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			existing := baseCronJob()
			tt.mutate(existing)
			desired := baseCronJob()

			got := cronJobNeedsUpdate(existing, desired)
			if got != tt.expected {
				t.Errorf("cronJobNeedsUpdate() = %v, want %v", got, tt.expected)
			}
		})
	}
}
