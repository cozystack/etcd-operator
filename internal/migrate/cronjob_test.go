/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package migrate

import (
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"

	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

// TestTranslateSchedule pins the schedule → CronJob generation: print-only
// action, schedule/limits mapping, the embedded `kubectl create` of an
// EtcdSnapshot with generateName, and the companion RBAC objects.
func TestTranslateSchedule(t *testing.T) {
	plan := TranslateSchedule("nightly", "ns", legacy.EtcdBackupScheduleSpec{
		ClusterRef:                 corev1.LocalObjectReference{Name: "my-etcd"},
		Schedule:                   "0 2 * * *",
		SuccessfulJobsHistoryLimit: ptrInt32(7),
		FailedJobsHistoryLimit:     ptrInt32(2),
		Destination: legacy.BackupDestination{PVC: &legacy.PVCBackupDestination{
			ClaimName: "backups", SubPath: "etcd",
		}},
	})

	if plan.Action != ActionPrint {
		t.Fatalf("Action = %s, want Print (schedules are never applied)", plan.Action)
	}
	if plan.DeleteRef != nil {
		t.Fatal("schedules must not be deleted: the replacement is not applied by the tool")
	}

	cj, ok := plan.Target.(*batchv1.CronJob)
	if !ok {
		t.Fatalf("Target is %T, want *CronJob", plan.Target)
	}
	if cj.Spec.Schedule != "0 2 * * *" {
		t.Errorf("schedule = %q", cj.Spec.Schedule)
	}
	if cj.Spec.SuccessfulJobsHistoryLimit == nil || *cj.Spec.SuccessfulJobsHistoryLimit != 7 ||
		cj.Spec.FailedJobsHistoryLimit == nil || *cj.Spec.FailedJobsHistoryLimit != 2 {
		t.Errorf("history limits = %v/%v", cj.Spec.SuccessfulJobsHistoryLimit, cj.Spec.FailedJobsHistoryLimit)
	}

	pod := cj.Spec.JobTemplate.Spec.Template.Spec
	if pod.ServiceAccountName != "nightly-snapshotter" {
		t.Errorf("serviceAccountName = %q", pod.ServiceAccountName)
	}
	if len(pod.Containers) != 1 {
		t.Fatalf("containers = %d", len(pod.Containers))
	}
	script := strings.Join(pod.Containers[0].Command, "\n")
	for _, want := range []string{
		"kubectl create",
		"kind: EtcdSnapshot",
		"generateName: nightly-",
		"name: my-etcd",      // clusterRef
		"claimName: backups", // destination mapped through
	} {
		if !strings.Contains(script, want) {
			t.Errorf("CronJob script missing %q:\n%s", want, script)
		}
	}

	// RBAC companions: SA + Role(create etcdsnapshots) + RoleBinding.
	if len(plan.Extras) != 3 {
		t.Fatalf("extras = %d, want SA+Role+RoleBinding", len(plan.Extras))
	}
	var role *rbacv1.Role
	for _, e := range plan.Extras {
		if r, ok := e.(*rbacv1.Role); ok {
			role = r
		}
	}
	if role == nil {
		t.Fatal("no Role among extras")
	}
	if len(role.Rules) != 1 || role.Rules[0].Resources[0] != "etcdsnapshots" || role.Rules[0].Verbs[0] != "create" {
		t.Errorf("Role rules = %+v", role.Rules)
	}

	if !hasWarning(plan.Warnings, "PRINTED ONLY") {
		t.Errorf("expected review-and-apply warning, got %v", plan.Warnings)
	}
}

// TestTranslateSchedule_MalformedDestination: an invalid destination is a
// hard error, mirroring the backup translation.
func TestTranslateSchedule_MalformedDestination(t *testing.T) {
	plan := TranslateSchedule("s", "ns", legacy.EtcdBackupScheduleSpec{
		ClusterRef: corev1.LocalObjectReference{Name: "c"},
		Schedule:   "@hourly",
	})
	if plan.Action != ActionError {
		t.Fatalf("Action = %s, want Error", plan.Action)
	}
}
