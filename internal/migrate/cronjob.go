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
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

// kubectlImage runs the generated CronJob's container. Pinned rather than
// :latest so the printed manifest is reproducible; the user reviews the
// manifest anyway and can swap in an in-house image.
const kubectlImage = "bitnami/kubectl:1.33"

// TranslateSchedule converts one legacy EtcdBackupSchedule into a PRINTED
// CronJob manifest (plus the ServiceAccount/Role/RoleBinding it needs). The
// new API has no schedule CRD by design — recurring snapshots are driven
// from outside — so nothing is applied and the legacy CR is not deleted; the
// user reviews, adjusts, and applies the manifests themselves.
func TranslateSchedule(name, namespace string, spec legacy.EtcdBackupScheduleSpec) ResourcePlan {
	plan := ResourcePlan{
		SourceKind: "EtcdBackupSchedule",
		SourceName: name,
		Namespace:  namespace,
		Action:     ActionPrint,
	}

	dest, err := translateLocation(spec.Destination)
	if err != nil {
		plan.Action = ActionError
		plan.Errors = append(plan.Errors, "spec.destination: "+err.Error())
		return plan
	}

	// The EtcdSnapshot each tick creates. generateName + `kubectl create`
	// (not apply) because snapshots are immutable one-shots — every tick
	// must produce a fresh object.
	snapshot := &lll.EtcdSnapshot{
		TypeMeta:   metav1.TypeMeta{APIVersion: lll.GroupVersion.String(), Kind: "EtcdSnapshot"},
		ObjectMeta: metav1.ObjectMeta{GenerateName: name + "-", Namespace: namespace},
		Spec: lll.EtcdSnapshotSpec{
			ClusterRef:  spec.ClusterRef,
			Destination: dest,
		},
	}
	snapshotYAML, yErr := yaml.Marshal(snapshot)
	if yErr != nil {
		plan.Action = ActionError
		plan.Errors = append(plan.Errors, "render EtcdSnapshot template: "+yErr.Error())
		return plan
	}

	saName := name + "-snapshotter"
	script := fmt.Sprintf("kubectl create -f - <<'EOF'\n%sEOF\n", snapshotYAML)

	plan.Target = &batchv1.CronJob{
		TypeMeta:   metav1.TypeMeta{APIVersion: "batch/v1", Kind: "CronJob"},
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: batchv1.CronJobSpec{
			Schedule:                   spec.Schedule,
			SuccessfulJobsHistoryLimit: spec.SuccessfulJobsHistoryLimit,
			FailedJobsHistoryLimit:     spec.FailedJobsHistoryLimit,
			JobTemplate: batchv1.JobTemplateSpec{
				Spec: batchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							ServiceAccountName: saName,
							RestartPolicy:      corev1.RestartPolicyNever,
							Containers: []corev1.Container{{
								Name:    "create-snapshot",
								Image:   kubectlImage,
								Command: []string{"/bin/sh", "-ec", script},
							}},
						},
					},
				},
			},
		},
	}
	plan.Extras = scheduleRBAC(saName, namespace)

	plan.Warnings = append(plan.Warnings,
		"EtcdBackupSchedule has no v1alpha2 CRD: the CronJob manifest above is PRINTED ONLY — review it (image, schedule, RBAC) and apply it yourself",
		"the legacy EtcdBackupSchedule CR is left in place; delete it manually once the CronJob replacement is applied")
	return plan
}

// scheduleRBAC builds the SA/Role/RoleBinding the CronJob needs to create
// EtcdSnapshot objects in its namespace.
func scheduleRBAC(saName, namespace string) []client.Object {
	return []client.Object{
		&corev1.ServiceAccount{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace},
		},
		&rbacv1.Role{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "Role"},
			ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace},
			Rules: []rbacv1.PolicyRule{{
				APIGroups: []string{lll.GroupVersion.Group},
				Resources: []string{"etcdsnapshots"},
				Verbs:     []string{"create"},
			}},
		},
		&rbacv1.RoleBinding{
			TypeMeta:   metav1.TypeMeta{APIVersion: "rbac.authorization.k8s.io/v1", Kind: "RoleBinding"},
			ObjectMeta: metav1.ObjectMeta{Name: saName, Namespace: namespace},
			RoleRef: rbacv1.RoleRef{
				APIGroup: rbacv1.GroupName,
				Kind:     "Role",
				Name:     saName,
			},
			Subjects: []rbacv1.Subject{{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      saName,
				Namespace: namespace,
			}},
		},
	}
}
