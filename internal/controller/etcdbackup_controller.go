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
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
	"github.com/aenix-io/etcd-operator/internal/log"
)

// EtcdBackupReconciler reconciles a EtcdBackup object
type EtcdBackupReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	OperatorImage string
}

// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackups,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackups/finalizers,verbs=update
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;delete

// Reconcile handles EtcdBackup resources by creating backup Jobs and tracking their status.
func (r *EtcdBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log.Debug(ctx, "reconciling EtcdBackup")

	backup := &etcdaenixiov1alpha1.EtcdBackup{}
	if err := r.Get(ctx, req.NamespacedName, backup); err != nil {
		if errors.IsNotFound(err) {
			log.Debug(ctx, "EtcdBackup not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !backup.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	if meta.IsStatusConditionTrue(backup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionComplete) ||
		meta.IsStatusConditionTrue(backup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionFailed) {
		return ctrl.Result{}, nil
	}

	cluster := &etcdaenixiov1alpha1.EtcdCluster{}
	clusterKey := types.NamespacedName{
		Name:      backup.Spec.ClusterRef.Name,
		Namespace: backup.Namespace,
	}
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if errors.IsNotFound(err) {
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:               etcdaenixiov1alpha1.EtcdBackupConditionFailed,
				Status:             metav1.ConditionTrue,
				Reason:             "ClusterNotFound",
				Message:            fmt.Sprintf("EtcdCluster %q not found", backup.Spec.ClusterRef.Name),
				ObservedGeneration: backup.Generation,
			})
			return r.updateStatus(ctx, backup)
		}
		return ctrl.Result{}, fmt.Errorf("failed to get EtcdCluster: %w", err)
	}

	existingJobs := &batchv1.JobList{}
	if err := r.List(ctx, existingJobs,
		client.InNamespace(backup.Namespace),
		client.MatchingLabels{"etcd.aenix.io/etcdbackup-name": backup.Name},
	); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to list Jobs: %w", err)
	}
	if len(existingJobs.Items) > 0 {
		return r.reconcileJobStatus(ctx, backup, &existingJobs.Items[0])
	}

	if r.OperatorImage == "" {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupConditionFailed,
			Status:             metav1.ConditionTrue,
			Reason:             "ConfigurationError",
			Message:            "OPERATOR_IMAGE environment variable is not set; cannot create backup Job",
			ObservedGeneration: backup.Generation,
		})
		return r.updateStatus(ctx, backup)
	}

	job, err := factory.CreateBackupJob(backup, cluster, r.OperatorImage, r.Scheme)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to build backup Job: %w", err)
	}

	if err := r.Create(ctx, job); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create backup Job: %w", err)
	}

	log.Info(ctx, "backup Job created", "job", job.Name)
	meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
		Type:               etcdaenixiov1alpha1.EtcdBackupConditionStarted,
		Status:             metav1.ConditionTrue,
		Reason:             "JobCreated",
		Message:            fmt.Sprintf("Backup Job %q created", job.Name),
		ObservedGeneration: backup.Generation,
	})

	return r.updateStatus(ctx, backup)
}

func (r *EtcdBackupReconciler) reconcileJobStatus(
	ctx context.Context,
	backup *etcdaenixiov1alpha1.EtcdBackup,
	job *batchv1.Job,
) (ctrl.Result, error) {
	if job.Status.Succeeded >= 1 {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupConditionComplete,
			Status:             metav1.ConditionTrue,
			Reason:             "JobSucceeded",
			Message:            "Backup completed successfully",
			ObservedGeneration: backup.Generation,
		})
		return r.updateStatus(ctx, backup)
	}

	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
				Type:               etcdaenixiov1alpha1.EtcdBackupConditionFailed,
				Status:             metav1.ConditionTrue,
				Reason:             "JobFailed",
				Message:            c.Message,
				ObservedGeneration: backup.Generation,
			})
			return r.updateStatus(ctx, backup)
		}
	}

	if !meta.IsStatusConditionTrue(backup.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupConditionStarted) {
		meta.SetStatusCondition(&backup.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupConditionStarted,
			Status:             metav1.ConditionTrue,
			Reason:             "JobCreated",
			Message:            fmt.Sprintf("Backup Job %q is running", job.Name),
			ObservedGeneration: backup.Generation,
		})
		return r.updateStatus(ctx, backup)
	}

	return ctrl.Result{}, nil
}

func (r *EtcdBackupReconciler) updateStatus(ctx context.Context, backup *etcdaenixiov1alpha1.EtcdBackup) (ctrl.Result, error) {
	err := r.Status().Update(ctx, backup)
	if err == nil {
		return ctrl.Result{}, nil
	}
	if errors.IsConflict(err) {
		log.Debug(ctx, "conflict during EtcdBackup status update")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Error(ctx, err, "cannot update EtcdBackup status")
	return ctrl.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdaenixiov1alpha1.EtcdBackup{}).
		Owns(&batchv1.Job{}).
		Complete(r)
}
