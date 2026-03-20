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
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
	"github.com/aenix-io/etcd-operator/internal/log"
)

// cronJobNeedsUpdate compares only the fields that the operator controls,
// avoiding false positives from API Server-defaulted fields like
// TerminationMessagePath, ImagePullPolicy, etc.
func cronJobNeedsUpdate(existing, desired *batchv1.CronJob) bool {
	if existing.Spec.Schedule != desired.Spec.Schedule {
		return true
	}
	if existing.Spec.ConcurrencyPolicy != desired.Spec.ConcurrencyPolicy {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.SuccessfulJobsHistoryLimit, desired.Spec.SuccessfulJobsHistoryLimit) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Spec.FailedJobsHistoryLimit, desired.Spec.FailedJobsHistoryLimit) {
		return true
	}
	if !equality.Semantic.DeepEqual(existing.Labels, desired.Labels) {
		return true
	}

	existingContainers := existing.Spec.JobTemplate.Spec.Template.Spec.Containers
	desiredContainers := desired.Spec.JobTemplate.Spec.Template.Spec.Containers
	if len(existingContainers) != len(desiredContainers) {
		return true
	}
	for i := range desiredContainers {
		if existingContainers[i].Image != desiredContainers[i].Image {
			return true
		}
		if !equality.Semantic.DeepEqual(existingContainers[i].Command, desiredContainers[i].Command) {
			return true
		}
		if !equality.Semantic.DeepEqual(existingContainers[i].Env, desiredContainers[i].Env) {
			return true
		}
		if !equality.Semantic.DeepEqual(existingContainers[i].VolumeMounts, desiredContainers[i].VolumeMounts) {
			return true
		}
	}

	existingVolumes := existing.Spec.JobTemplate.Spec.Template.Spec.Volumes
	desiredVolumes := desired.Spec.JobTemplate.Spec.Template.Spec.Volumes
	if len(existingVolumes) != len(desiredVolumes) {
		return true
	}
	for i := range desiredVolumes {
		if existingVolumes[i].Name != desiredVolumes[i].Name {
			return true
		}
		if !equality.Semantic.DeepEqual(existingVolumes[i].VolumeSource, desiredVolumes[i].VolumeSource) {
			return true
		}
	}
	return false
}

// EtcdBackupScheduleReconciler reconciles a [github.com/aenix-io/etcd-operator/api/v1alpha1.EtcdBackupSchedule] object
type EtcdBackupScheduleReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	OperatorImage string
}

// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackupschedules,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackupschedules/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdbackupschedules/finalizers,verbs=update
// +kubebuilder:rbac:groups=etcd.aenix.io,resources=etcdclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete

// Reconcile handles EtcdBackupSchedule resources by creating CronJobs and tracking their status.
func (r *EtcdBackupScheduleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log.Debug(ctx, "reconciling EtcdBackupSchedule")

	schedule := &etcdaenixiov1alpha1.EtcdBackupSchedule{}
	if err := r.Get(ctx, req.NamespacedName, schedule); err != nil {
		if errors.IsNotFound(err) {
			log.Debug(ctx, "EtcdBackupSchedule not found")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !schedule.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	cluster := &etcdaenixiov1alpha1.EtcdCluster{}
	clusterKey := types.NamespacedName{
		Name:      schedule.Spec.ClusterRef.Name,
		Namespace: schedule.Namespace,
	}
	if err := r.Get(ctx, clusterKey, cluster); err != nil {
		if errors.IsNotFound(err) {
			meta.SetStatusCondition(&schedule.Status.Conditions, metav1.Condition{
				Type:               etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady,
				Status:             metav1.ConditionFalse,
				Reason:             "ClusterNotFound",
				Message:            fmt.Sprintf("EtcdCluster %q not found, will retry", schedule.Spec.ClusterRef.Name),
				ObservedGeneration: schedule.Generation,
			})
			// Requeue to retry when the cluster may have been created.
			// The schedule controller does not watch EtcdCluster resources,
			// so periodic requeue is needed to recover from this state.
			result, updateErr := r.updateStatus(ctx, schedule)
			if updateErr != nil {
				return result, updateErr
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return ctrl.Result{}, fmt.Errorf("failed to get EtcdCluster: %w", err)
	}

	if r.OperatorImage == "" {
		meta.SetStatusCondition(&schedule.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupScheduleConditionFailed,
			Status:             metav1.ConditionTrue,
			Reason:             "ConfigurationError",
			Message:            "OPERATOR_IMAGE environment variable is not set; cannot create backup CronJob",
			ObservedGeneration: schedule.Generation,
		})
		meta.SetStatusCondition(&schedule.Status.Conditions, metav1.Condition{
			Type:               etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             "ConfigurationError",
			Message:            "OPERATOR_IMAGE environment variable is not set",
			ObservedGeneration: schedule.Generation,
		})
		return r.updateStatus(ctx, schedule)
	}

	cronJobName := factory.GetBackupCronJobName(schedule)
	existingCronJob := &batchv1.CronJob{}
	err := r.Get(ctx, types.NamespacedName{Name: cronJobName, Namespace: schedule.Namespace}, existingCronJob)
	if err == nil {
		return r.reconcileExistingCronJob(ctx, schedule, cluster, existingCronJob)
	}
	if !errors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("failed to get CronJob: %w", err)
	}

	cronJob, err := factory.CreateBackupCronJob(schedule, cluster, r.OperatorImage, r.Scheme)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to build backup CronJob: %w", err)
	}

	if err := r.Create(ctx, cronJob); err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to create backup CronJob: %w", err)
	}

	log.Info(ctx, "backup CronJob created", "cronJob", cronJobName)
	meta.SetStatusCondition(&schedule.Status.Conditions, metav1.Condition{
		Type:               etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "CronJobCreated",
		Message:            fmt.Sprintf("CronJob %q created", cronJobName),
		ObservedGeneration: schedule.Generation,
	})
	// Clear any previous failure condition
	meta.RemoveStatusCondition(&schedule.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupScheduleConditionFailed)

	return r.updateStatus(ctx, schedule)
}

func (r *EtcdBackupScheduleReconciler) reconcileExistingCronJob(
	ctx context.Context,
	schedule *etcdaenixiov1alpha1.EtcdBackupSchedule,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	existingCronJob *batchv1.CronJob,
) (ctrl.Result, error) {
	// Build desired CronJob to detect spec changes
	desiredCronJob, err := factory.CreateBackupCronJob(schedule, cluster, r.OperatorImage, r.Scheme)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to build desired CronJob: %w", err)
	}

	// Update CronJob if meaningful fields changed
	if cronJobNeedsUpdate(existingCronJob, desiredCronJob) {
		existingCronJob.Spec = desiredCronJob.Spec
		existingCronJob.Labels = desiredCronJob.Labels
		if err := r.Update(ctx, existingCronJob); err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to update CronJob: %w", err)
		}
		log.Info(ctx, "backup CronJob updated", "cronJob", existingCronJob.Name)
	}

	// Sync status from CronJob
	if existingCronJob.Status.LastScheduleTime != nil {
		schedule.Status.LastScheduleTime = existingCronJob.Status.LastScheduleTime
	}
	if existingCronJob.Status.LastSuccessfulTime != nil {
		schedule.Status.LastSuccessfulBackupTime = existingCronJob.Status.LastSuccessfulTime
	}

	meta.SetStatusCondition(&schedule.Status.Conditions, metav1.Condition{
		Type:               etcdaenixiov1alpha1.EtcdBackupScheduleConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             "CronJobReady",
		Message:            fmt.Sprintf("CronJob %q is active", existingCronJob.Name),
		ObservedGeneration: schedule.Generation,
	})
	meta.RemoveStatusCondition(&schedule.Status.Conditions, etcdaenixiov1alpha1.EtcdBackupScheduleConditionFailed)

	return r.updateStatus(ctx, schedule)
}

func (r *EtcdBackupScheduleReconciler) updateStatus(ctx context.Context, schedule *etcdaenixiov1alpha1.EtcdBackupSchedule) (ctrl.Result, error) {
	err := r.Status().Update(ctx, schedule)
	if err == nil {
		return ctrl.Result{}, nil
	}
	if errors.IsConflict(err) {
		log.Debug(ctx, "conflict during EtcdBackupSchedule status update")
		return ctrl.Result{Requeue: true}, nil
	}
	log.Error(ctx, err, "cannot update EtcdBackupSchedule status")
	return ctrl.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *EtcdBackupScheduleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&etcdaenixiov1alpha1.EtcdBackupSchedule{}).
		// we'll watch for ETCD Cluster changes and enqueue EtcdBackupSchedule objects
		// that are referencing the EtcdCluster being an event source.
		Watches(&etcdaenixiov1alpha1.EtcdCluster{},
			handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
				etcdCluster, ok := obj.(*etcdaenixiov1alpha1.EtcdCluster)
				if !ok {
					return nil
				}
				etcdSchedules := &etcdaenixiov1alpha1.EtcdBackupScheduleList{}
				if err := r.List(ctx, etcdSchedules, client.InNamespace(etcdCluster.Namespace), client.MatchingFields{etcdaenixiov1alpha1.EtcdClusterRefIndex: etcdCluster.Name}); err != nil {
					log.Error(ctx, err, "cannot list EtcdBackupSchedules for EtcdCluster")
					return nil
				}
				var requests []reconcile.Request
				for _, schedule := range etcdSchedules.Items {
					requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: schedule.Namespace, Name: schedule.Name}})
				}
				return requests
			})).
		Owns(&batchv1.CronJob{}).
		Complete(r)
}
