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

package v1alpha1

import (
	"fmt"
	"regexp"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var etcdbackupschedulelog = logf.Log.WithName("etcdbackupschedule-resource")

// cronRegexp validates standard 5-field cron expressions (minute hour day-of-month month day-of-week).
var cronRegexp = regexp.MustCompile(`^(@(annually|yearly|monthly|weekly|daily|hourly)|((\*|[0-9,/\-*]+)\s+){4}(\*|[0-9,/\-*]+))$`)

// SetupWebhookWithManager will setup the manager to manage the webhooks
func (r *EtcdBackupSchedule) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

// +kubebuilder:webhook:path=/validate-etcd-aenix-io-v1alpha1-etcdbackupschedule,mutating=false,failurePolicy=fail,sideEffects=None,groups=etcd.aenix.io,resources=etcdbackupschedules,verbs=create;update,versions=v1alpha1,name=vetcdbackupschedule.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &EtcdBackupSchedule{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *EtcdBackupSchedule) ValidateCreate() (admission.Warnings, error) {
	etcdbackupschedulelog.Info("validate create", "name", r.Name)

	var allErrors field.ErrorList

	if r.Spec.ClusterRef.Name == "" {
		allErrors = append(allErrors, field.Required(
			field.NewPath("spec", "clusterRef", "name"),
			"clusterRef.name is required",
		))
	}

	if r.Spec.Schedule == "" {
		allErrors = append(allErrors, field.Required(
			field.NewPath("spec", "schedule"),
			"schedule is required",
		))
	} else if !cronRegexp.MatchString(r.Spec.Schedule) {
		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "schedule"),
			r.Spec.Schedule,
			"schedule must be a valid cron expression",
		))
	}

	// CronJob name = "{name}-scheduled-backup" (17 char suffix).
	// CronJob names must be <= 52 chars (63 max Pod name - 11 for Job/Pod suffixes).
	const cronJobSuffix = "-scheduled-backup"
	const maxCronJobNameLen = 52
	maxNameLen := maxCronJobNameLen - len(cronJobSuffix)
	if len(r.Name) > maxNameLen {
		allErrors = append(allErrors, field.Invalid(
			field.NewPath("metadata", "name"),
			r.Name,
			fmt.Sprintf("name must be at most %d characters (CronJob name limit is %d, suffix %q is %d characters)",
				maxNameLen, maxCronJobNameLen, cronJobSuffix, len(cronJobSuffix)),
		))
	}

	destErrors := r.validateDestination()
	allErrors = append(allErrors, destErrors...)

	if len(allErrors) > 0 {
		return nil, errors.NewInvalid(
			schema.GroupKind{Group: GroupVersion.Group, Kind: "EtcdBackupSchedule"},
			r.Name, allErrors)
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *EtcdBackupSchedule) ValidateUpdate(_ runtime.Object) (admission.Warnings, error) {
	etcdbackupschedulelog.Info("validate update", "name", r.Name)

	// Schedules are mutable, so re-validate the full spec
	return r.ValidateCreate()
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *EtcdBackupSchedule) ValidateDelete() (admission.Warnings, error) {
	etcdbackupschedulelog.Info("validate delete", "name", r.Name)
	return nil, nil
}

func (r *EtcdBackupSchedule) validateDestination() field.ErrorList {
	return validateBackupDestination(r.Spec.Destination, field.NewPath("spec", "destination"))
}

