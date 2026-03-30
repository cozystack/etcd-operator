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
	"context"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

var etcdbackuplog = logf.Log.WithName("etcdbackup-resource")

type EtcdBackupValidator struct{}

// SetupWebhookWithManager will set up the manager to manage the webhooks
func (r *EtcdBackupValidator) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &EtcdBackup{}).
		WithValidator(r).
		Complete()
}

// +kubebuilder:webhook:path=/validate-etcd-aenix-io-v1alpha1-etcdbackup,mutating=false,failurePolicy=fail,sideEffects=None,groups=etcd.aenix.io,resources=etcdbackups,verbs=create;update,versions=v1alpha1,name=vetcdbackup.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*EtcdBackup] = &EtcdBackupValidator{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (*EtcdBackupValidator) ValidateCreate(_ context.Context, obj *EtcdBackup) (admission.Warnings, error) {
	etcdbackuplog.Info("validate create", "name", obj.Name)

	var allErrors field.ErrorList

	if obj.Spec.ClusterRef.Name == "" {
		allErrors = append(allErrors, field.Required(
			field.NewPath("spec", "clusterRef", "name"),
			"clusterRef.name is required",
		))
	}

	destErrors := obj.validateDestination()
	allErrors = append(allErrors, destErrors...)

	if len(allErrors) > 0 {
		return nil, errors.NewInvalid(
			schema.GroupKind{Group: GroupVersion.Group, Kind: "EtcdBackup"},
			obj.Name, allErrors)
	}

	return nil, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (*EtcdBackupValidator) ValidateUpdate(_ context.Context, old, new *EtcdBackup) (admission.Warnings, error) {
	etcdbackuplog.Info("validate update", "name", old.Name)

	if !equality.Semantic.DeepEqual(old.Spec, new.Spec) {
		var allErrors field.ErrorList
		allErrors = append(allErrors, field.Forbidden(
			field.NewPath("spec"),
			"EtcdBackup spec is immutable",
		))
		return nil, errors.NewInvalid(
			schema.GroupKind{Group: GroupVersion.Group, Kind: "EtcdBackup"},
			old.Name, allErrors)
	}

	return nil, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (*EtcdBackupValidator) ValidateDelete(_ context.Context, obj *EtcdBackup) (admission.Warnings, error) {
	etcdbackuplog.Info("validate delete", "name", obj.Name)
	return nil, nil
}

func (r *EtcdBackup) validateDestination() field.ErrorList {
	return validateBackupDestination(r.Spec.Destination, field.NewPath("spec", "destination"))
}

// validateBackupDestination validates a BackupDestination at the given field path.
// This is shared between EtcdBackup and EtcdCluster (bootstrap restore source) webhooks.
func validateBackupDestination(dest BackupDestination, destPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList

	if dest.S3 == nil && dest.PVC == nil {
		allErrors = append(allErrors, field.Required(
			destPath,
			"exactly one of s3 or pvc must be specified",
		))
		return allErrors
	}

	if dest.S3 != nil && dest.PVC != nil {
		allErrors = append(allErrors, field.Invalid(
			destPath,
			"both s3 and pvc",
			"exactly one of s3 or pvc must be specified, not both",
		))
		return allErrors
	}

	if s3 := dest.S3; s3 != nil {
		s3Path := destPath.Child("s3")
		if s3.Endpoint == "" {
			allErrors = append(allErrors, field.Required(s3Path.Child("endpoint"), "endpoint is required"))
		} else if !strings.HasPrefix(s3.Endpoint, "http://") && !strings.HasPrefix(s3.Endpoint, "https://") {
			allErrors = append(allErrors, field.Invalid(s3Path.Child("endpoint"), s3.Endpoint,
				"endpoint must start with http:// or https://"))
		}
		if s3.Bucket == "" {
			allErrors = append(allErrors, field.Required(s3Path.Child("bucket"), "bucket is required"))
		}
		if s3.CredentialsSecretRef.Name == "" {
			allErrors = append(allErrors, field.Required(s3Path.Child("credentialsSecretRef", "name"), "credentialsSecretRef.name is required"))
		}
	}

	if pvc := dest.PVC; pvc != nil {
		pvcPath := destPath.Child("pvc")
		if pvc.ClaimName == "" {
			allErrors = append(allErrors, field.Required(pvcPath.Child("claimName"), "claimName is required"))
		}
		if pvc.SubPath != "" {
			cleaned := filepath.Clean(pvc.SubPath)
			if strings.HasPrefix(cleaned, "..") || filepath.IsAbs(cleaned) {
				allErrors = append(allErrors, field.Invalid(
					pvcPath.Child("subPath"), pvc.SubPath,
					"subPath must be a relative path and must not contain '..' components",
				))
			}
		}
	}

	return allErrors
}
