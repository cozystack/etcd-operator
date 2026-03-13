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
	"math"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// log is for logging in this package.
var etcdclusterlog = logf.Log.WithName("etcdcluster-resource")

// SetupWebhookWithManager will setup the manager to manage the webhooks
func (r *EtcdCluster) SetupWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-etcd-aenix-io-v1alpha1-etcdcluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=etcd.aenix.io,resources=etcdclusters,verbs=create;update,versions=v1alpha1,name=metcdcluster.kb.io,admissionReviewVersions=v1

var _ webhook.Defaulter = &EtcdCluster{}

// Default implements webhook.Defaulter so a webhook will be registered for the type
func (r *EtcdCluster) Default() {
	etcdclusterlog.Info("default", "name", r.Name)
	if r.Spec.Storage.EmptyDir == nil {
		if len(r.Spec.Storage.VolumeClaimTemplate.Spec.AccessModes) == 0 {
			r.Spec.Storage.VolumeClaimTemplate.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
		}
		storage := r.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests.Storage()
		if storage == nil || storage.IsZero() {
			r.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests = corev1.ResourceList{
				corev1.ResourceStorage: resource.MustParse("4Gi"),
			}
		}
	}
}

// +kubebuilder:webhook:path=/validate-etcd-aenix-io-v1alpha1-etcdcluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=etcd.aenix.io,resources=etcdclusters,verbs=create;update,versions=v1alpha1,name=vetcdcluster.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &EtcdCluster{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *EtcdCluster) ValidateCreate() (admission.Warnings, error) {
	etcdclusterlog.Info("validate create", "name", r.Name)

	var allErrors field.ErrorList

	warnings, pdbErr := r.validatePdb()
	if pdbErr != nil {
		allErrors = append(allErrors, pdbErr...)
	}

	securityErr := r.validateSecurity()
	if securityErr != nil {
		allErrors = append(allErrors, securityErr...)
	}

	if errOptions := validateOptions(r); errOptions != nil {
		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "options"),
			r.Spec.Options,
			errOptions.Error()))
	}

	bootstrapErr := r.validateBootstrap()
	allErrors = append(allErrors, bootstrapErr...)

	if len(allErrors) > 0 {
		err := errors.NewInvalid(
			schema.GroupKind{Group: GroupVersion.Group, Kind: "EtcdCluster"},
			r.Name, allErrors)
		return warnings, err
	}

	return warnings, nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *EtcdCluster) ValidateUpdate(old runtime.Object) (admission.Warnings, error) {
	etcdclusterlog.Info("validate update", "name", r.Name)
	var warnings admission.Warnings
	oldCluster := old.(*EtcdCluster)

	// Check if replicas are being resized
	if *oldCluster.Spec.Replicas != *r.Spec.Replicas {
		warnings = append(warnings, "cluster resize is not currently supported")
	}

	var allErrors field.ErrorList

	// Check if storage size is being decreased
	oldStorage := oldCluster.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
	newStorage := r.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]
	if newStorage.Cmp(oldStorage) < 0 {
		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "storage", "volumeClaimTemplate", "resources", "requests", "storage"),
			newStorage.String(),
			"decreasing storage size is not allowed"),
		)
	}

	// Check if storage type is changing
	if oldCluster.Spec.Storage.EmptyDir == nil && r.Spec.Storage.EmptyDir != nil ||
		oldCluster.Spec.Storage.EmptyDir != nil && r.Spec.Storage.EmptyDir == nil {
		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "storage", "emptyDir"),
			r.Spec.Storage.EmptyDir,
			"field is immutable"),
		)
	}

	// Check if the bootstrap is being changed
	if !equality.Semantic.DeepEqual(oldCluster.Spec.Bootstrap, r.Spec.Bootstrap) {
		allErrors = append(allErrors, field.Forbidden(
			field.NewPath("spec", "bootstrap"),
			"field is immutable after cluster creation"),
		)
	}

	// Validate PodDisruptionBudget
	pdbWarnings, pdbErr := r.validatePdb()
	if pdbErr != nil {
		allErrors = append(allErrors, pdbErr...)
	}
	if len(pdbWarnings) > 0 {
		warnings = append(warnings, pdbWarnings...)
	}

	// Validate Security
	securityErr := r.validateSecurity()
	if securityErr != nil {
		allErrors = append(allErrors, securityErr...)
	}

	// Validate Options
	if errOptions := validateOptions(r); errOptions != nil {
		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "options"),
			r.Spec.Options,
			errOptions.Error()))
	}

	if len(allErrors) > 0 {
		err := errors.NewInvalid(
			schema.GroupKind{Group: GroupVersion.Group, Kind: "EtcdCluster"},
			r.Name, allErrors)
		return warnings, err
	}

	return warnings, nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *EtcdCluster) ValidateDelete() (admission.Warnings, error) {
	etcdclusterlog.Info("validate delete", "name", r.Name)
	return nil, nil
}

// validatePdb validates PDB fields
func (r *EtcdCluster) validatePdb() (admission.Warnings, field.ErrorList) {
	if r.Spec.PodDisruptionBudgetTemplate == nil {
		return nil, nil
	}
	pdb := r.Spec.PodDisruptionBudgetTemplate
	var warnings admission.Warnings
	var allErrors field.ErrorList

	if pdb.Spec.MinAvailable != nil && pdb.Spec.MaxUnavailable != nil {
		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "podDisruptionBudget", "minAvailable"),
			pdb.Spec.MinAvailable.IntValue(),
			"minAvailable is mutually exclusive with maxUnavailable"),
		)
		return nil, allErrors
	}

	minQuorumSize := r.CalculateQuorumSize()
	if pdb.Spec.MinAvailable != nil {
		minAvailable := pdb.Spec.MinAvailable.IntValue()
		if pdb.Spec.MinAvailable.Type == intstr.String && minAvailable == 0 && pdb.Spec.MinAvailable.StrVal != "0" {
			var percentage int
			_, err := fmt.Sscanf(pdb.Spec.MinAvailable.StrVal, "%d%%", &percentage)
			if err != nil {
				allErrors = append(allErrors, field.Invalid(
					field.NewPath("spec", "podDisruptionBudget", "minAvailable"),
					pdb.Spec.MinAvailable.StrVal,
					"invalid percentage value"),
				)
			} else {
				minAvailable = int(math.Ceil(float64(*r.Spec.Replicas) * (float64(percentage) / 100)))
			}
		}

		if minAvailable < 0 {
			allErrors = append(allErrors, field.Invalid(
				field.NewPath("spec", "podDisruptionBudget", "minAvailable"),
				pdb.Spec.MinAvailable.IntValue(),
				"value cannot be less than zero"),
			)
		}
		if minAvailable > int(*r.Spec.Replicas) {
			allErrors = append(allErrors, field.Invalid(
				field.NewPath("spec", "podDisruptionBudget", "minAvailable"),
				pdb.Spec.MinAvailable.IntValue(),
				"value cannot be larger than number of replicas"),
			)
		}
		if minAvailable < minQuorumSize {
			warnings = append(warnings, "current number of spec.podDisruptionBudget.minAvailable can lead to loss of quorum")
		}
	}
	if pdb.Spec.MaxUnavailable != nil {
		maxUnavailable := pdb.Spec.MaxUnavailable.IntValue()
		if pdb.Spec.MaxUnavailable.Type == intstr.String && maxUnavailable == 0 && pdb.Spec.MaxUnavailable.StrVal != "0" {
			var percentage int
			_, err := fmt.Sscanf(pdb.Spec.MaxUnavailable.StrVal, "%d%%", &percentage)
			if err != nil {
				allErrors = append(allErrors, field.Invalid(
					field.NewPath("spec", "podDisruptionBudget", "maxUnavailable"),
					pdb.Spec.MaxUnavailable.StrVal,
					"invalid percentage value"),
				)
			} else {
				maxUnavailable = int(math.Ceil(float64(*r.Spec.Replicas) * (float64(percentage) / 100)))
			}
		}
		if maxUnavailable < 0 {
			allErrors = append(allErrors, field.Invalid(
				field.NewPath("spec", "podDisruptionBudget", "maxUnavailable"),
				pdb.Spec.MaxUnavailable.IntValue(),
				"value cannot be less than zero"),
			)
		}
		if maxUnavailable > int(*r.Spec.Replicas) {
			allErrors = append(allErrors, field.Invalid(
				field.NewPath("spec", "podDisruptionBudget", "maxUnavailable"),
				pdb.Spec.MaxUnavailable.IntValue(),
				"value cannot be larger than number of replicas"),
			)
		}
		if int(*r.Spec.Replicas)-maxUnavailable < minQuorumSize {
			warnings = append(warnings, "current number of spec.podDisruptionBudget.maxUnavailable can lead to loss of quorum")
		}
	}

	if len(allErrors) > 0 {
		return nil, allErrors
	}

	return warnings, nil
}

func (r *EtcdCluster) validateSecurity() field.ErrorList {

	var allErrors field.ErrorList

	if r.Spec.Security == nil {
		return nil
	}

	security := r.Spec.Security

	if (security.TLS.PeerSecret != "" && security.TLS.PeerTrustedCASecret == "") ||
		(security.TLS.PeerSecret == "" && security.TLS.PeerTrustedCASecret != "") {

		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "security", "tls"),
			security.TLS,
			"both spec.security.tls.peerSecret and spec.security.tls.peerTrustedCASecret must be filled or empty"),
		)
	}

	if (security.TLS.ClientSecret != "" && security.TLS.ClientTrustedCASecret == "") ||
		(security.TLS.ClientSecret == "" && security.TLS.ClientTrustedCASecret != "") {

		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "security", "tls"),
			security.TLS,
			"both spec.security.tls.clientSecret and spec.security.tls.clientTrustedCASecret must be filled or empty"),
		)
	}

	if security.EnableAuth && (security.TLS.ClientSecret == "" || security.TLS.ServerSecret == "") {

		allErrors = append(allErrors, field.Invalid(
			field.NewPath("spec", "security"),
			security.TLS,
			"if auth is enabled, client secret and server secret must be provided"),
		)
	}

	if len(allErrors) > 0 {
		return allErrors
	}

	return nil
}

func (r *EtcdCluster) validateBootstrap() field.ErrorList {
	if r.Spec.Bootstrap == nil || r.Spec.Bootstrap.Restore == nil {
		return nil
	}
	return validateBackupDestination(
		r.Spec.Bootstrap.Restore.Source,
		field.NewPath("spec", "bootstrap", "restore", "source"),
	)
}

func validateOptions(cluster *EtcdCluster) error {
	if len(cluster.Spec.Options) == 0 {
		return nil
	}

	systemflags := map[string]struct{}{
		"name":                        {},
		"listen-peer-urls":            {},
		"listen-client-urls":          {},
		"initial-advertise-peer-urls": {},
		"data-dir":                    {},
		"auto-tls":                    {},
		"peer-auto-tls":               {},
		"advertise-client-urls":       {},
	}

	errlist := []error{}

	for name := range cluster.Spec.Options {
		if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") {
			errlist = append(errlist, fmt.Errorf("options should not start with dashes or have trailing dashes. Flag: %s", name))

			continue
		}

		if _, exists := systemflags[name]; exists {
			errlist = append(errlist, fmt.Errorf("can't use extra argument '%s' in .spec.options "+
				"because it is generated by the operator for cluster setup", name))
		}
	}

	return kerrors.NewAggregate(errlist)
}
