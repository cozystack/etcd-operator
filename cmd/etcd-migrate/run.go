/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/migrate"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

// deployRef is a parsed --legacy-controller / --new-controller coordinate.
type deployRef struct {
	Namespace string
	Name      string
}

// checkControllersDown verifies every Deployment coordinate is either absent
// or scaled to zero with no pods left under its selector. Identical
// coordinates (the common case: both generations deploy under the same name)
// are checked once.
func checkControllersDown(ctx context.Context, kube kubernetes.Interface, refs []deployRef) error {
	seen := map[deployRef]bool{}
	for _, ref := range refs {
		if seen[ref] {
			continue
		}
		seen[ref] = true

		dep, err := kube.AppsV1().Deployments(ref.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue // not installed — trivially down
		}
		if err != nil {
			return fmt.Errorf("check controller %s/%s: %w", ref.Namespace, ref.Name, err)
		}
		if dep.Spec.Replicas == nil || *dep.Spec.Replicas != 0 {
			return fmt.Errorf("controller %s/%s is not scaled down (spec.replicas=%v); "+
				"scale it to 0 first: kubectl -n %s scale deploy %s --replicas=0 (or pass --skip-controller-check)",
				ref.Namespace, ref.Name, dep.Spec.Replicas, ref.Namespace, ref.Name)
		}
		selector, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
		if err != nil {
			return fmt.Errorf("check controller %s/%s: bad selector: %w", ref.Namespace, ref.Name, err)
		}
		pods, err := kube.CoreV1().Pods(ref.Namespace).List(ctx, metav1.ListOptions{LabelSelector: selector.String()})
		if err != nil {
			return fmt.Errorf("check controller %s/%s pods: %w", ref.Namespace, ref.Name, err)
		}
		for _, p := range pods.Items {
			if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
				continue
			}
			return fmt.Errorf("controller %s/%s still has pod %s (%s); "+
				"wait for it to terminate (or pass --skip-controller-check)",
				ref.Namespace, ref.Name, p.Name, p.Status.Phase)
		}
	}
	return nil
}

// legacyCluster is one discovered legacy EtcdCluster, with the identity bits
// the snapshot phase needs alongside the decoded spec.
type legacyCluster struct {
	Name      string
	Namespace string
	UID       string
	Spec      legacy.EtcdClusterSpec
}

type legacyBackup struct {
	Name      string
	Namespace string
	Spec      legacy.EtcdBackupSpec
}

type legacySchedule struct {
	Name      string
	Namespace string
	Spec      legacy.EtcdBackupScheduleSpec
}

// discovered is everything the legacy API still holds.
type discovered struct {
	Clusters  []legacyCluster
	Backups   []legacyBackup
	Schedules []legacySchedule
}

// discover lists all legacy CRs in the namespace ("" = all). A missing
// legacy CRD is treated as zero resources of that kind, so the tool works
// on clusters where only a subset of the legacy CRDs was ever installed.
func discover(ctx context.Context, dyn dynamic.Interface, namespace string) (discovered, error) {
	var out discovered

	err := listLegacy(ctx, dyn, migrate.ClusterGVR, namespace, func(u *unstructured.Unstructured) error {
		var spec legacy.EtcdClusterSpec
		if err := decodeSpec(u, &spec); err != nil {
			return err
		}
		out.Clusters = append(out.Clusters, legacyCluster{
			Name: u.GetName(), Namespace: u.GetNamespace(), UID: string(u.GetUID()), Spec: spec,
		})
		return nil
	})
	if err != nil {
		return out, err
	}

	err = listLegacy(ctx, dyn, migrate.BackupGVR, namespace, func(u *unstructured.Unstructured) error {
		var spec legacy.EtcdBackupSpec
		if err := decodeSpec(u, &spec); err != nil {
			return err
		}
		out.Backups = append(out.Backups, legacyBackup{Name: u.GetName(), Namespace: u.GetNamespace(), Spec: spec})
		return nil
	})
	if err != nil {
		return out, err
	}

	err = listLegacy(ctx, dyn, migrate.ScheduleGVR, namespace, func(u *unstructured.Unstructured) error {
		var spec legacy.EtcdBackupScheduleSpec
		if err := decodeSpec(u, &spec); err != nil {
			return err
		}
		out.Schedules = append(out.Schedules, legacySchedule{Name: u.GetName(), Namespace: u.GetNamespace(), Spec: spec})
		return nil
	})
	return out, err
}

// listLegacy lists one legacy GVR, tolerating an uninstalled CRD.
func listLegacy(ctx context.Context, dyn dynamic.Interface, gvr schema.GroupVersionResource, namespace string, visit func(*unstructured.Unstructured) error) error {
	list, err := dyn.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil // legacy CRD not installed
		}
		return fmt.Errorf("list %s: %w", gvr.Resource, err)
	}
	for i := range list.Items {
		if err := visit(&list.Items[i]); err != nil {
			return fmt.Errorf("decode %s %s/%s: %w", gvr.Resource, list.Items[i].GetNamespace(), list.Items[i].GetName(), err)
		}
	}
	return nil
}

// decodeSpec converts an unstructured legacy object's .spec into a trimmed
// legacy struct.
func decodeSpec(u *unstructured.Unstructured, into any) error {
	spec, found, err := unstructured.NestedMap(u.Object, "spec")
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("object has no spec")
	}
	return runtime.DefaultUnstructuredConverter.FromUnstructured(spec, into)
}

// buildPlans translates everything discovered into per-resource plans.
// Clusters become in-place adoptions, built from the live facts gathered by
// inspectCluster; a cluster whose inspection failed gets an error plan (its
// pods/PVCs are never touched).
func buildPlans(d discovered, facts map[string]migrate.ClusterFacts, inspectErrs map[string]error, opts migrate.TranslateOptions) []migrate.ResourcePlan {
	var plans []migrate.ResourcePlan
	for _, c := range d.Clusters {
		key := c.Namespace + "/" + c.Name
		if err, failed := inspectErrs[key]; failed {
			plans = append(plans, migrate.ResourcePlan{
				SourceKind: "EtcdCluster",
				SourceName: c.Name,
				Namespace:  c.Namespace,
				Action:     migrate.ActionError,
				Errors:     []string{fmt.Sprintf("inspecting the live cluster failed: %v", err)},
			})
			continue
		}
		plans = append(plans, migrate.BuildAdoption(c.Name, c.Namespace, c.Spec, facts[key], opts))
	}
	for _, b := range d.Backups {
		plans = append(plans, migrate.TranslateBackup(b.Name, b.Namespace, b.Spec))
	}
	for _, s := range d.Schedules {
		plans = append(plans, migrate.TranslateSchedule(s.Name, s.Namespace, s.Spec))
	}
	return plans
}

// markExisting downgrades Create plans whose target already exists to Skip,
// making the tool re-runnable after a partial apply. The legacy delete still
// proceeds on apply — the target exists, the source is leftover. Adoption
// plans are NOT downgraded: every adoption step is idempotent and a partial
// previous run (CRs created, pod patches missing) must be completed, not
// skipped.
func markExisting(ctx context.Context, c client.Client, plans []migrate.ResourcePlan) error {
	for i := range plans {
		if plans[i].Action == migrate.ActionAdopt && plans[i].Target != nil {
			existing := plans[i].Target.DeepCopyObject().(client.Object)
			err := c.Get(ctx, types.NamespacedName{Namespace: plans[i].Target.GetNamespace(), Name: plans[i].Target.GetName()}, existing)
			if err == nil {
				plans[i].Notes = append(plans[i].Notes,
					"new-API EtcdCluster already exists (previous run); adoption steps re-run idempotently to complete any partial state")
			} else if !apierrors.IsNotFound(err) {
				return fmt.Errorf("check existing %s %s/%s: %w",
					plans[i].SourceKind, plans[i].Namespace, plans[i].Target.GetName(), err)
			}
			continue
		}
		if plans[i].Action != migrate.ActionCreate || plans[i].Target == nil {
			continue
		}
		existing := plans[i].Target.DeepCopyObject().(client.Object)
		err := c.Get(ctx, types.NamespacedName{Namespace: plans[i].Target.GetNamespace(), Name: plans[i].Target.GetName()}, existing)
		switch {
		case apierrors.IsNotFound(err):
			// proceed with Create
		case err != nil:
			return fmt.Errorf("check existing %s %s/%s: %w",
				plans[i].SourceKind, plans[i].Namespace, plans[i].Target.GetName(), err)
		default:
			plans[i].Action = migrate.ActionSkip
			plans[i].Notes = append(plans[i].Notes,
				"target already exists under the new API (created by a previous run); only the legacy CR cleanup remains")
		}
	}
	return nil
}

// applyStats summarizes one apply pass.
type applyStats struct {
	Adopted, Created, Deleted, Skipped, Printed, Errored int
}

// applyPlans executes the plan: extras + target created per resource, then
// the legacy source is deleted (children cascade via their owner refs; the
// legacy CRs carry no finalizers, so deletion does not block).
func applyPlans(ctx context.Context, c client.Client, dyn dynamic.Interface, plans []migrate.ResourcePlan, out io.Writer) (applyStats, error) {
	var stats applyStats
	for i := range plans {
		p := &plans[i]
		switch p.Action {
		case migrate.ActionError:
			stats.Errored++
			continue
		case migrate.ActionPrint:
			stats.Printed++
			continue
		case migrate.ActionAdopt:
			if err := applyAdoption(ctx, c, dyn, p, out); err != nil {
				return stats, fmt.Errorf("adopt %s/%s: %w", p.Namespace, p.SourceName, err)
			}
			stats.Adopted++
			continue
		case migrate.ActionCreate:
			for _, extra := range p.Extras {
				if err := c.Create(ctx, extra); err != nil {
					if apierrors.IsAlreadyExists(err) {
						fmt.Fprintf(out, "  %s %s/%s already exists — left untouched\n",
							extra.GetObjectKind().GroupVersionKind().Kind, extra.GetNamespace(), extra.GetName())
						continue
					}
					return stats, fmt.Errorf("create %s %s/%s: %w",
						extra.GetObjectKind().GroupVersionKind().Kind, extra.GetNamespace(), extra.GetName(), err)
				}
			}
			if err := c.Create(ctx, p.Target); err != nil && !apierrors.IsAlreadyExists(err) {
				return stats, fmt.Errorf("create %s %s/%s: %w",
					p.SourceKind, p.Namespace, p.Target.GetName(), err)
			}
			stats.Created++
		case migrate.ActionSkip:
			stats.Skipped++
		}

		if p.DeleteRef != nil {
			policy := metav1.DeletePropagationBackground
			err := dyn.Resource(p.DeleteRef.GVR).Namespace(p.DeleteRef.Namespace).
				Delete(ctx, p.DeleteRef.Name, metav1.DeleteOptions{PropagationPolicy: &policy})
			if err != nil && !apierrors.IsNotFound(err) {
				return stats, fmt.Errorf("delete legacy %s %s/%s: %w",
					p.DeleteRef.GVR.Resource, p.DeleteRef.Namespace, p.DeleteRef.Name, err)
			}
			stats.Deleted++
			fmt.Fprintf(out, "  deleted legacy %s %s/%s\n", p.DeleteRef.GVR.Resource, p.DeleteRef.Namespace, p.DeleteRef.Name)
		}
	}
	return stats, nil
}

// confirm asks for an interactive yes before a mutating run.
func confirm(in io.Reader, out io.Writer, prompt string) bool {
	fmt.Fprintf(out, "%s [y/N]: ", prompt)
	reader := bufio.NewReader(in)
	line, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	return answer == "y" || answer == "yes"
}

// backupDestination assembles the SnapshotLocation from the --backup-*
// flags. validate() has already checked exactly one destination is set.
func backupDestination(cfg *Config) lll.SnapshotLocation {
	if cfg.BackupPVCClaim != "" {
		return lll.SnapshotLocation{PVC: &lll.PVCSnapshotLocation{
			ClaimName: cfg.BackupPVCClaim,
			SubPath:   cfg.BackupPVCSubPath,
		}}
	}
	return lll.SnapshotLocation{S3: &lll.S3SnapshotLocation{
		Endpoint:             cfg.BackupS3Endpoint,
		Bucket:               cfg.BackupS3Bucket,
		Key:                  cfg.BackupS3Key,
		Region:               cfg.BackupS3Region,
		ForcePathStyle:       cfg.BackupS3ForcePathStyle,
		CredentialsSecretRef: corev1.LocalObjectReference{Name: cfg.BackupS3CredentialsSecret},
	}}
}
