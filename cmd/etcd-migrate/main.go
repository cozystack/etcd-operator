/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// etcd-migrate adopts running legacy etcd.aenix.io/v1alpha1 clusters onto
// etcd-operator.cozystack.io/v1alpha2 IN PLACE. It runs in the window where
// BOTH operator Deployments are scaled to zero while the etcd Pods keep
// serving: it inspects each live cluster (member list, cluster ID), takes a
// safety backup, creates the new-API CRs with prefilled status, re-owns the
// existing Pods/PVCs/Services, and dismantles the legacy CR + StatefulSet
// with Orphan propagation. The etcd Pods are never restarted and no data is
// moved — the new operator simply takes over the running data plane.
package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Import all auth providers
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/migrate"
)

// version is stamped at build time via -ldflags "-X main.version=<tag>"
// (see the Makefile's CLI_LDFLAGS); "dev" for un-stamped local builds.
var version = "dev"

func main() {
	cfg := &Config{}
	rootCmd := &cobra.Command{
		Use:   "etcd-migrate",
		Short: "Migrate etcd.aenix.io/v1alpha1 resources to etcd-operator.cozystack.io/v1alpha2",
		Long: `etcd-migrate adopts running legacy etcd-operator clusters (EtcdCluster,
EtcdBackup, EtcdBackupSchedule of group etcd.aenix.io/v1alpha1) onto
etcd-operator.cozystack.io/v1alpha2 IN PLACE: the etcd pods, their PVCs and
Services stay exactly as they are; only ownership, labels and CRs change.

Run it with BOTH operator Deployments scaled to zero (the etcd pods keep
serving traffic throughout). By default it is a dry-run that inspects each
live cluster and prints the planned manifests and steps; --apply executes
the adoption. Scale the NEW operator up afterwards.

Safety: before anything is mutated, each cluster is snapshotted to the
--backup-s3-*/--backup-pvc-* destination. Nothing is restored from the
artifact — it exists for disaster recovery. Skipping it requires an
explicit --skip-backup.`,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := cfg.validate(); err != nil {
				return err
			}
			return runMigration(cmd.Context(), cfg, os.Stdin, os.Stdout)
		},
	}
	bindFlags(rootCmd, cfg)
	// A `version` subcommand rather than a --version flag: --version is already
	// taken by the etcd-version override (see bindFlags).
	rootCmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "Print the etcd-migrate binary version",
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			_, _ = fmt.Fprintln(cmd.OutOrStdout(), version)
		},
	})
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// newScheme registers everything the tool writes: the v1alpha2 types plus
// the core/batch/rbac kinds used for Secrets, Jobs and printed manifests.
func newScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	if err := lll.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return scheme, nil
}

// runMigration is the top-level flow: clients → gate → discover → plan →
// render → (confirm → snapshot → apply).
func runMigration(ctx context.Context, cfg *Config, stdin io.Reader, stdout io.Writer) error {
	restCfg, err := clientcmd.BuildConfigFromFlags("", cfg.Kubeconfig)
	if err != nil {
		return fmt.Errorf("error building kubeconfig: %w", err)
	}
	kube, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("error creating Kubernetes client: %w", err)
	}
	dyn, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		return fmt.Errorf("error creating dynamic client: %w", err)
	}
	scheme, err := newScheme()
	if err != nil {
		return err
	}
	ctrlClient, err := client.New(restCfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("error creating controller-runtime client: %w", err)
	}

	// Safety gate: neither generation of the operator may be running.
	if !cfg.SkipControllerCheck {
		legacyNS, legacyName, _ := splitRef(cfg.LegacyController)
		newNS, newName, _ := splitRef(cfg.NewController)
		if err := checkControllersDown(ctx, kube, []deployRef{
			{Namespace: legacyNS, Name: legacyName},
			{Namespace: newNS, Name: newName},
		}); err != nil {
			return err
		}
		fmt.Fprintln(stdout, "✓ both operator Deployments are down")
	} else {
		fmt.Fprintln(stdout, "! controller check skipped (--skip-controller-check)")
	}

	d, err := discover(ctx, dyn, cfg.Namespace)
	if err != nil {
		return err
	}
	if len(d.Clusters)+len(d.Backups)+len(d.Schedules) == 0 {
		fmt.Fprintln(stdout, "no legacy etcd.aenix.io resources found — nothing to migrate")
		printCRDNotice(stdout)
		return nil
	}
	fmt.Fprintf(stdout, "discovered %d EtcdCluster, %d EtcdBackup, %d EtcdBackupSchedule (legacy)\n\n",
		len(d.Clusters), len(d.Backups), len(d.Schedules))

	// Inspect every live cluster (read-only: MemberList + AuthStatus over a
	// port-forward, pod/PVC reads). Runs in dry-run too, so the rendered
	// plan shows the real cluster ID and member IDs.
	facts := map[string]migrate.ClusterFacts{}
	inspectErrs := map[string]error{}
	for _, lc := range d.Clusters {
		f, ierr := inspectCluster(ctx, restCfg, kube, lc)
		if ierr != nil {
			inspectErrs[lc.Namespace+"/"+lc.Name] = ierr
			continue
		}
		facts[lc.Namespace+"/"+lc.Name] = f
		fmt.Fprintf(stdout, "inspected %s/%s: clusterID=%s, %d members, auth=%v\n",
			lc.Namespace, lc.Name, f.ClusterIDHex, len(f.Members), f.AuthEnabled)
	}
	fmt.Fprintln(stdout)

	plans := buildPlans(d, facts, inspectErrs, migrate.TranslateOptions{
		VersionOverride: cfg.Version,
		AuthSecretName:  cfg.AuthSecret,
	})
	for i := range plans {
		if plans[i].Action == migrate.ActionAdopt {
			verifyAdoptionPVCs(ctx, kube, plans[i].Namespace, &plans[i])
		}
	}
	if err := markExisting(ctx, ctrlClient, plans); err != nil {
		return err
	}
	if !cfg.backupConfigured() && !cfg.SkipBackup {
		for i := range plans {
			if plans[i].Action == migrate.ActionAdopt {
				plans[i].Warnings = append(plans[i].Warnings,
					"no backup destination configured; --apply will require --backup-s3-*/--backup-pvc-* or an explicit --skip-backup")
			}
		}
	}

	render(stdout, plans)

	if !cfg.Apply {
		fmt.Fprintln(stdout, "\nDry-run complete: nothing was changed. Re-run with --apply to execute the plan.")
		printCRDNotice(stdout)
		return errorIfPlanFailed(plans)
	}

	if !cfg.Yes && !confirm(stdin, stdout,
		"\nThis will ADOPT the clusters above in place (re-own pods/PVCs/Services, replace the legacy CRs; pods keep running). Proceed?") {
		return fmt.Errorf("aborted")
	}

	var backup func() error
	if cfg.backupConfigured() {
		backup = func() error { return runBackups(ctx, cfg, restCfg, kube, ctrlClient, plans, d, stdout) }
	} else {
		fmt.Fprintln(stdout, "! pre-adoption backup skipped (--skip-backup)")
	}

	stats, err := runMutationPhases(
		func() error { return disableAuthForAdoptions(ctx, restCfg, kube, plans, d, facts, stdout) },
		backup,
		func() (applyStats, error) { return applyPlans(ctx, ctrlClient, dyn, plans, stdout) },
	)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "\ndone: %d adopted, %d created, %d legacy CRs deleted, %d skipped (already migrated), %d print-only, %d errored\n",
		stats.Adopted, stats.Created, stats.Deleted, stats.Skipped, stats.Printed, stats.Errored)
	if stats.Adopted > 0 {
		fmt.Fprintln(stdout, "\nNEXT: scale the new operator up — it will take over the adopted clusters without touching the pods:\n  kubectl -n "+
			mustNamespace(cfg.NewController)+" scale deploy "+mustName(cfg.NewController)+" --replicas=1")
	}
	printCRDNotice(stdout)
	return errorIfPlanFailed(plans)
}

// runMutationPhases runs the post-confirmation adoption phases in the order
// their inter-phase contracts REQUIRE, and returns the apply stats:
//
//  1. authDisable — switch auth off on every auth-enabled legacy etcd.
//  2. backup      — snapshot each to-be-adopted cluster (nil ⇒ --skip-backup).
//  3. apply       — re-own the data plane and create the new CRs.
//
// Auth-disable MUST precede backup: the snapshot Job dials etcd anonymously
// (cert-only, no user), and etcd gates the Maintenance Snapshot RPC behind
// auth when it is enabled — so for an auth-enabled cluster (the Cozystack/
// Kamaji case) the backup can only succeed once auth is off. Running them in
// the reverse order silently flips exactly those clusters to ActionError and
// excludes them from adoption. A cluster whose auth-disable fails is itself
// flipped to ActionError and then skipped by the backup and apply phases, so
// an unprotected cluster is never adopted.
func runMutationPhases(authDisable func() error, backup func() error, apply func() (applyStats, error)) (applyStats, error) {
	if err := authDisable(); err != nil {
		return applyStats{}, err
	}
	if backup != nil {
		if err := backup(); err != nil {
			return applyStats{}, err
		}
	}
	return apply()
}

// mustNamespace/mustName split a pre-validated namespace/name ref.
func mustNamespace(ref string) string { ns, _, _ := splitRef(ref); return ns }
func mustName(ref string) string      { _, n, _ := splitRef(ref); return n }

// disableAuthForAdoptions runs `auth disable` on every to-be-adopted cluster
// whose live etcd reports auth enabled. Idempotent (already-off is a no-op).
func disableAuthForAdoptions(ctx context.Context, restCfg *rest.Config, kube kubernetes.Interface,
	plans []migrate.ResourcePlan, d discovered, facts map[string]migrate.ClusterFacts, out io.Writer) error {
	specs := map[string]legacyCluster{}
	for _, lc := range d.Clusters {
		specs[lc.Namespace+"/"+lc.Name] = lc
	}
	for i := range plans {
		p := &plans[i]
		if p.SourceKind != "EtcdCluster" || p.Action != migrate.ActionAdopt {
			continue
		}
		key := p.Namespace + "/" + p.SourceName
		if !facts[key].AuthEnabled {
			continue
		}
		lc := specs[key]
		fmt.Fprintf(out, "disabling auth on legacy etcd %s …\n", key)
		if err := disableLegacyAuth(ctx, restCfg, kube, lc); err != nil {
			p.Action = migrate.ActionError
			p.Errors = append(p.Errors, fmt.Sprintf("auth disable failed: %v", err))
			fmt.Fprintf(out, "  ERROR: %v — cluster left untouched\n", err)
		}
	}
	return nil
}

// errorIfPlanFailed makes the process exit non-zero when any resource could
// not be migrated, so scripts and CI can gate on it.
func errorIfPlanFailed(plans []migrate.ResourcePlan) error {
	errored := 0
	for i := range plans {
		if plans[i].Action == migrate.ActionError {
			errored++
		}
	}
	if errored > 0 {
		return fmt.Errorf("%d resource(s) could not be migrated — see the errors above", errored)
	}
	return nil
}
