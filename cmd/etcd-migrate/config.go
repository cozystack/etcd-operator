/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"k8s.io/client-go/util/homedir"
)

// defaultLegacyControllerRef is where the legacy v1alpha1 repo's kustomize
// install deploys its controller.
const defaultLegacyControllerRef = "etcd-operator-system/etcd-operator-controller-manager"

// defaultNewControllerRef is where this repo's Helm chart deploys the operator —
// release name "etcd-operator" in the etcd-operator-system namespace. (The
// generations no longer share a name: kustomize named it
// etcd-operator-controller-manager; the chart names it after the release.)
const defaultNewControllerRef = "etcd-operator-system/etcd-operator"

// Config holds every flag of the migrate CLI.
type Config struct {
	Kubeconfig string
	Namespace  string // "" = all namespaces

	Apply               bool
	Yes                 bool
	SkipControllerCheck bool
	LegacyController    string // ns/name
	NewController       string // ns/name

	Version    string // etcd version override for every cluster
	AuthSecret string // existing basic-auth Secret name for auth clusters

	// Backup phase: a safety snapshot of every cluster taken right before
	// adoption. Nothing is restored from it — the data stays in place — but
	// adoption rewires ownership of live storage, so "no backup" must be an
	// explicit choice (--skip-backup), not a forgotten flag.
	SkipBackup    bool
	AgentImage    string
	BackupTimeout time.Duration

	BackupS3Endpoint          string
	BackupS3Bucket            string
	BackupS3Key               string
	BackupS3Region            string
	BackupS3ForcePathStyle    bool
	BackupS3CredentialsSecret string

	BackupPVCClaim   string
	BackupPVCSubPath string
}

// bindFlags registers every flag on the root command.
func bindFlags(cmd *cobra.Command, cfg *Config) {
	defaultKubeconfig := os.Getenv("KUBECONFIG")
	if defaultKubeconfig == "" {
		defaultKubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}

	f := cmd.PersistentFlags()
	f.StringVarP(&cfg.Kubeconfig, "kubeconfig", "k", defaultKubeconfig, "Path to the kubeconfig file")
	f.StringVarP(&cfg.Namespace, "namespace", "n", "", "Namespace to migrate (default: all namespaces)")
	f.BoolVar(&cfg.Apply, "apply", false, "Execute the adoption. Without it the tool only prints the plan (dry-run).")
	f.BoolVarP(&cfg.Yes, "yes", "y", false, "Skip the interactive confirmation before --apply mutates the cluster")
	f.BoolVar(&cfg.SkipControllerCheck, "skip-controller-check", false, "Skip verifying that both operator Deployments are scaled down")
	f.StringVar(&cfg.LegacyController, "legacy-controller", defaultLegacyControllerRef, "Legacy operator Deployment as namespace/name")
	f.StringVar(&cfg.NewController, "new-controller", defaultNewControllerRef, "New operator Deployment as namespace/name")
	f.StringVar(&cfg.Version, "version", "", "etcd version (X.Y.Z) to set on every migrated cluster, overriding image-tag extraction")
	f.StringVar(&cfg.AuthSecret, "auth-secret", "", "Existing kubernetes.io/basic-auth Secret (in each cluster's namespace) to reference for clusters with enableAuth; default generates one per cluster")

	f.BoolVar(&cfg.SkipBackup, "skip-backup", false, "Skip the pre-adoption safety snapshot (NOT recommended)")
	f.StringVar(&cfg.AgentImage, "agent-image", "", "Operator image carrying the snapshot agent (default: taken from the new controller Deployment's spec)")
	f.DurationVar(&cfg.BackupTimeout, "backup-timeout", 30*time.Minute, "How long to wait for each backup Job")
	f.StringVar(&cfg.BackupS3Endpoint, "backup-s3-endpoint", "", "S3 endpoint for backup storage")
	f.StringVar(&cfg.BackupS3Bucket, "backup-s3-bucket", "", "S3 bucket for backup storage")
	f.StringVar(&cfg.BackupS3Key, "backup-s3-key", "", "S3 key prefix for backup storage")
	f.StringVar(&cfg.BackupS3Region, "backup-s3-region", "", "S3 region")
	f.BoolVar(&cfg.BackupS3ForcePathStyle, "backup-s3-force-path-style", false, "Use path-style S3 addressing (MinIO/Ceph)")
	f.StringVar(&cfg.BackupS3CredentialsSecret, "backup-s3-credentials-secret", "", "Secret with AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY, required in EVERY migrated cluster's namespace")
	f.StringVar(&cfg.BackupPVCClaim, "backup-pvc-claim", "", "PVC for backup storage, required in EVERY migrated cluster's namespace (mutually exclusive with the s3 flags)")
	f.StringVar(&cfg.BackupPVCSubPath, "backup-pvc-subpath", "", "Subdirectory within the backup PVC")
}

// validate cross-checks the flag set.
func (cfg *Config) validate() error {
	if _, _, err := splitRef(cfg.LegacyController); err != nil {
		return fmt.Errorf("--legacy-controller: %w", err)
	}
	if _, _, err := splitRef(cfg.NewController); err != nil {
		return fmt.Errorf("--new-controller: %w", err)
	}

	s3 := cfg.BackupS3Endpoint != "" || cfg.BackupS3Bucket != "" || cfg.BackupS3CredentialsSecret != ""
	pvc := cfg.BackupPVCClaim != ""
	switch {
	case cfg.SkipBackup && (s3 || pvc):
		return fmt.Errorf("--skip-backup contradicts the --backup-* destination flags; drop one side")
	case cfg.SkipBackup:
		return nil
	case s3 && pvc:
		return fmt.Errorf("--backup-s3-* and --backup-pvc-* are mutually exclusive")
	case !s3 && !pvc:
		// The dry-run is allowed to proceed without a destination so users
		// can review the plan first; --apply is not.
		if cfg.Apply {
			return fmt.Errorf("adoption rewires ownership of live etcd storage; provide a backup destination " +
				"(--backup-s3-{endpoint,bucket,credentials-secret} or --backup-pvc-claim) or opt out explicitly with --skip-backup")
		}
		return nil
	case s3 && (cfg.BackupS3Endpoint == "" || cfg.BackupS3Bucket == "" || cfg.BackupS3CredentialsSecret == ""):
		return fmt.Errorf("S3 backup destination needs all of --backup-s3-endpoint, --backup-s3-bucket and --backup-s3-credentials-secret")
	}
	return nil
}

// backupConfigured reports whether a backup destination is set.
func (cfg *Config) backupConfigured() bool {
	return !cfg.SkipBackup && (cfg.BackupS3Endpoint != "" || cfg.BackupPVCClaim != "")
}

// splitRef parses a "namespace/name" flag value.
func splitRef(ref string) (namespace, name string, err error) {
	parts := strings.Split(ref, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("%q is not of the form namespace/name", ref)
	}
	return parts[0], parts[1], nil
}
