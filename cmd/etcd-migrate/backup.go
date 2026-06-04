/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"context"
	"fmt"
	"io"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/etcdclient"
	"github.com/cozystack/etcd-operator/internal/migrate"
	"github.com/cozystack/etcd-operator/internal/portforward"
)

// jobPollInterval is how often waitForJob re-reads the snapshot Job.
const jobPollInterval = 5 * time.Second

// runBackups executes the per-cluster safety-backup phase before anything is
// mutated: a one-off agent Job snapshots each to-be-adopted cluster to the
// configured destination. Nothing is restored from the artifact — the data
// stays in place — it exists so a botched adoption is recoverable. A failure
// flips that cluster's plan to ActionError so the adoption never starts for
// an unprotected cluster.
func runBackups(ctx context.Context, cfg *Config, restCfg *rest.Config, kube kubernetes.Interface,
	c client.Client, plans []migrate.ResourcePlan, d discovered, out io.Writer) error {

	agentImage, err := resolveAgentImage(ctx, cfg, kube)
	if err != nil {
		return err
	}
	dest := backupDestination(cfg)

	specs := map[string]legacyCluster{}
	for _, lc := range d.Clusters {
		specs[lc.Namespace+"/"+lc.Name] = lc
	}

	for i := range plans {
		p := &plans[i]
		if p.SourceKind != "EtcdCluster" || p.Action != migrate.ActionAdopt {
			continue
		}
		lc, ok := specs[p.Namespace+"/"+p.SourceName]
		if !ok {
			continue
		}

		fmt.Fprintf(out, "backing up legacy cluster %s/%s …\n", lc.Namespace, lc.Name)
		if err := backupOne(ctx, cfg, kube, c, lc, dest, agentImage, out); err != nil {
			p.Action = migrate.ActionError
			p.Errors = append(p.Errors, fmt.Sprintf("backup failed: %v", err))
			p.DeleteRef = nil
			fmt.Fprintf(out, "  ERROR: %v — cluster left untouched\n", err)
			continue
		}
		fmt.Fprintf(out, "  backup stored\n")
	}
	return nil
}

// backupOne handles a single legacy cluster's one-off agent Job. Idempotent:
// a completed Job from a previous run is reused (the agent's SNAPSHOT_UID
// overwrite guard recognizes its own artifact), a failed or stale one is
// replaced. Note the Job's etcdctl dial is anonymous — for auth-enabled
// clusters the auth-disable phase runs BEFORE this one.
func backupOne(ctx context.Context, cfg *Config, kube kubernetes.Interface,
	c client.Client, lc legacyCluster, dest lll.SnapshotLocation, agentImage string, out io.Writer) error {
	_ = kube // parity with backup destinations that may need lookups later

	job := migrate.BuildSnapshotJob(lc.Name, lc.Namespace, lc.UID, lc.Spec, dest, agentImage)

	existing := &batchv1.Job{}
	getErr := c.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, existing)
	switch {
	case getErr == nil && jobSucceeded(existing):
		fmt.Fprintf(out, "  snapshot Job already completed — reusing its artifact\n")
		return nil
	case getErr == nil:
		// Leftover from a previous failed/interrupted attempt: replace it.
		policy := metav1.DeletePropagationForeground
		if err := c.Delete(ctx, existing, &client.DeleteOptions{PropagationPolicy: &policy}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete stale snapshot Job: %w", err)
		}
		if err := waitJobGone(ctx, c, job.Namespace, job.Name, 2*time.Minute); err != nil {
			return err
		}
	case !apierrors.IsNotFound(getErr):
		return fmt.Errorf("read snapshot Job: %w", getErr)
	}

	if err := c.Create(ctx, job); err != nil {
		return fmt.Errorf("create snapshot Job: %w", err)
	}
	return waitForJob(ctx, c, job.Namespace, job.Name, cfg.BackupTimeout)
}

// jobSucceeded reports a Complete=True condition.
func jobSucceeded(job *batchv1.Job) bool {
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// waitJobGone polls until the Job (deleted with foreground propagation)
// disappears, so a same-name recreate cannot race its terminating pods.
func waitJobGone(ctx context.Context, c client.Client, namespace, name string, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &batchv1.Job{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("await snapshot Job deletion: %w", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("stale snapshot Job %s/%s did not terminate within %s", namespace, name, timeout)
		case <-time.After(jobPollInterval):
		}
	}
}

// resolveAgentImage returns --agent-image, falling back to the image in the
// NEW controller Deployment's spec — readable even at spec.replicas=0. With
// neither available the snapshot phase cannot run.
func resolveAgentImage(ctx context.Context, cfg *Config, kube kubernetes.Interface) (string, error) {
	if cfg.AgentImage != "" {
		return cfg.AgentImage, nil
	}
	ns, name, _ := splitRef(cfg.NewController)
	dep, err := kube.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("the backup phase needs the agent image: the new controller Deployment %s/%s does not exist, pass --agent-image explicitly", ns, name)
		}
		return "", fmt.Errorf("read new controller Deployment %s/%s: %w", ns, name, err)
	}
	for _, ctr := range dep.Spec.Template.Spec.Containers {
		if ctr.Name == "manager" {
			return ctr.Image, nil
		}
	}
	if len(dep.Spec.Template.Spec.Containers) > 0 {
		return dep.Spec.Template.Spec.Containers[0].Image, nil
	}
	return "", fmt.Errorf("new controller Deployment %s/%s has no containers; pass --agent-image", ns, name)
}

// disableLegacyAuth turns authentication off on the still-running legacy
// etcd so the snapshot carries no auth state. The legacy root user is
// NoPassword (cert-only), so the dial authenticates with the legacy
// operator's client certificate over a port-forward to a member Pod — the
// same identity the legacy operator itself used for auth management.
func disableLegacyAuth(ctx context.Context, restCfg *rest.Config, kube kubernetes.Interface, lc legacyCluster) error {
	pod, err := findRunningEtcdPod(ctx, kube, lc)
	if err != nil {
		return err
	}
	localPort, stop, err := portforward.ForwardToPod(restCfg, lc.Namespace, pod, 2379)
	if err != nil {
		return fmt.Errorf("port-forward to %s: %w", pod, err)
	}
	defer stop()

	tlsCfg, err := legacyOperatorTLSConfig(ctx, kube, lc)
	if err != nil {
		return err
	}

	cli, err := etcdclient.New([]string{fmt.Sprintf("localhost:%d", localPort)}, tlsCfg, "", "")
	if err != nil {
		return fmt.Errorf("dial legacy etcd: %w", err)
	}
	defer func() { _ = cli.Close() }()

	statusCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	status, err := cli.AuthStatus(statusCtx)
	if err != nil {
		return fmt.Errorf("read auth status: %w", err)
	}
	if !status.Enabled {
		return nil // already off — nothing to do
	}
	disableCtx, cancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel2()
	if _, err := cli.AuthDisable(disableCtx); err != nil {
		return fmt.Errorf("auth disable: %w", err)
	}
	return nil
}

// findRunningEtcdPod picks one Running member Pod of the legacy cluster
// (label set: app.kubernetes.io/name=etcd + instance=<cluster>).
func findRunningEtcdPod(ctx context.Context, kube kubernetes.Interface, lc legacyCluster) (string, error) {
	pods, err := kube.CoreV1().Pods(lc.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=etcd,app.kubernetes.io/instance=" + lc.Name,
	})
	if err != nil {
		return "", fmt.Errorf("list etcd pods: %w", err)
	}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodRunning {
			return p.Name, nil
		}
	}
	return "", fmt.Errorf("no Running etcd pod found for legacy cluster %s/%s", lc.Namespace, lc.Name)
}

// waitForJob polls until the Job reports Complete or Failed, or the timeout
// elapses.
func waitForJob(ctx context.Context, c client.Client, namespace, name string, timeout time.Duration) error {
	deadline := time.After(timeout)
	for {
		job := &batchv1.Job{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, job); err != nil {
			return fmt.Errorf("read snapshot Job: %w", err)
		}
		for _, cond := range job.Status.Conditions {
			if cond.Status != corev1.ConditionTrue {
				continue
			}
			switch cond.Type {
			case batchv1.JobComplete:
				return nil
			case batchv1.JobFailed:
				return fmt.Errorf("snapshot Job failed: %s: %s", cond.Reason, cond.Message)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return fmt.Errorf("snapshot Job did not finish within %s", timeout)
		case <-time.After(jobPollInterval):
		}
	}
}
