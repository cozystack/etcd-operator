/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package migrate

import (
	"testing"

	corev1 "k8s.io/api/core/v1"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/internal/migrate/legacy"
)

// TestBuildSnapshotJob pins the Job wiring against the agent's env contract
// and the LEGACY secret layout (separate CA secret, legacy client service).
func TestBuildSnapshotJob(t *testing.T) {
	spec := legacy.EtcdClusterSpec{
		Security: &legacy.SecuritySpec{
			EnableAuth: true,
			TLS: legacy.TLSSpec{
				ServerSecret:          "srv",
				ServerTrustedCASecret: "srv-ca",
				ClientSecret:          "op-client",
			},
		},
	}
	dest := lll.SnapshotLocation{S3: &lll.S3SnapshotLocation{
		Endpoint: "https://minio", Bucket: "b", Key: "k", Region: "r", ForcePathStyle: true,
		CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
	}}

	job := BuildSnapshotJob("my-etcd", "ns", "uid-1", spec, dest, "ghcr.io/op:1")

	if job.Name != "my-etcd-migration-snapshot" || job.Namespace != "ns" {
		t.Errorf("job identity = %s/%s", job.Namespace, job.Name)
	}
	ctr := job.Spec.Template.Spec.Containers[0]
	if ctr.Image != "ghcr.io/op:1" || ctr.Command[1] != "snapshot-agent" {
		t.Errorf("container = %s %v", ctr.Image, ctr.Command)
	}

	env := map[string]string{}
	for _, e := range ctr.Env {
		env[e.Name] = e.Value
	}
	// The endpoint is the LEGACY client service (named after the cluster),
	// https because serverSecret is set.
	if env["ETCD_ENDPOINTS"] != "https://my-etcd.ns.svc:2379" {
		t.Errorf("ETCD_ENDPOINTS = %q", env["ETCD_ENDPOINTS"])
	}
	if env["SNAPSHOT_NAME"] != "ns-my-etcd-migration" || env["SNAPSHOT_UID"] != "uid-1" {
		t.Errorf("snapshot identity env = %q/%q", env["SNAPSHOT_NAME"], env["SNAPSHOT_UID"])
	}
	if env["S3_ENDPOINT"] != "https://minio" || env["S3_BUCKET"] != "b" || env["S3_KEY"] != "k" ||
		env["S3_REGION"] != "r" || env["S3_FORCE_PATH_STYLE"] != "true" {
		t.Errorf("S3 env = %v", env)
	}
	// NO auth env: the legacy root is NoPassword and auth is disabled
	// before the Job runs.
	for _, e := range ctr.Env {
		if e.Name == "ETCD_USERNAME" || e.Name == "ETCD_PASSWORD" {
			t.Errorf("snapshot Job must dial anonymously, found %s", e.Name)
		}
	}
	if env["ETCD_TLS_CA_PATH"] == "" || env["ETCD_TLS_CERT_PATH"] == "" {
		t.Errorf("TLS env missing: %v", env)
	}

	// The CA mount must come from the SEPARATE legacy CA secret.
	mounted := map[string]string{}
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Secret != nil {
			mounted[v.Name] = v.Secret.SecretName
		}
	}
	if mounted["etcd-ca"] != "srv-ca" {
		t.Errorf("CA volume from %q, want srv-ca (legacy separate CA secret)", mounted["etcd-ca"])
	}
	if mounted["etcd-client"] != "op-client" {
		t.Errorf("client volume from %q, want op-client", mounted["etcd-client"])
	}
}

// TestBuildSnapshotJob_PlaintextAndCAFallback covers the plaintext endpoint
// scheme and the post-merge layout where the CA lives inside the server
// secret itself.
func TestBuildSnapshotJob_PlaintextAndCAFallback(t *testing.T) {
	t.Run("plaintext", func(t *testing.T) {
		job := BuildSnapshotJob("c", "ns", "u", legacy.EtcdClusterSpec{},
			lll.SnapshotLocation{PVC: &lll.PVCSnapshotLocation{ClaimName: "claim"}}, "img")
		for _, e := range job.Spec.Template.Spec.Containers[0].Env {
			if e.Name == "ETCD_ENDPOINTS" && e.Value != "http://c.ns.svc:2379" {
				t.Errorf("ETCD_ENDPOINTS = %q, want plaintext", e.Value)
			}
			if e.Name == "ETCD_TLS_CA_PATH" {
				t.Error("plaintext cluster must not mount TLS material")
			}
		}
	})

	t.Run("ca falls back to server secret", func(t *testing.T) {
		spec := legacy.EtcdClusterSpec{Security: &legacy.SecuritySpec{
			TLS: legacy.TLSSpec{ServerSecret: "srv"}, // no separate CA secret
		}}
		job := BuildSnapshotJob("c", "ns", "u", spec,
			lll.SnapshotLocation{PVC: &lll.PVCSnapshotLocation{ClaimName: "claim"}}, "img")
		for _, v := range job.Spec.Template.Spec.Volumes {
			if v.Name == "etcd-ca" && v.Secret.SecretName != "srv" {
				t.Errorf("CA volume from %q, want srv", v.Secret.SecretName)
			}
		}
	})
}
