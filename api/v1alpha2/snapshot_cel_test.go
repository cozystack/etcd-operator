/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha2_test

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

func s3Source() lll.SnapshotLocation {
	return lll.SnapshotLocation{S3: &lll.S3SnapshotLocation{
		Endpoint:             "https://minio.svc:9000",
		Bucket:               "etcd",
		Key:                  "snapshots/snap.db", // exact key — required for a restore source
		CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
	}}
}

func pvcSource() lll.SnapshotLocation {
	// subPath is the exact snapshot file — required for a restore source.
	return lll.SnapshotLocation{PVC: &lll.PVCSnapshotLocation{ClaimName: "snap-pvc", SubPath: "snap.db"}}
}

func TestCEL_SnapshotLocationExactlyOne(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	t.Run("both rejected", func(t *testing.T) {
		b := &lll.EtcdSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "both", Namespace: "default"},
			Spec: lll.EtcdSnapshotSpec{
				ClusterRef: corev1.LocalObjectReference{Name: "c1"},
				Destination: lll.SnapshotLocation{
					S3:  s3Source().S3,
					PVC: pvcSource().PVC,
				},
			},
		}
		err := k8s.Create(ctx, b)
		if err == nil {
			_ = k8s.Delete(ctx, b)
			t.Fatal("apiserver accepted both s3 and pvc; expected rejection")
		}
		if !strings.Contains(err.Error(), "exactly one of destination.s3 or destination.pvc") {
			t.Fatalf("error did not mention exactly-one rule: %v", err)
		}
	})

	t.Run("neither rejected", func(t *testing.T) {
		b := &lll.EtcdSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "neither", Namespace: "default"},
			Spec:       lll.EtcdSnapshotSpec{ClusterRef: corev1.LocalObjectReference{Name: "c1"}},
		}
		err := k8s.Create(ctx, b)
		if err == nil {
			_ = k8s.Delete(ctx, b)
			t.Fatal("apiserver accepted empty destination; expected rejection")
		}
		if !strings.Contains(err.Error(), "exactly one of destination.s3 or destination.pvc") {
			t.Fatalf("error did not mention exactly-one rule: %v", err)
		}
	})

	t.Run("s3 only accepted", func(t *testing.T) {
		b := &lll.EtcdSnapshot{
			ObjectMeta: metav1.ObjectMeta{Name: "s3-only", Namespace: "default"},
			Spec: lll.EtcdSnapshotSpec{
				ClusterRef:  corev1.LocalObjectReference{Name: "c1"},
				Destination: s3Source(),
			},
		}
		if err := k8s.Create(ctx, b); err != nil {
			t.Fatalf("s3-only destination rejected unexpectedly: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, b) })
	})
}

// A restore S3 source addresses one exact object; an empty key must be
// rejected by the apiserver rather than failing opaquely in the seed.
func TestCEL_RestoreS3KeyRequired(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	t.Run("empty key rejected", func(t *testing.T) {
		c := validCluster("restore-nokey")
		c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: lll.SnapshotLocation{
			S3: &lll.S3SnapshotLocation{
				Endpoint:             "https://minio.svc:9000",
				Bucket:               "etcd",
				CredentialsSecretRef: corev1.LocalObjectReference{Name: "s3-creds"},
				// Key intentionally omitted.
			},
		}}}
		err := k8s.Create(ctx, c)
		if err == nil {
			_ = k8s.Delete(ctx, c)
			t.Fatal("apiserver accepted a restore source with empty s3.key; expected rejection")
		}
		if !strings.Contains(err.Error(), "exact (non-empty) object key") {
			t.Fatalf("error did not mention the exact-key rule: %v", err)
		}
	})

	t.Run("non-empty key accepted", func(t *testing.T) {
		c := validCluster("restore-withkey")
		c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: s3Source()}}
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("restore source with explicit key rejected unexpectedly: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
	})

	t.Run("pvc source with subPath accepted", func(t *testing.T) {
		c := validCluster("restore-pvc")
		c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: pvcSource()}}
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("pvc restore source rejected unexpectedly: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
	})
}

// Restore onto memory-backed (tmpfs) storage is a footgun: a seed Pod restart
// wipes the data dir and the agent re-restores the snapshot. The apiserver
// must reject the combination.
func TestCEL_RestoreWithMemoryStorageRejected(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	t.Run("memory + restore rejected", func(t *testing.T) {
		c := validCluster("restore-mem")
		c.Spec.Storage.Medium = lll.StorageMediumMemory
		c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: s3Source()}}
		err := k8s.Create(ctx, c)
		if err == nil {
			_ = k8s.Delete(ctx, c)
			t.Fatal("apiserver accepted restore + medium=Memory; expected rejection")
		}
		if !strings.Contains(err.Error(), "unsupported with spec.storage.medium=Memory") {
			t.Fatalf("error did not mention the memory/restore rule: %v", err)
		}
	})

	t.Run("memory without restore still accepted", func(t *testing.T) {
		c := validCluster("mem-norestore")
		c.Spec.Storage.Medium = lll.StorageMediumMemory
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("memory cluster without restore rejected unexpectedly: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
	})

	t.Run("restore on persistent storage accepted", func(t *testing.T) {
		c := validCluster("restore-persistent")
		c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: s3Source()}}
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("restore on persistent storage rejected unexpectedly: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
	})
}

// Symmetric to the S3 key rule: a PVC restore source must name the exact
// snapshot file via subPath; an empty subPath would resolve to the mount dir.
func TestCEL_RestorePVCSubPathRequired(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	c := validCluster("restore-pvc-nosubpath")
	c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: lll.SnapshotLocation{
		PVC: &lll.PVCSnapshotLocation{ClaimName: "snap-pvc"}, // subPath intentionally omitted
	}}}
	err := k8s.Create(ctx, c)
	if err == nil {
		_ = k8s.Delete(ctx, c)
		t.Fatal("apiserver accepted a PVC restore source with empty subPath; expected rejection")
	}
	if !strings.Contains(err.Error(), "exact (non-empty) snapshot file path") {
		t.Fatalf("error did not mention the subPath rule: %v", err)
	}
}

func TestCEL_BootstrapImmutable(t *testing.T) {
	skipIfNoEnvtest(t)
	ctx := context.Background()

	t.Run("cannot add bootstrap to existing cluster", func(t *testing.T) {
		c := validCluster("boot-add")
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("Create: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

		got := &lll.EtcdCluster{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		got.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: s3Source()}}
		err := k8s.Update(ctx, got)
		if err == nil {
			t.Fatal("apiserver accepted adding spec.bootstrap; expected rejection")
		}
		if !strings.Contains(err.Error(), "spec.bootstrap") {
			t.Fatalf("error did not mention spec.bootstrap: %v", err)
		}
	})

	t.Run("cannot mutate bootstrap on existing cluster", func(t *testing.T) {
		c := validCluster("boot-mutate")
		c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: s3Source()}}
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("Create with bootstrap: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })

		got := &lll.EtcdCluster{}
		if err := k8s.Get(ctx, ctrlclient.ObjectKeyFromObject(c), got); err != nil {
			t.Fatalf("Get: %v", err)
		}
		got.Spec.Bootstrap.Restore.Source = pvcSource()
		if err := k8s.Update(ctx, got); err == nil {
			t.Fatal("apiserver accepted mutating spec.bootstrap; expected rejection")
		}
	})

	t.Run("create with bootstrap accepted", func(t *testing.T) {
		c := validCluster("boot-create")
		c.Spec.Bootstrap = &lll.BootstrapSpec{Restore: &lll.RestoreSpec{Source: s3Source()}}
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("Create with bootstrap rejected unexpectedly: %v", err)
		}
		t.Cleanup(func() { _ = k8s.Delete(ctx, c) })
	})
}
