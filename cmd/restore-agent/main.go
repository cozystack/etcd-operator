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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.uber.org/zap"
)

const (
	defaultDataDir   = "/var/run/etcd/default.etcd"
	snapshotFilename = "snapshot.db"
	// restoreCompleteFile is a sentinel written into dataDir after
	// snapshot.Restore() returns nil. The skip-check in run() keys off
	// this file, not member/, because etcdutl's snapshot.Restore creates
	// member/snap/ very early — before the snapshot bytes are copied,
	// before the SHA-256 check, and before the WAL is written. A pod
	// killed mid-restore (OOMKill during io.Copy, ENOSPC, SHA mismatch,
	// eviction) leaves member/ on disk in a partial state; a sentinel-
	// based skip lets the next attempt see "incomplete" and the wipe
	// branch in restoreDataDir() reclaim the dataDir for retry.
	restoreCompleteFile = ".restore-complete"
)

// restoreSnapshotDir is a package-level variable (not const) so tests can override it.
var restoreSnapshotDir = "/restore"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	dataDir := os.Getenv("ETCD_DATA_DIR")
	if dataDir == "" {
		dataDir = defaultDataDir
	}

	sentinel := filepath.Join(dataDir, restoreCompleteFile)
	if _, err := os.Stat(sentinel); err == nil {
		fmt.Println("restore sentinel present, skipping restore")
		return nil
	}

	// Previously a second initContainer (restore-datadir) ran
	// `/bin/sh -c "etcdutl snapshot restore ..."` from the distroless
	// quay.io/coreos/etcd image and crashed because no /bin/sh exists.
	// This binary now does both steps: download + restore via etcdutl's
	// Go API. One initContainer, no shell.
	source := os.Getenv("RESTORE_SOURCE")
	switch source {
	case "s3":
		if err := downloadFromS3(); err != nil {
			return err
		}
	case "pvc":
		if err := copyFromPVC(); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown RESTORE_SOURCE: %q (expected 's3' or 'pvc')", source)
	}

	return restoreDataDir(dataDir)
}

func getTimeout() (time.Duration, error) {
	v := os.Getenv("RESTORE_TIMEOUT_MINUTES")
	if v == "" {
		return 10 * time.Minute, nil
	}
	mins, err := strconv.Atoi(v)
	if err != nil || mins <= 0 {
		return 0, fmt.Errorf("RESTORE_TIMEOUT_MINUTES must be a positive integer, got %q", v)
	}
	return time.Duration(mins) * time.Minute, nil
}

func downloadFromS3() error {
	timeout, err := getTimeout()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	endpoint := os.Getenv("S3_ENDPOINT")
	bucket := os.Getenv("S3_BUCKET")
	key := os.Getenv("S3_KEY")
	region := os.Getenv("S3_REGION")

	if bucket == "" || key == "" {
		return fmt.Errorf("S3_BUCKET and S3_KEY are required")
	}

	if region == "" {
		region = "us-east-1"
	}

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	if accessKey == "" || secretKey == "" {
		return fmt.Errorf("AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are required")
	}

	cfg := aws.Config{
		Region:      region,
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
		if os.Getenv("S3_FORCE_PATH_STYLE") == "true" {
			o.UsePathStyle = true
		}
	})

	fmt.Printf("downloading snapshot from s3://%s/%s\n", bucket, key)
	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to download from S3: %w", err)
	}
	defer func() {
		_ = result.Body.Close()
	}()

	return writeSnapshot(result.Body)
}

func copyFromPVC() error {
	timeout, err := getTimeout()
	if err != nil {
		return err
	}

	backupPath := os.Getenv("PVC_BACKUP_PATH")
	if backupPath == "" {
		return fmt.Errorf("PVC_BACKUP_PATH is required")
	}

	fmt.Printf("copying snapshot from %s\n", backupPath)
	src, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file %s: %w", backupPath, err)
	}

	var closeOnce sync.Once
	closeSrc := func() { closeOnce.Do(func() { _ = src.Close() }) }

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer func() {
		closeSrc()
		cancel()
	}()

	// Close the file descriptor on timeout to unblock any kernel-level
	// read stuck on unresponsive NFS/network-backed storage.
	go func() {
		<-ctx.Done()
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			closeSrc()
		}
	}()

	err = writeSnapshot(src)
	return errors.Join(err, ctx.Err())
}

func writeSnapshot(reader io.Reader) error {
	snapshotPath := filepath.Join(restoreSnapshotDir, snapshotFilename)

	if err := os.MkdirAll(restoreSnapshotDir, 0700); err != nil {
		return fmt.Errorf("failed to create restore directory: %w", err)
	}

	f, err := os.OpenFile(snapshotPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file %s: %w", snapshotPath, err)
	}

	written, err := io.Copy(f, reader)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(snapshotPath)
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	if written == 0 {
		_ = f.Close()
		_ = os.Remove(snapshotPath)
		return fmt.Errorf("downloaded snapshot is empty (0 bytes)")
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(snapshotPath)
		return fmt.Errorf("failed to sync snapshot to disk: %w", err)
	}

	if err := f.Close(); err != nil {
		_ = os.Remove(snapshotPath)
		return fmt.Errorf("failed to close snapshot file: %w", err)
	}

	fmt.Printf("snapshot downloaded successfully (%d bytes) to %s\n", written, snapshotPath)
	return nil
}

// restoreDataDir invokes etcdutl's snapshot.Restore Go API to populate
// the etcd member data directory from the downloaded snapshot. Replaces
// the legacy /bin/sh -c "etcdutl snapshot restore ..." initContainer
// that failed on distroless etcd images.
func restoreDataDir(dataDir string) error {
	snapshotPath := filepath.Join(restoreSnapshotDir, snapshotFilename)
	if _, err := os.Stat(snapshotPath); err != nil {
		return fmt.Errorf("snapshot file missing at %s: %w", snapshotPath, err)
	}

	podName := os.Getenv("POD_NAME")
	if podName == "" {
		return fmt.Errorf("POD_NAME env var is required")
	}
	podNamespace := os.Getenv("POD_NAMESPACE")
	if podNamespace == "" {
		return fmt.Errorf("POD_NAMESPACE env var is required")
	}
	// Both come from the cluster-state ConfigMap via envFrom. Trim them
	// symmetrically — ConfigMap string values that get round-tripped
	// through `kubectl apply -f -` heredocs can pick up trailing
	// whitespace that breaks etcdutl's strict initial-cluster parse.
	initialCluster := strings.TrimSpace(os.Getenv("ETCD_INITIAL_CLUSTER"))
	if initialCluster == "" {
		return fmt.Errorf("ETCD_INITIAL_CLUSTER env var is required (envFrom cluster-state ConfigMap)")
	}
	clusterToken := strings.TrimSpace(os.Getenv("ETCD_INITIAL_CLUSTER_TOKEN"))
	if clusterToken == "" {
		return fmt.Errorf("ETCD_INITIAL_CLUSTER_TOKEN env var is required (envFrom cluster-state ConfigMap)")
	}
	// HEADLESS_SVC is set unconditionally by the operator's statefulset
	// renderer from GetHeadlessServiceName(cluster). Require it here so
	// a missing value surfaces as an explicit error rather than a
	// silently-wrong default that resolves DNS to nothing.
	headlessSvc := os.Getenv("HEADLESS_SVC")
	if headlessSvc == "" {
		return fmt.Errorf("HEADLESS_SVC env var is required")
	}
	peerURL := fmt.Sprintf("https://%s.%s.%s.svc:2380", podName, headlessSvc, podNamespace)

	// Partial-state recovery: dataDir may exist from a previous restore
	// that crashed midway. etcdutl's Restore() creates member/snap/
	// before the SHA-256 check and WAL write, so a failure between
	// member/snap/ creation and the success write leaves the dir
	// half-populated and Restore() refuses to overwrite on retry. The
	// skip-check in run() keys off the .restore-complete sentinel, so
	// reaching this point means the previous attempt did not finish —
	// wipe whatever exists so Restore() can recreate cleanly.
	if _, err := os.Stat(dataDir); err == nil {
		fmt.Printf("partial restore state at %s, wiping for retry\n", dataDir)
		if err := os.RemoveAll(dataDir); err != nil {
			return fmt.Errorf("wipe partial data dir %s: %w", dataDir, err)
		}
	}

	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("create zap logger: %w", err)
	}
	mgr := snapshot.NewV3(logger)
	fmt.Printf("restoring snapshot %s -> %s (name=%s peer=%s)\n", snapshotPath, dataDir, podName, peerURL)
	if err := mgr.Restore(snapshot.RestoreConfig{
		SnapshotPath:        snapshotPath,
		Name:                podName,
		OutputDataDir:       dataDir,
		PeerURLs:            []string{peerURL},
		InitialCluster:      initialCluster,
		InitialClusterToken: clusterToken,
	}); err != nil {
		return fmt.Errorf("etcdutl snapshot.Restore: %w", err)
	}
	// Mark the dataDir as fully restored. run()'s skip-check on the next
	// pod start sees this sentinel and short-circuits; any failure path
	// before this line leaves no sentinel and forces a wipe-and-retry.
	sentinelPath := filepath.Join(dataDir, restoreCompleteFile)
	if err := os.WriteFile(sentinelPath, nil, 0o600); err != nil {
		return fmt.Errorf("write restore-complete sentinel %s: %w", sentinelPath, err)
	}
	fmt.Printf("snapshot restored to %s\n", dataDir)
	return nil
}
