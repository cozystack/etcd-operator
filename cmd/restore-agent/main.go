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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	defaultDataDir     = "/var/run/etcd/default.etcd"
	restoreSnapshotDir = "/restore"
	snapshotFilename   = "snapshot.db"
)

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

	memberDir := filepath.Join(dataDir, "member")
	if _, err := os.Stat(memberDir); err == nil {
		fmt.Println("data directory already exists, skipping restore download")
		return nil
	}

	source := os.Getenv("RESTORE_SOURCE")
	switch source {
	case "s3":
		return downloadFromS3()
	case "pvc":
		return copyFromPVC()
	default:
		return fmt.Errorf("unknown RESTORE_SOURCE: %q (expected 's3' or 'pvc')", source)
	}
}

func getTimeout() time.Duration {
	if v := os.Getenv("RESTORE_TIMEOUT_MINUTES"); v != "" {
		if mins, err := strconv.Atoi(v); err == nil && mins > 0 {
			return time.Duration(mins) * time.Minute
		}
	}
	return 10 * time.Minute
}

func downloadFromS3() error {
	ctx, cancel := context.WithTimeout(context.Background(), getTimeout())
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
	backupPath := os.Getenv("PVC_BACKUP_PATH")
	if backupPath == "" {
		return fmt.Errorf("PVC_BACKUP_PATH is required")
	}

	fmt.Printf("copying snapshot from %s\n", backupPath)
	src, err := os.Open(backupPath)
	if err != nil {
		return fmt.Errorf("failed to open backup file %s: %w", backupPath, err)
	}
	defer func() {
		_ = src.Close()
	}()

	return writeSnapshot(src)
}

func writeSnapshot(reader io.Reader) error {
	snapshotPath := filepath.Join(restoreSnapshotDir, snapshotFilename)

	if err := os.MkdirAll(restoreSnapshotDir, 0755); err != nil {
		return fmt.Errorf("failed to create restore directory: %w", err)
	}

	f, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("failed to create snapshot file %s: %w", snapshotPath, err)
	}

	written, err := io.Copy(f, reader)
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to write snapshot: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("failed to sync snapshot to disk: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close snapshot file: %w", err)
	}

	fmt.Printf("snapshot downloaded successfully (%d bytes) to %s\n", written, snapshotPath)
	return nil
}
