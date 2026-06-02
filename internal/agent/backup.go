/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	smithy "github.com/aws/smithy-go"
)

// backupUIDMetaKey is the S3 user-metadata key stamping which EtcdBackup wrote
// an object. S3 lowercases user-metadata keys, so keep it lowercase.
const backupUIDMetaKey = "etcd-backup-uid"

// backupUIDSuffix names the sidecar file that records which EtcdBackup wrote a
// PVC snapshot. A filesystem has no equivalent of S3 user-metadata, so we stamp
// ownership in a sibling "<name>.db.uid" file — giving the PVC path the same
// self-retry idempotency the S3 path gets from object metadata.
const backupUIDSuffix = ".uid"

// ensureFileAbsent refuses to overwrite an existing PVC snapshot written by a
// different backup, mirroring ensureObjectAbsent for S3: two backups sharing a
// name and PVC destination would otherwise silently clobber each other. A file
// carrying this backup's UID (via its .uid sidecar) is our own from a prior
// attempt and may be overwritten idempotently. Returns nil when the snapshot is
// absent or owned by this backup.
func ensureFileAbsent(finalPath, uid string) error {
	if _, err := os.Stat(finalPath); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("check for existing snapshot file %s: %w", finalPath, err)
	}
	if uid != "" {
		if owner, err := os.ReadFile(finalPath + backupUIDSuffix); err == nil && string(owner) == uid {
			return nil // our own file from a previous attempt — retry is idempotent
		}
	}
	return fmt.Errorf("snapshot file %s already exists and was not written by this backup; refusing to overwrite (use a unique backup name or destination subPath)", finalPath)
}

// headObjectAPI is the slice of the S3 client used to check object existence —
// small enough to fake in tests.
type headObjectAPI interface {
	HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

// ensureObjectAbsent refuses to overwrite an existing snapshot. The S3 object
// key is derived from the EtcdBackup name, so two backups sharing a name (e.g.
// same name across namespaces, or a reused name) would otherwise silently
// clobber an earlier snapshot with no trace.
//
// The one object we MAY overwrite is our own: a Job retry (BackoffLimit) or a
// crash after PutObject but before the marker is read would otherwise find the
// snapshot it just wrote and fail the whole backup. We stamp each object with
// the EtcdBackup UID; an existing object carrying our uid is treated as a
// no-conflict (the re-upload is idempotent). Returns nil when the object is
// absent or owned by this backup.
func ensureObjectAbsent(ctx context.Context, api headObjectAPI, bucket, key, uid string) error {
	out, err := api.HeadObject(ctx, &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err == nil {
		if uid != "" && out.Metadata[backupUIDMetaKey] == uid {
			return nil // our own object from a previous attempt — retry is idempotent
		}
		return fmt.Errorf("snapshot object s3://%s/%s already exists and was not written by this backup; refusing to overwrite (use a unique backup name or destination key)", bucket, key)
	}
	// A genuine "not found" is the success case. Different S3-compatible
	// stores surface it differently: the typed NotFound, or a bare smithy
	// API error with a 404-ish code.
	var nf *s3types.NotFound
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nf) || errors.As(err, &nsk) {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey", "404":
			return nil
		}
	}
	return fmt.Errorf("check for existing snapshot object: %w", err)
}

// RunBackup takes an etcd snapshot and stores it at the configured
// destination, then prints the marker line the EtcdBackup controller scans:
//
//	snapshot uploaded: uri="..." size=N sha256=<hex>
//
// All configuration comes from the environment (see agent.go).
func RunBackup(ctx context.Context) error {
	name := os.Getenv(envBackupName)
	if name == "" {
		name = "snapshot"
	}
	dest, err := loadDestination()
	if err != nil {
		return err
	}

	cli, err := etcdClient()
	if err != nil {
		return fmt.Errorf("connect to etcd: %w", err)
	}
	defer cli.Close()

	rc, err := cli.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("take etcd snapshot: %w", err)
	}
	defer rc.Close()

	var size int64
	var sum string
	switch dest.kind {
	case "pvc":
		// Refuse to clobber a snapshot a different backup wrote (symmetric to
		// the S3 overwrite guard); our own file from a prior attempt is fine.
		finalPath := dest.localPath(name)
		uid := os.Getenv(envBackupUID)
		if err := ensureFileAbsent(finalPath, uid); err != nil {
			return err
		}
		// Write atomically into the destination: stage to a temp file in the
		// same directory, then rename into place. A crashed or interrupted
		// backup never leaves a truncated <name>.db that a later restore
		// would try to load.
		size, sum, err = writeSnapshotAtomic(finalPath, rc)
		if err != nil {
			return err
		}
		// Stamp ownership so a Job retry recognizes its own snapshot instead of
		// tripping the overwrite guard above. Best-effort after the data is
		// safely in place — a missing sidecar only costs a retry the idempotent
		// fast-path, never correctness.
		if uid != "" {
			_ = os.WriteFile(finalPath+backupUIDSuffix, []byte(uid), 0o644)
		}
	default: // s3
		// Stream straight to S3 — no local staging. The backup Pod has no sized
		// volume, so staging a multi-GB snapshot to the container's ephemeral
		// storage risks eviction mid-upload (the restore path stages to the data
		// volume with a free-space pre-flight precisely to avoid this class). The
		// manager uploader holds only the in-flight parts in memory; we tee the
		// stream through sha256 + a byte counter to get size/checksum as it goes.
		// The S3 object only appears on a complete PutObject, so no atomic dance.
		size, sum, err = uploadS3Stream(ctx, dest, dest.objectKey(name), rc, os.Getenv(envBackupUID))
		if err != nil {
			return fmt.Errorf("upload to s3: %w", err)
		}
	}

	// Marker line consumed by controllers/etcdbackup_controller.go.
	fmt.Printf("snapshot uploaded: uri=%q size=%d sha256=%s\n", dest.uri(name), size, sum)
	return nil
}

// writeSnapshot copies the snapshot stream to path, returning the byte count
// and the hex sha256 over the written bytes.
func writeSnapshot(path string, src io.Reader) (int64, string, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, "", fmt.Errorf("create %s: %w", path, err)
	}
	defer f.Close()
	var h hash.Hash = sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), src)
	if err != nil {
		return 0, "", fmt.Errorf("write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		return 0, "", fmt.Errorf("sync snapshot: %w", err)
	}
	return n, hex.EncodeToString(h.Sum(nil)), nil
}

// writeSnapshotAtomic stages the stream to a temp file in finalPath's
// directory and renames it into place only after a fully-successful write, so
// a partial/interrupted backup never leaves a truncated file at finalPath. On
// any write error the temp file is removed and finalPath is left untouched.
func writeSnapshotAtomic(finalPath string, src io.Reader) (int64, string, error) {
	dir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, "", fmt.Errorf("create destination dir: %w", err)
	}
	f, err := os.CreateTemp(dir, ".etcd-snapshot-*.db.tmp")
	if err != nil {
		return 0, "", fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := f.Name()
	f.Close()

	size, sum, err := writeSnapshot(tmpPath, src)
	if err != nil {
		os.Remove(tmpPath)
		return 0, "", err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return 0, "", fmt.Errorf("move snapshot into place: %w", err)
	}
	return size, sum, nil
}

// uploadS3Stream uploads the snapshot stream directly to S3 (no local staging),
// returning the streamed byte count and hex sha256. The overwrite/ownership
// guard runs first; ownership is stamped via object metadata.
func uploadS3Stream(ctx context.Context, dest destination, key string, body io.Reader, uid string) (int64, string, error) {
	client, err := dest.s3Client(ctx)
	if err != nil {
		return 0, "", err
	}
	if err := ensureObjectAbsent(ctx, client, dest.s3Bucket, key, uid); err != nil {
		return 0, "", err
	}
	var metadata map[string]string
	if uid != "" {
		metadata = map[string]string{backupUIDMetaKey: uid} // stamp ownership so a retry recognizes its own object
	}
	return uploadStreamHashed(ctx, manager.NewUploader(client), &s3.PutObjectInput{
		Bucket:   aws.String(dest.s3Bucket),
		Key:      aws.String(key),
		Metadata: metadata,
	}, body)
}

// s3Uploader abstracts manager.Uploader.Upload so uploadStreamHashed is testable
// without a live S3 endpoint.
type s3Uploader interface {
	Upload(ctx context.Context, in *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error)
}

// uploadStreamHashed tees body through a sha256 hash and a byte counter, sets it
// as the upload Body, and returns the streamed byte count + hex digest. The
// uploader consumes Body, so size/checksum reflect exactly what was stored.
func uploadStreamHashed(ctx context.Context, up s3Uploader, in *s3.PutObjectInput, body io.Reader) (int64, string, error) {
	h := sha256.New()
	cr := &countingReader{r: io.TeeReader(body, h)}
	in.Body = cr
	if _, err := up.Upload(ctx, in); err != nil {
		return 0, "", err
	}
	return cr.n, hex.EncodeToString(h.Sum(nil)), nil
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
