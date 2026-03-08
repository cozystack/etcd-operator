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
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestGetTimeout_Default(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "")

	d := getTimeout()
	if d != 10*time.Minute {
		t.Errorf("expected 10m default, got %v", d)
	}
}

func TestGetTimeout_Custom(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "30")

	d := getTimeout()
	if d != 30*time.Minute {
		t.Errorf("expected 30m, got %v", d)
	}
}

func TestGetTimeout_Invalid(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "abc")

	d := getTimeout()
	if d != 10*time.Minute {
		t.Errorf("expected 10m fallback for invalid value, got %v", d)
	}
}

func TestGetTimeout_Zero(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "0")

	d := getTimeout()
	if d != 10*time.Minute {
		t.Errorf("expected 10m fallback for zero, got %v", d)
	}
}

func withTempRestoreDir(t *testing.T) string {
	t.Helper()
	tmpDir := t.TempDir()
	origDir := restoreSnapshotDir
	restoreSnapshotDir = tmpDir
	t.Cleanup(func() { restoreSnapshotDir = origDir })
	return tmpDir
}

func TestWriteSnapshot_NonEmpty(t *testing.T) {
	tmpDir := withTempRestoreDir(t)

	data := []byte("fake etcd snapshot data for testing")
	err := writeSnapshot(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("writeSnapshot failed: %v", err)
	}

	snapshotPath := filepath.Join(tmpDir, snapshotFilename)
	info, err := os.Stat(snapshotPath)
	if err != nil {
		t.Fatalf("snapshot file not found: %v", err)
	}
	if info.Size() != int64(len(data)) {
		t.Errorf("expected %d bytes, got %d", len(data), info.Size())
	}
}

func TestWriteSnapshot_Empty(t *testing.T) {
	tmpDir := withTempRestoreDir(t)

	err := writeSnapshot(bytes.NewReader(nil))
	if err == nil {
		t.Fatal("expected error for empty snapshot")
	}

	// Verify the empty file was cleaned up
	snapshotPath := filepath.Join(tmpDir, snapshotFilename)
	if _, statErr := os.Stat(snapshotPath); !os.IsNotExist(statErr) {
		t.Error("expected snapshot file to be removed after empty snapshot error")
	}
}

func TestCopyFromPVC_FileNotFound(t *testing.T) {
	withTempRestoreDir(t)
	t.Setenv("PVC_BACKUP_PATH", "/nonexistent/backup.db")

	err := copyFromPVC()
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestCopyFromPVC_MissingEnv(t *testing.T) {
	t.Setenv("PVC_BACKUP_PATH", "")

	err := copyFromPVC()
	if err == nil {
		t.Error("expected error for missing PVC_BACKUP_PATH")
	}
}

func TestCopyFromPVC_Success(t *testing.T) {
	tmpDir := withTempRestoreDir(t)

	// Create a source file to copy from
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "backup.db")
	data := []byte("fake etcd snapshot from PVC")
	if err := os.WriteFile(srcPath, data, 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PVC_BACKUP_PATH", srcPath)

	err := copyFromPVC()
	if err != nil {
		t.Fatalf("copyFromPVC failed: %v", err)
	}

	// Verify the snapshot was copied
	snapshotPath := filepath.Join(tmpDir, snapshotFilename)
	content, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatalf("failed to read copied snapshot: %v", err)
	}
	if !bytes.Equal(content, data) {
		t.Errorf("snapshot content mismatch: got %q, want %q", content, data)
	}
}

func TestCopyFromPVC_EmptyFile(t *testing.T) {
	tmpDir := withTempRestoreDir(t)

	// Create an empty source file
	srcDir := t.TempDir()
	srcPath := filepath.Join(srcDir, "empty.db")
	if err := os.WriteFile(srcPath, []byte{}, 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PVC_BACKUP_PATH", srcPath)

	err := copyFromPVC()
	if err == nil {
		t.Fatal("expected error for empty PVC backup file")
	}

	// Verify the empty file was cleaned up
	snapshotPath := filepath.Join(tmpDir, snapshotFilename)
	if _, statErr := os.Stat(snapshotPath); !os.IsNotExist(statErr) {
		t.Error("expected snapshot file to be removed after empty snapshot error")
	}
}

func TestRun_SkipsWhenDataDirExists(t *testing.T) {
	tmpDir := t.TempDir()
	memberDir := filepath.Join(tmpDir, "member")
	if err := os.MkdirAll(memberDir, 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ETCD_DATA_DIR", tmpDir)

	err := run()
	if err != nil {
		t.Errorf("expected nil error when data dir exists, got %v", err)
	}
}

func TestRun_InvalidRestoreSource(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("ETCD_DATA_DIR", tmpDir)
	t.Setenv("RESTORE_SOURCE", "invalid")

	err := run()
	if err == nil {
		t.Error("expected error for invalid RESTORE_SOURCE")
	}
}

func TestDownloadFromS3_MissingBucket(t *testing.T) {
	t.Setenv("S3_BUCKET", "")
	t.Setenv("S3_KEY", "backup.db")

	err := downloadFromS3()
	if err == nil {
		t.Error("expected error for missing S3_BUCKET")
	}
}

func TestDownloadFromS3_MissingCredentials(t *testing.T) {
	t.Setenv("S3_BUCKET", "test-bucket")
	t.Setenv("S3_KEY", "backup.db")
	t.Setenv("AWS_ACCESS_KEY_ID", "")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "")

	err := downloadFromS3()
	if err == nil {
		t.Error("expected error for missing credentials")
	}
}
