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
	"strings"
	"testing"
	"time"
)

func TestGetTimeout_Default(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "")

	d, err := getTimeout()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 10*time.Minute {
		t.Errorf("expected 10m default, got %v", d)
	}
}

func TestGetTimeout_Custom(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "30")

	d, err := getTimeout()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if d != 30*time.Minute {
		t.Errorf("expected 30m, got %v", d)
	}
}

func TestGetTimeout_Invalid(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "abc")

	_, err := getTimeout()
	if err == nil {
		t.Error("expected error for invalid value")
	}
}

func TestGetTimeout_Zero(t *testing.T) {
	t.Setenv("RESTORE_TIMEOUT_MINUTES", "0")

	_, err := getTimeout()
	if err == nil {
		t.Error("expected error for zero value")
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

func TestRun_SkipsWhenSentinelPresent(t *testing.T) {
	tmpDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmpDir, restoreCompleteFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ETCD_DATA_DIR", tmpDir)

	err := run()
	if err != nil {
		t.Errorf("expected nil error when sentinel is present, got %v", err)
	}
}

func TestRun_DoesNotSkipOnBareMemberDir(t *testing.T) {
	// Pre-fix behaviour skipped whenever member/ existed, which left
	// half-populated dataDirs (created by etcdutl before the SHA-256
	// check / WAL write) unrecoverable across pod restarts. Lock the
	// fix: member/ alone must NOT trigger the skip; only the sentinel
	// written after mgr.Restore() returned nil counts.
	tmpDir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(tmpDir, "member", "snap"), 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ETCD_DATA_DIR", tmpDir)
	// RESTORE_SOURCE intentionally unset — run() must fall through the
	// skip-check and error with "unknown RESTORE_SOURCE", which proves
	// the partial-state directory did not short-circuit the agent.
	err := run()
	if err == nil {
		t.Fatal("expected run() to fall through skip-check when only member/ exists")
	}
	if !strings.Contains(err.Error(), "unknown RESTORE_SOURCE") {
		t.Fatalf("expected fallthrough to RESTORE_SOURCE dispatch, got %v", err)
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

// writeFakeSnapshot drops a non-empty placeholder at the path
// restoreDataDir() looks for so the env-var / wipe branches run
// before snapshot.Restore() rejects the bogus content. We only assert
// on the pre-Restore branches; the Restore call itself is expected
// to error and the test treats anything that reaches that point as
// "passed env validation".
func writeFakeSnapshot(t *testing.T) {
	t.Helper()
	dir := withTempRestoreDir(t)
	if err := os.WriteFile(filepath.Join(dir, snapshotFilename), []byte("placeholder"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func setValidRestoreEnv(t *testing.T) {
	t.Helper()
	t.Setenv("POD_NAME", "test-0")
	t.Setenv("POD_NAMESPACE", "ns")
	t.Setenv("ETCD_INITIAL_CLUSTER", "test-0=https://test-0.test-headless.ns.svc:2380")
	t.Setenv("ETCD_INITIAL_CLUSTER_TOKEN", "tkn")
	t.Setenv("HEADLESS_SVC", "test-headless")
}

func TestRestoreDataDir_MissingSnapshotFile(t *testing.T) {
	withTempRestoreDir(t)
	setValidRestoreEnv(t)

	err := restoreDataDir(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "snapshot file missing") {
		t.Fatalf("expected snapshot-missing error, got %v", err)
	}
}

func TestRestoreDataDir_MissingEnv(t *testing.T) {
	cases := []struct {
		name    string
		clear   string
		wantSub string
	}{
		{"POD_NAME", "POD_NAME", "POD_NAME"},
		{"POD_NAMESPACE", "POD_NAMESPACE", "POD_NAMESPACE"},
		{"ETCD_INITIAL_CLUSTER", "ETCD_INITIAL_CLUSTER", "ETCD_INITIAL_CLUSTER"},
		{"ETCD_INITIAL_CLUSTER_TOKEN", "ETCD_INITIAL_CLUSTER_TOKEN", "ETCD_INITIAL_CLUSTER_TOKEN"},
		{"HEADLESS_SVC", "HEADLESS_SVC", "HEADLESS_SVC"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeFakeSnapshot(t)
			setValidRestoreEnv(t)
			t.Setenv(tc.clear, "")

			err := restoreDataDir(t.TempDir())
			if err == nil {
				t.Fatalf("expected error when %s unset", tc.clear)
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Fatalf("error %q does not mention %q", err, tc.wantSub)
			}
		})
	}
}

func TestRestoreDataDir_TrimsWhitespaceEnv(t *testing.T) {
	writeFakeSnapshot(t)
	setValidRestoreEnv(t)
	// Both come from the same ConfigMap; both must trim. If only one
	// trims, ConfigMap values with trailing newlines silently break
	// initial-cluster parsing or token comparison.
	t.Setenv("ETCD_INITIAL_CLUSTER", "   \n")
	t.Setenv("ETCD_INITIAL_CLUSTER_TOKEN", "tkn")
	err := restoreDataDir(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "ETCD_INITIAL_CLUSTER ") {
		t.Fatalf("whitespace-only ETCD_INITIAL_CLUSTER must error, got %v", err)
	}

	writeFakeSnapshot(t)
	setValidRestoreEnv(t)
	t.Setenv("ETCD_INITIAL_CLUSTER_TOKEN", " \t\n ")
	err = restoreDataDir(t.TempDir())
	if err == nil || !strings.Contains(err.Error(), "ETCD_INITIAL_CLUSTER_TOKEN") {
		t.Fatalf("whitespace-only ETCD_INITIAL_CLUSTER_TOKEN must error, got %v", err)
	}
}

func TestRestoreDataDir_WipesStrayDataDir(t *testing.T) {
	writeFakeSnapshot(t)
	setValidRestoreEnv(t)

	// dataDir exists with stray files but no member/ subdir. The wipe
	// branch must reclaim it before invoking Restore(), otherwise
	// etcdutl refuses to overwrite.
	dataDir := filepath.Join(t.TempDir(), "default.etcd")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	stray := filepath.Join(dataDir, "stray.txt")
	if err := os.WriteFile(stray, []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	// We expect an error from Restore() (the snapshot file is not a
	// real etcd snapshot) — what matters is that the wipe ran first.
	_ = restoreDataDir(dataDir)

	if _, err := os.Stat(stray); !os.IsNotExist(err) {
		t.Fatalf("stray file not removed; wipe did not run (stat err=%v)", err)
	}
}

func TestRestoreDataDir_WipesPartialMemberDir(t *testing.T) {
	writeFakeSnapshot(t)
	setValidRestoreEnv(t)

	// Simulate the failure mode the sentinel guards against: etcdutl
	// created member/snap/ but the agent died before writing the
	// sentinel. The wipe branch must clear the dir for a retry.
	dataDir := filepath.Join(t.TempDir(), "default.etcd")
	memberSnap := filepath.Join(dataDir, "member", "snap")
	if err := os.MkdirAll(memberSnap, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(memberSnap, "db"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	_ = restoreDataDir(dataDir)

	if _, err := os.Stat(memberSnap); !os.IsNotExist(err) {
		t.Fatalf("member/snap not removed; wipe did not run (stat err=%v)", err)
	}
}

func TestRestoreDataDir_SentinelGuardsSuccess(t *testing.T) {
	// Locks the post-success contract: once mgr.Restore() returns nil
	// the sentinel must be in place so run() short-circuits on the
	// next pod start instead of re-restoring (and on PVC-backed
	// clusters, instead of wiping a healthy data dir). The agent under
	// test can't reach the actual Restore call with a placeholder
	// snapshot, so we drive the contract from run()'s side: writing
	// the sentinel by hand must produce the skip behaviour.
	tmpDir := t.TempDir()
	canary := filepath.Join(tmpDir, "member", "snap", "db")
	if err := os.MkdirAll(filepath.Dir(canary), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(canary, []byte("healthy-member-data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(tmpDir, restoreCompleteFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Setenv("ETCD_DATA_DIR", tmpDir)

	if err := run(); err != nil {
		t.Fatalf("run() must skip without error when sentinel is present, got %v", err)
	}
	if _, err := os.Stat(canary); err != nil {
		t.Fatalf("member/ canary missing; run() did not honour the sentinel skip-check: %v", err)
	}
}
