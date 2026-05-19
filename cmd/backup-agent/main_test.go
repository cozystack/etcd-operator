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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

func TestInjectTimestamp(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"simple file", "backups/snap.db"},
		{"no extension", "backups/snapshot"},
		{"nested path", "daily/etcd/backup.db"},
		{"root file", "snap.db"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := injectTimestamp(tt.input)

			// Result should contain the original extension
			ext := filepath.Ext(tt.input)
			if ext != "" && !strings.HasSuffix(result, ext) {
				t.Errorf("expected result to end with %q, got %q", ext, result)
			}

			// Result should contain a timestamp pattern (YYYYMMDD-HHMMSS)
			base := strings.TrimSuffix(tt.input, ext)
			if !strings.HasPrefix(result, base+"-") {
				t.Errorf("expected result to start with %q, got %q", base+"-", result)
			}

			// Result should be longer than input (timestamp added)
			if len(result) <= len(tt.input) {
				t.Errorf("expected result to be longer than input, got %q (len %d) vs %q (len %d)",
					result, len(result), tt.input, len(tt.input))
			}

			// Timestamp portion should be 15 chars: YYYYMMDD-HHMMSS
			timestampPart := strings.TrimPrefix(result, base+"-")
			timestampPart = strings.TrimSuffix(timestampPart, ext)
			if len(timestampPart) != 15 {
				t.Errorf("expected timestamp portion to be 15 chars (YYYYMMDD-HHMMSS), got %q (len %d)",
					timestampPart, len(timestampPart))
			}
		})
	}
}

func TestBuildTLSConfig_Disabled(t *testing.T) {
	t.Setenv("ETCD_TLS_ENABLED", "false")

	cfg, err := buildTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil TLS config when TLS is disabled")
	}
}

func TestBuildTLSConfig_Unset(t *testing.T) {
	t.Setenv("ETCD_TLS_ENABLED", "")

	cfg, err := buildTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Error("expected nil TLS config when ETCD_TLS_ENABLED is not set")
	}
}

func TestBuildTLSConfig_EnabledMinVersion(t *testing.T) {
	t.Setenv("ETCD_TLS_ENABLED", "true")
	t.Setenv("ETCD_TLS_CERT_PATH", "")
	t.Setenv("ETCD_TLS_KEY_PATH", "")
	t.Setenv("ETCD_TLS_CA_PATH", "")

	cfg, err := buildTLSConfig()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil TLS config")
	}
	if cfg.MinVersion != 0x0303 { // tls.VersionTLS12
		t.Errorf("expected MinVersion TLS 1.2 (0x0303), got 0x%04x", cfg.MinVersion)
	}
}

func TestBuildTLSConfig_InvalidCertPath(t *testing.T) {
	t.Setenv("ETCD_TLS_ENABLED", "true")
	t.Setenv("ETCD_TLS_CERT_PATH", "/nonexistent/cert.pem")
	t.Setenv("ETCD_TLS_KEY_PATH", "/nonexistent/key.pem")
	t.Setenv("ETCD_TLS_CA_PATH", "")

	_, err := buildTLSConfig()
	if err == nil {
		t.Error("expected error for invalid cert path")
	}
}

func TestBuildTLSConfig_InvalidCAPath(t *testing.T) {
	t.Setenv("ETCD_TLS_ENABLED", "true")
	t.Setenv("ETCD_TLS_CERT_PATH", "")
	t.Setenv("ETCD_TLS_KEY_PATH", "")
	t.Setenv("ETCD_TLS_CA_PATH", "/nonexistent/ca.pem")

	_, err := buildTLSConfig()
	if err == nil {
		t.Error("expected error for invalid CA path")
	}
}

// TestWriteToPVC_EmitsSnapshotMarkerWithSHA256 pins the agent → controller
// contract: writeToPVC writes the snapshot bytes verbatim AND emits a
// terminal `snapshot written: uri="file://..." size=... sha256=...` marker
// (URI emitted via %q so whitespace round-trips) whose sha256 matches the
// input bytes. EtcdBackupReconciler parses this exact marker with
// snapshotMarkerRegexp in internal/controller/etcdbackup_controller.go;
// a drift here breaks the status.snapshot wire-up silently.
func TestWriteToPVC_EmitsSnapshotMarkerWithSHA256(t *testing.T) {
	payload := []byte("this is a fake etcd snapshot\n")

	dir := t.TempDir()
	backupPath := filepath.Join(dir, "snap.db")
	t.Setenv("PVC_BACKUP_PATH", backupPath)
	t.Setenv("BACKUP_INCLUDE_REVISION", "")
	t.Setenv("BACKUP_TIMESTAMP", "")

	// Capture stdout — writeToPVC emits the terminal marker via fmt.Printf.
	origStdout := os.Stdout
	rPipe, wPipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = wPipe
	defer func() { os.Stdout = origStdout }()

	// writeToPVC takes io.Reader. Use a bytes.Reader over the payload.
	if err := writeToPVC(bytes.NewReader(payload), 0); err != nil {
		_ = wPipe.Close()
		t.Fatalf("writeToPVC: %v", err)
	}
	if err := wPipe.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	captured, err := io.ReadAll(rPipe)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}

	// Verify the file landed with the right bytes.
	got, err := os.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("read backup file: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("backup file mismatch:\nwant %q\ngot  %q", payload, got)
	}

	// Verify the marker line is present and well-formed.
	h := sha256.New()
	_, _ = h.Write(payload)
	wantSHA := hex.EncodeToString(h.Sum(nil))

	markerRE := regexp.MustCompile(
		`(?m)^snapshot written: uri=("(?:[^"\\]|\\.)*") size=(\d+) sha256=([a-f0-9]+)$`,
	)
	m := markerRE.FindSubmatch(captured)
	if m == nil {
		t.Fatalf("terminal marker not found in stdout; captured:\n%s", captured)
	}
	gotURI, err := strconv.Unquote(string(m[1]))
	if err != nil {
		t.Fatalf("uri capture %q does not unquote: %v", m[1], err)
	}
	wantURI := "file://" + backupPath
	if gotURI != wantURI {
		t.Errorf("uri: got %q want %q", gotURI, wantURI)
	}
	if string(m[2]) != fmt.Sprintf("%d", len(payload)) {
		t.Errorf("size: got %s want %d", m[2], len(payload))
	}
	if string(m[3]) != wantSHA {
		t.Errorf("sha256: got %s want %s", m[3], wantSHA)
	}
}

// TestWriteToPVC_PathWithSpace_MarkerSurvivesWhitespace pins blocker
// 3's fix: an in-container backup path containing a literal space
// must round-trip through the marker line. Before quoting the URI,
// the agent emitted `uri=file:///dir with space/snap.db` and the
// controller-side regex captured `uri=file:///dir` (greedy \S+
// stopped at the first space), silently truncating the URI. The
// %q-encoded marker preserves the entire path, and strconv.Unquote
// on the controller recovers it byte-for-byte.
func TestWriteToPVC_PathWithSpace_MarkerSurvivesWhitespace(t *testing.T) {
	payload := []byte("snapshot bytes with whitespace path\n")

	parent := t.TempDir()
	// Embed a literal space in the directory name on the agent's
	// side of the pipe; mirrors a PVC SubPath like "etcd backups".
	dirWithSpace := filepath.Join(parent, "etcd backups")
	if err := os.MkdirAll(dirWithSpace, 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	backupPath := filepath.Join(dirWithSpace, "snap.db")
	t.Setenv("PVC_BACKUP_PATH", backupPath)
	t.Setenv("BACKUP_INCLUDE_REVISION", "")
	t.Setenv("BACKUP_TIMESTAMP", "")

	origStdout := os.Stdout
	rPipe, wPipe, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stdout = wPipe
	defer func() { os.Stdout = origStdout }()

	if err := writeToPVC(bytes.NewReader(payload), 0); err != nil {
		_ = wPipe.Close()
		t.Fatalf("writeToPVC: %v", err)
	}
	if err := wPipe.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	captured, err := io.ReadAll(rPipe)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}

	markerRE := regexp.MustCompile(
		`(?m)^snapshot written: uri=("(?:[^"\\]|\\.)*") size=(\d+) sha256=([a-f0-9]+)$`,
	)
	m := markerRE.FindSubmatch(captured)
	if m == nil {
		t.Fatalf("terminal marker not found in stdout; captured:\n%s", captured)
	}
	gotURI, err := strconv.Unquote(string(m[1]))
	if err != nil {
		t.Fatalf("uri capture %q does not unquote: %v", m[1], err)
	}
	wantURI := "file://" + backupPath
	if gotURI != wantURI {
		t.Errorf("uri (with space) round-trip failed:\n  got  %q\n  want %q", gotURI, wantURI)
	}
}

// TestUploadToS3_EmptyPayloadAborts mirrors TestWriteToPVC_Empty
// PayloadAborts for the S3 destination: an empty etcd Snapshot()
// stream must bail BEFORE any S3 call. We don't need to mock S3 — the
// guard runs immediately after io.Copy finishes (written == 0), so
// constructing the S3 client and PutObject are never reached and no
// network address is contacted. Pinning this in a test makes the
// hashing-and-empty-check ordering load-bearing: moving the empty
// check below the S3 upload would silently upload zero-byte
// "snapshots" to production buckets.
func TestUploadToS3_EmptyPayloadAborts(t *testing.T) {
	t.Setenv("S3_BUCKET", "test-bucket")
	t.Setenv("S3_KEY", "backups/snap.db")
	t.Setenv("AWS_ACCESS_KEY_ID", "test-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	// Point at a guaranteed-unreachable endpoint. If the empty-payload
	// guard ever regressed and the function tried to upload, the error
	// would be a network/DNS failure with a different message.
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("S3_REGION", "us-east-1")
	t.Setenv("BACKUP_INCLUDE_REVISION", "")
	t.Setenv("BACKUP_TIMESTAMP", "")

	err := uploadToS3(context.Background(), bytes.NewReader(nil), 0)
	if err == nil {
		t.Fatal("expected error for empty payload")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Errorf("expected 'empty' in error message, got: %v", err)
	}
}

// TestWriteToPVC_EmptyPayloadAborts pins the "empty snapshot aborts"
// guard — important because the prior contract was the only safety net
// against a misbehaving etcd Snapshot() returning an empty stream. The
// sha256 hasher must NOT see any bytes in that case, and no marker is
// emitted.
func TestWriteToPVC_EmptyPayloadAborts(t *testing.T) {
	dir := t.TempDir()
	backupPath := filepath.Join(dir, "empty.db")
	t.Setenv("PVC_BACKUP_PATH", backupPath)
	t.Setenv("BACKUP_INCLUDE_REVISION", "")
	t.Setenv("BACKUP_TIMESTAMP", "")

	if err := writeToPVC(bytes.NewReader(nil), 0); err == nil {
		t.Fatal("expected error for empty payload")
	}
	if _, err := os.Stat(backupPath); !os.IsNotExist(err) {
		t.Errorf("expected backup file to be cleaned up on empty-payload abort; stat err: %v", err)
	}
}
