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

package controller

import (
	"errors"
	"io"
	"strings"
	"testing"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
)

// Full-length sha256 fixtures used throughout the scan tests. The
// parser's snapshotMarkerRegexp anchors the sha256 capture to
// exactly 64 hex chars so a truncated emit (the kubelet tore the
// log near EOL, bufio's pathological isPrefix split a giant line)
// cannot pass validation with a 1-byte hash. Using full-length
// fixtures here keeps each "rejection" case failing for the reason
// it was named for, not because the hash happens to be too short.
const (
	sha256Hex64A = "abcd1234ef000000000000000000000000000000000000000000000000000000"
	sha256Hex64B = "deadbeefcafef00d000000000000000000000000000000000000000000000000"
	sha256Hex64C = "0000000000000000000000000000000000000000000000000000000000000000"
	sha256Hex64D = "cafe000000000000000000000000000000000000000000000000000000000000"
	sha256Hex64E = "ab00000000000000000000000000000000000000000000000000000000000000"
	sha256Hex64F = "1100000000000000000000000000000000000000000000000000000000000000"
	sha256Hex64G = "2200000000000000000000000000000000000000000000000000000000000000"
)

// TestScanBackupAgentLog pins the regex contract against the exact
// fmt.Printf strings emitted by cmd/backup-agent/main.go. Drift in
// either format breaks restore (status.snapshot stays empty and
// callers have no way to recover the per-run URI / size / sha256).
func TestScanBackupAgentLog(t *testing.T) {
	cases := []struct {
		name string
		log  string
		want *etcdaenixiov1alpha1.BackupSnapshot
	}{
		{
			"s3 terminal marker",
			"etcd revision: 42\n" +
				"taking etcd snapshot...\n" +
				"uploading snapshot to s3://my-bucket/etcd/backups/snap-rev42.db (20512 bytes)\n" +
				`snapshot uploaded: uri="s3://my-bucket/etcd/backups/snap-rev42.db" size=20512 sha256=` + sha256Hex64A + "\n",
			&etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "s3://my-bucket/etcd/backups/snap-rev42.db",
				SizeBytes: 20512,
				Checksum:  "sha256:" + sha256Hex64A,
			},
		},
		{
			"pvc terminal marker",
			"taking etcd snapshot...\n" +
				"writing snapshot to /backup/data/etcd/backups/snap-rev42.db\n" +
				`snapshot written: uri="file:///backup/data/etcd/backups/snap-rev42.db" size=2048 sha256=` + sha256Hex64B + "\n",
			&etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "file:///backup/data/etcd/backups/snap-rev42.db",
				SizeBytes: 2048,
				Checksum:  "sha256:" + sha256Hex64B,
			},
		},
		{
			"latest marker wins (pod restart re-emits log)",
			`snapshot uploaded: uri="s3://b/first.db" size=10 sha256=` + sha256Hex64F + "\n" +
				`snapshot uploaded: uri="s3://b/second.db" size=20 sha256=` + sha256Hex64G + "\n",
			&etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "s3://b/second.db",
				SizeBytes: 20,
				Checksum:  "sha256:" + sha256Hex64G,
			},
		},
		{
			"uri with slashes is captured whole",
			`snapshot uploaded: uri="s3://b1/a/b/c/d.db" size=1 sha256=` + sha256Hex64C + "\n",
			&etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "s3://b1/a/b/c/d.db",
				SizeBytes: 1,
				Checksum:  "sha256:" + sha256Hex64C,
			},
		},
		{
			// Pins blocker 3's fix: an S3 key with a literal space
			// (legal per the S3 object-key spec) used to truncate at
			// the first whitespace under the old \S+ capture. The
			// %q-encoded marker preserves the whole key; the parser
			// strconv.Unquote()s capture group 1 to recover it.
			"s3 key with embedded space",
			`snapshot uploaded: uri="s3://my-bucket/etcd backups/snap-rev1.db" size=99 sha256=` + sha256Hex64D + "\n",
			&etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "s3://my-bucket/etcd backups/snap-rev1.db",
				SizeBytes: 99,
				Checksum:  "sha256:" + sha256Hex64D,
			},
		},
		{
			// Pin the escape-sequence path so a future agent emitting
			// a quote-bearing key (rare but legal) survives.
			"uri with escaped quote",
			`snapshot uploaded: uri="s3://b/q\"key" size=1 sha256=` + sha256Hex64E + "\n",
			&etcdaenixiov1alpha1.BackupSnapshot{
				URI:       `s3://b/q"key`,
				SizeBytes: 1,
				Checksum:  "sha256:" + sha256Hex64E,
			},
		},
		{
			// The marker is intentionally unanchored on the right so
			// a future agent can append a token (e.g. revision=N)
			// without breaking older controllers. Pin that property:
			// an extra trailing token must NOT cause "no marker found".
			"forward-compat: trailing token is ignored",
			`snapshot uploaded: uri="s3://b/k.db" size=10 sha256=` + sha256Hex64A + ` revision=12345 extra=ignored` + "\n",
			&etcdaenixiov1alpha1.BackupSnapshot{
				URI:       "s3://b/k.db",
				SizeBytes: 10,
				Checksum:  "sha256:" + sha256Hex64A,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := scanBackupAgentLog(strings.NewReader(tc.log))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatalf("got nil snapshot, want %+v", tc.want)
			}
			if got.URI != tc.want.URI || got.SizeBytes != tc.want.SizeBytes || got.Checksum != tc.want.Checksum {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestScanBackupAgentLog_OverlyLongLineDoesNotMaskMarker pins the
// ErrTooLong fallback: a multi-MiB log line (e.g. a stray stack trace
// dumped on stderr) is silently dropped and a subsequent valid marker
// line is still parsed. The old bufio.Scanner-based parser aborted
// the entire scan in this case, which would have left
// status.snapshot empty for an otherwise-successful backup.
func TestScanBackupAgentLog_OverlyLongLineDoesNotMaskMarker(t *testing.T) {
	// 2 MiB junk line, well over the 4 KiB ReadLine buffer.
	junk := strings.Repeat("x", 2*1024*1024)
	log := io.MultiReader(
		strings.NewReader("warning: etcd printed a giant blob:\n"),
		strings.NewReader(junk+"\n"),
		strings.NewReader(`snapshot written: uri="file:///snap.db" size=42 sha256=`+sha256Hex64B+"\n"),
	)
	got, err := scanBackupAgentLog(log)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("got nil snapshot")
	}
	if got.URI != "file:///snap.db" || got.SizeBytes != 42 || got.Checksum != "sha256:"+sha256Hex64B {
		t.Errorf("got %+v, want URI=file:///snap.db size=42 sha256:%s", got, sha256Hex64B)
	}
}

// errAfterReader wraps an io.Reader and synthesizes a hard error
// once the wrapped reader returns io.EOF, so a test can simulate
// "stream errored after we already read the marker line". Used to
// pin the contract that scanBackupAgentLog preserves a parsed
// marker across a mid-stream read failure.
type errAfterReader struct {
	inner io.Reader
	err   error
}

func (e *errAfterReader) Read(p []byte) (int, error) {
	n, err := e.inner.Read(p)
	if err == io.EOF {
		return n, e.err
	}
	return n, err
}

// TestScanBackupAgentLog_PreservesMarkerOnMidStreamError pins the
// load-bearing contract that a mid-stream read failure AFTER the
// terminal marker line was already parsed does NOT drop the
// snapshot. The kubelet's log proxy can RST the connection
// mid-stream; by the time the workqueue retries the agent pod may
// be GC'd, and the URI/sha256 we already had would be lost
// forever. The marker we parsed is authoritative — the trailing
// stream error is informational.
func TestScanBackupAgentLog_PreservesMarkerOnMidStreamError(t *testing.T) {
	const log = `snapshot uploaded: uri="s3://b/k.db" size=10 sha256=` + sha256Hex64A + "\n"
	r := &errAfterReader{
		inner: strings.NewReader(log),
		err:   errors.New("simulated kubelet log proxy RST"),
	}
	got, err := scanBackupAgentLog(r)
	if err != nil {
		t.Fatalf("scan returned error after marker was parsed: %v", err)
	}
	if got == nil {
		t.Fatal("got nil snapshot; expected marker to survive trailing read error")
	}
	if got.URI != "s3://b/k.db" {
		t.Errorf("URI = %q, want s3://b/k.db", got.URI)
	}
	if got.SizeBytes != 10 {
		t.Errorf("SizeBytes = %d, want 10", got.SizeBytes)
	}
	if got.Checksum != "sha256:"+sha256Hex64A {
		t.Errorf("Checksum = %q, want sha256:%s", got.Checksum, sha256Hex64A)
	}
}

// TestScanBackupAgentLog_PropagatesErrorWhenNoMarkerParsed: a
// mid-stream read failure with NO already-parsed marker must
// still propagate so the caller's default branch retries. Pinning
// the negative case ensures the preservation logic above didn't
// over-swallow real infrastructure failures.
func TestScanBackupAgentLog_PropagatesErrorWhenNoMarkerParsed(t *testing.T) {
	streamErr := errors.New("simulated kubelet log proxy RST")
	r := &errAfterReader{
		inner: strings.NewReader("taking etcd snapshot...\n"),
		err:   streamErr,
	}
	_, err := scanBackupAgentLog(r)
	if err == nil {
		t.Fatal("expected error when stream fails before any marker is parsed")
	}
	if !errors.Is(err, streamErr) {
		t.Errorf("got %v, want wrapped streamErr", err)
	}
}

// TestScanBackupAgentLog_SizeOverflowFallback pins the only real
// codepath that produces a zero SizeBytes while BackupSnapshot is
// non-nil: a regex-conformant size capture that overflows int64. The
// scanner intentionally falls through with SizeBytes=0 rather than
// dropping the marker entirely, so a reviewer reading status still
// sees the snapshot URI and checksum. This is unreachable in
// practice (the agent would have to write more than 9.2 EiB to a
// single snapshot) but pinning the documented behavior here lets a
// future refactor that hardens the fallback land safely.
func TestScanBackupAgentLog_SizeOverflowFallback(t *testing.T) {
	// math.MaxInt64 = 9_223_372_036_854_775_807 (19 digits). 20 nines
	// is just past that bound; strconv.ParseInt returns ErrRange.
	const overflow = "99999999999999999999"
	log := `snapshot uploaded: uri="s3://b/k.db" size=` + overflow + ` sha256=` + sha256Hex64A + "\n"

	got, err := scanBackupAgentLog(strings.NewReader(log))
	if err != nil {
		t.Fatalf("scan must NOT fail on size overflow: %v", err)
	}
	if got == nil {
		t.Fatal("got nil snapshot; expected URI + checksum to survive overflow fallback")
	}
	if got.SizeBytes != 0 {
		t.Errorf("SizeBytes = %d, want 0 (overflow fallback)", got.SizeBytes)
	}
	if got.URI != "s3://b/k.db" {
		t.Errorf("URI = %q, want s3://b/k.db", got.URI)
	}
	if got.Checksum != "sha256:"+sha256Hex64A {
		t.Errorf("Checksum = %q, want sha256:%s", got.Checksum, sha256Hex64A)
	}
}

func TestScanBackupAgentLog_NoMarker(t *testing.T) {
	cases := []struct {
		name string
		log  string
	}{
		{"empty log", ""},
		{"only breadcrumb lines", "uploading snapshot to s3://b/k (1 bytes)\nwriting snapshot to /x\n"},
		{"marker missing sha256", `snapshot uploaded: uri="s3://b/k.db" size=1` + "\n"},
		{"marker missing size", `snapshot uploaded: uri="s3://b/k.db" sha256=` + sha256Hex64A + "\n"},
		{"old unquoted uri form rejected", "snapshot uploaded: uri=s3://b/k.db size=1 sha256=" + sha256Hex64A + "\n"},
		// blocker B: an empty quoted URI must NOT slip past the
		// scanner — the CRD pattern (^(s3|file)://.+) would reject it
		// on Status().Update and the controller would spin retrying
		// the apiserver write. Tighten the contract at parse time.
		{"empty uri rejected", `snapshot uploaded: uri="" size=1 sha256=` + sha256Hex64A + "\n"},
		// blocker C: a misordered marker (size before uri) must NOT
		// parse. The doc on snapshotMarkerRegexp claims "renaming or
		// reordering tokens breaks parsing"; pin it.
		{"misordered tokens rejected", `snapshot uploaded: size=10 uri="s3://b/k.db" sha256=` + sha256Hex64A + "\n"},
		// blocker E: defense in depth — a syntactically valid marker
		// whose URI doesn't satisfy the CRD's (s3|file)://.+ pattern
		// must NOT parse, otherwise we'd burn the retry budget on
		// Status().Update validation errors.
		{"uri with wrong scheme rejected", `snapshot uploaded: uri="https://b/k.db" size=10 sha256=` + sha256Hex64A + "\n"},
		// Tightened sha256 length: the regex now requires exactly
		// 64 hex chars (a full sha256). A truncated trailing
		// `sha256=a` used to satisfy the old `[a-f0-9]+` capture
		// and land a meaningless 1-byte "checksum" on
		// status.snapshot. Pin both extremes — minimum (1 char) and
		// just-shy (63 chars) — so a future loosening of the
		// length anchor is caught by these tests.
		{"sha256 too short — 1 hex char", `snapshot uploaded: uri="s3://b/k.db" size=10 sha256=a` + "\n"},
		{"sha256 too short — 63 hex chars", `snapshot uploaded: uri="s3://b/k.db" size=10 sha256=` + strings.Repeat("a", 63) + "\n"},
		// Symmetric upper bound: the regex now anchors a trailing
		// whitespace OR end-of-line after the 64th hex char so a
		// malformed emit of 65+ hex chars (no separator) cannot
		// silently truncate to a "valid" 64-char hash. A forward-
		// compatible trailing token like " revision=N" still
		// works because that whitespace boundary matches.
		{"sha256 too long — 65 hex chars", `snapshot uploaded: uri="s3://b/k.db" size=10 sha256=` + strings.Repeat("a", 65) + "\n"},
		{"sha256 too long — 80 hex chars", `snapshot uploaded: uri="s3://b/k.db" size=10 sha256=` + strings.Repeat("a", 80) + "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			snap, err := scanBackupAgentLog(strings.NewReader(tc.log))
			if err == nil {
				t.Fatalf("expected error, got snapshot %+v", snap)
			}
			// The "no marker found" path must surface the
			// errNoMarker sentinel — that is what tells the
			// reconciler to finalize Phase=Complete with empty
			// status.snapshot (rather than retrying forever or
			// returning to the workqueue's exponential-backoff path
			// as if the failure were transient).
			if !errors.Is(err, errNoMarker) {
				t.Errorf("got %v, want errNoMarker", err)
			}
		})
	}
}
