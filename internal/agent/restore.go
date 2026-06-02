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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	etcdversion "go.etcd.io/etcd/api/v3/version"
	"go.etcd.io/etcd/etcdutl/v3/snapshot"
	"go.uber.org/zap"
)

// RunRestore populates the etcd data dir from a snapshot before etcd starts.
// It runs as an initContainer on the bootstrap seed Pod. It is idempotent: if
// the data dir is already initialized (a `member/` directory exists), it is a
// no-op, so Pod restarts after the first boot leave the live data untouched.
//
// For a restore SOURCE the destination locators are EXACT (not prefixes):
// S3_KEY is the full object key, PVC_SUBPATH the full file path within the
// mounted source volume.
func RunRestore(ctx context.Context) error {
	dataDir := os.Getenv(envDataDir)
	if dataDir == "" {
		dataDir = "/var/lib/etcd"
	}
	memberDir := filepath.Join(dataDir, "member")
	if _, err := os.Stat(memberDir); err == nil {
		fmt.Printf("restore: %s already initialized, skipping\n", memberDir)
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat %s: %w", memberDir, err)
	}

	src, err := loadDestination()
	if err != nil {
		return err
	}

	// For a restore SOURCE the S3 key is the EXACT object (not a prefix). An
	// empty key would issue a GetObject for "" and fail with an opaque S3
	// error inside the seed init container — bricking bootstrap. Catch it
	// early with a clear message. (CEL also rejects this at the apiserver, but
	// the agent must not depend on that being enforced.)
	if src.kind == "s3" && src.s3Key == "" {
		return fmt.Errorf("restore source requires an exact S3 object key (%s); got empty", envS3Key)
	}
	// Symmetric to the S3 guard: a PVC restore source addresses one exact file
	// within the volume. An empty subPath would resolve to the mount directory,
	// which os.Stat happily accepts — etcdutl would then fail opaquely trying to
	// read a directory as a snapshot, bricking bootstrap. Require it explicitly.
	if src.kind == "pvc" && src.pvcSubPath == "" {
		return fmt.Errorf("restore source requires the exact snapshot file path within the volume (%s); got empty", envPVCSubPath)
	}

	// Version-compat pre-flight: snapshot.Restore (this agent's etcdutl) writes a
	// data dir with that etcdutl minor's storage semantics. A data dir rebuilt by
	// a different minor than the etcd that will boot on it is unvalidated and can
	// fail opaquely at the seed — the exact silent brick restore must avoid. Fail
	// early with a clear, actionable message instead.
	if err := checkRestoreVersionCompat(os.Getenv(envEtcdVersion), etcdutlMajorMinor()); err != nil {
		return err
	}

	// A prior attempt may have crashed (OOM, node reboot) after staging a
	// snapshot download but before its deferred cleanup ran. We are past the
	// member/ no-op gate, so the data dir is uninitialized and any staged
	// artifacts are stale debris — remove them before the free-space pre-flight,
	// which would otherwise count leftover downloads against the headroom it is
	// trying to protect (and they would accumulate across retries).
	if err := cleanStaleRestoreArtifacts(dataDir); err != nil {
		return err
	}

	// Obtain the snapshot file. The free-space pre-flight runs BEFORE the
	// expensive step in each branch (per the operations runbook's "fails early"
	// guarantee): etcdutl rebuilds the data dir (~snapshot-sized) and, for S3,
	// the download stages onto the same data volume first — a transient ~2x
	// footprint. If we cannot determine the size or free space we fail rather
	// than proceed blindly.
	var snapPath string
	switch src.kind {
	case "s3":
		// HeadObject gives the snapshot size without transferring it, so a data
		// volume too small to even hold the download fails on the pre-flight
		// with a clear, actionable message rather than as an opaque ENOSPC
		// partway through the download itself (the download stages INTO the
		// data dir, not the container's ephemeral /tmp).
		snapPath, err = fetchS3Snapshot(dataDir,
			func() (int64, error) { return headSnapshotSizeS3(ctx, src) },
			func() (string, error) { return downloadS3(ctx, src, dataDir) })
		if err != nil {
			return err
		}
		defer os.Remove(snapPath)
	case "pvc":
		// The snapshot lives on the read-only source mount, not the data
		// volume, so only the etcdutl rebuild consumes data-dir space — the ~2x
		// headroom check is conservative here, which is safe.
		snapPath = filepath.Join(src.pvcMount, src.pvcSubPath)
		fi, err := os.Stat(snapPath)
		if err != nil {
			return fmt.Errorf("snapshot file %s: %w", snapPath, err)
		}
		if fi.IsDir() {
			return fmt.Errorf("snapshot source %s is a directory, not a snapshot file", snapPath)
		}
		if err := ensureRestoreSpace(dataDir, fi.Size()); err != nil {
			return err
		}
	}

	// etcdutl restore into a staging dir (it refuses to overwrite an
	// existing OutputDataDir), then move member/ into the real data dir so
	// etcd's --data-dir stays /var/lib/etcd.
	staging := filepath.Join(dataDir, ".restore")
	_ = os.RemoveAll(staging) // clean any partial prior attempt

	var peerURLs []string
	if p := os.Getenv(envPeerURLs); p != "" {
		peerURLs = strings.Split(p, ",")
	}

	mgr := snapshot.NewV3(zap.NewExample())
	if err := mgr.Restore(snapshot.RestoreConfig{
		SnapshotPath:        snapPath,
		Name:                os.Getenv(envMemberName),
		OutputDataDir:       staging,
		PeerURLs:            peerURLs,
		InitialCluster:      os.Getenv(envInitialCluster),
		InitialClusterToken: os.Getenv(envInitialToken),
		// A clientv3 Maintenance.Snapshot stream has no appended integrity
		// hash (unlike `etcdutl snapshot save`), so skip the check.
		SkipHashCheck: true,
	}); err != nil {
		return fmt.Errorf("etcdutl restore: %w", err)
	}

	if err := os.Rename(filepath.Join(staging, "member"), memberDir); err != nil {
		return fmt.Errorf("move restored data into place: %w", err)
	}
	_ = os.RemoveAll(staging)

	fmt.Printf("restore: completed into %s\n", memberDir)
	return nil
}

// etcdutlMajorMinor returns the "X.Y" of the etcd release the restore agent is
// built against. The etcdutl, api, and server modules ship in lockstep under
// one git tag, so the api module's compiled-in version.Version is a reliable
// proxy for the etcdutl that snapshot.Restore uses — and unlike build info it
// is a compile-time constant present in every build mode (including tests).
func etcdutlMajorMinor() string {
	return majorMinor(etcdversion.Version)
}

// majorMinor extracts "X.Y" from a "X.Y.Z"(-ish) version, or "" if it lacks two
// leading components.
func majorMinor(v string) string {
	parts := strings.SplitN(v, ".", 3)
	if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
		return ""
	}
	return parts[0] + "." + parts[1]
}

// checkRestoreVersionCompat fails when the cluster's etcd version and the
// restore agent's etcdutl differ in major.minor. Empty/unparseable inputs skip
// the check (best-effort) rather than block a restore.
func checkRestoreVersionCompat(clusterVersion, etcdutlVersion string) error {
	cm, um := majorMinor(clusterVersion), majorMinor(etcdutlVersion)
	if cm == "" || um == "" {
		return nil
	}
	if cm != um {
		return fmt.Errorf("restore is only supported when the cluster's etcd version matches the restore agent's etcdutl (%s.x): spec.version=%s. A data dir rebuilt by a different etcd minor may not boot. Run an etcd %s.x cluster, or use an operator build whose etcdutl matches your etcd version", um, clusterVersion, um)
	}
	return nil
}

// cleanStaleRestoreArtifacts removes leftover staging from a crashed prior
// restore attempt: the S3 download temp files (etcd-restore-*.db, matching
// downloadS3's os.CreateTemp pattern) and the etcdutl staging dir (.restore),
// both staged in the data dir. Only called when the data dir is uninitialized
// (past the member/ no-op gate), so nothing live is at risk.
func cleanStaleRestoreArtifacts(dataDir string) error {
	matches, err := filepath.Glob(filepath.Join(dataDir, "etcd-restore-*.db"))
	if err != nil {
		return fmt.Errorf("scan for stale restore artifacts: %w", err)
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove stale restore artifact %s: %w", m, err)
		}
	}
	if err := os.RemoveAll(filepath.Join(dataDir, ".restore")); err != nil {
		return fmt.Errorf("remove stale restore staging dir: %w", err)
	}
	return nil
}

// fetchS3Snapshot enforces the head-before-download ordering that makes the
// "fails early on a too-small volume" guarantee real: it probes the snapshot
// size (headSize), runs the free-space pre-flight, and only invokes download if
// that passes — so an undersized data volume never even starts the transfer.
// head and download are injected so the ordering is unit-testable without S3.
func fetchS3Snapshot(dataDir string, headSize func() (int64, error), download func() (string, error)) (string, error) {
	size, err := headSize()
	if err != nil {
		return "", fmt.Errorf("head snapshot in s3: %w", err)
	}
	if err := ensureRestoreSpace(dataDir, size); err != nil {
		return "", err
	}
	path, err := download()
	if err != nil {
		return "", fmt.Errorf("download snapshot from s3: %w", err)
	}
	return path, nil
}

// headSnapshotSizeS3 returns the snapshot object's size via HeadObject, without
// downloading it — so the restore free-space pre-flight can run before the
// download consumes any of the data volume.
func headSnapshotSizeS3(ctx context.Context, src destination) (int64, error) {
	client, err := src.s3Client(ctx)
	if err != nil {
		return 0, err
	}
	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(src.s3Bucket),
		Key:    aws.String(src.s3Key), // exact object key for restore
	})
	if err != nil {
		return 0, err
	}
	if out.ContentLength == nil {
		return 0, fmt.Errorf("s3 HeadObject returned no ContentLength for %s", src.s3Key)
	}
	return *out.ContentLength, nil
}

// downloadS3 fetches the snapshot into stageDir (the data volume), returning
// the local path. The caller removes it after the restore.
func downloadS3(ctx context.Context, src destination, stageDir string) (string, error) {
	client, err := src.s3Client(ctx)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(stageDir, "etcd-restore-*.db")
	if err != nil {
		return "", err
	}
	defer f.Close()
	downloader := manager.NewDownloader(client)
	if _, err := downloader.Download(ctx, f, &s3.GetObjectInput{
		Bucket: aws.String(src.s3Bucket),
		Key:    aws.String(src.s3Key), // exact object key for restore
	}); err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

// restoreStagingFactor is the rough multiple of the snapshot size the data
// volume must have free during a restore: the snapshot we already hold plus
// the data dir etcdutl rebuilds from it.
const restoreStagingFactor = 2

// ensureRestoreSpace fails early (with an actionable message) when the data
// volume lacks headroom to stage a restore of a snapSize-byte snapshot, rather
// than letting etcdutl die with an opaque ENOSPC partway through.
func ensureRestoreSpace(dataDir string, snapSize int64) error {
	if snapSize < 0 {
		// A negative size (e.g. a bogus HeadObject ContentLength) would wrap to a
		// huge uint64 below and spuriously pass the check — reject it instead.
		return fmt.Errorf("snapshot reports a negative size (%d bytes); refusing to restore", snapSize)
	}
	avail, err := availableBytes(dataDir)
	if err != nil {
		// Can't verify free space — fail rather than proceed blindly, so the
		// documented pre-flight guarantee actually holds.
		return fmt.Errorf("check free space on %s: %w", dataDir, err)
	}
	need := uint64(snapSize) * restoreStagingFactor
	if avail < need {
		return fmt.Errorf("data dir %s has %d bytes free but restoring a %d-byte snapshot needs ~%d (≈%dx for staging); resize the data volume",
			dataDir, avail, snapSize, need, restoreStagingFactor)
	}
	return nil
}

// availableBytes returns the bytes available to an unprivileged writer on the
// filesystem backing dir.
func availableBytes(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, err
	}
	return st.Bavail * uint64(st.Bsize), nil
}
