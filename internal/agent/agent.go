/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package agent implements the snapshot and restore agents that run inside the
// operator image (invoked as `manager snapshot-agent` / `manager restore-agent`)
// in a Job (snapshot) or an initContainer (restore). The controller configures
// them entirely through environment variables so the agent image needs no
// Kubernetes API access.
package agent

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// SnapshotTimeout bounds a single snapshot agent run. clientv3.New does not block
// on connect, so a parked/unreachable cluster surfaces as the Snapshot RPC
// hanging on a context with no deadline — this gives that context one, so the
// agent exits with a clear "context deadline exceeded" rather than blocking
// indefinitely. Kept under the snapshot Job's ActiveDeadlineSeconds (1800s) so
// the agent self-terminates with a logged error before the kubelet SIGKILLs the
// Pod; the Job deadline is the authoritative backstop for any other hang.
const SnapshotTimeout = 25 * time.Minute

// RestoreTimeout bounds a single restore agent run. The restore agent runs as a
// Pod init container — there is no Job ActiveDeadlineSeconds backstop — so this
// is the SOLE guard against a slow/black-holed S3 endpoint hanging the
// HeadObject/Download on a deadline-less context and wedging cluster bootstrap
// in Init forever. The bounded context is threaded into the S3 calls so a hang
// fails with a clear "context deadline exceeded" the operator can surface.
const RestoreTimeout = 30 * time.Minute

// Environment variable contract shared by both agents. The snapshot-job factory
// (controllers/snapshot_job.go) and the restore initContainer (buildPod) set
// these.
const (
	// etcd connection.
	envEndpoints   = "ETCD_ENDPOINTS"   // comma-separated client URLs
	envTLSCAPath   = "ETCD_TLS_CA_PATH" // server CA for verification
	envTLSCertPath = "ETCD_TLS_CERT_PATH"
	envTLSKeyPath  = "ETCD_TLS_KEY_PATH"
	envUsername    = "ETCD_USERNAME"
	envPassword    = "ETCD_PASSWORD"

	// destination (snapshot) / source (restore).
	envDestKind     = "SNAPSHOT_DEST_KIND" // "s3" | "pvc"
	envSnapshotName = "SNAPSHOT_NAME"
	envSnapshotUID  = "SNAPSHOT_UID" // EtcdSnapshot UID; stamped on the S3 object so a retry recognizes its own upload
	envS3Endpoint   = "S3_ENDPOINT"
	envS3Bucket     = "S3_BUCKET"
	envS3Key        = "S3_KEY"
	envS3Region     = "S3_REGION"
	envS3PathStyle  = "S3_FORCE_PATH_STYLE"
	envPVCMountPath = "PVC_MOUNT_PATH" // where the destination/source PVC is mounted
	envPVCSubPath   = "PVC_SUBPATH"

	// restore-only.
	envDataDir        = "ETCD_DATA_DIR"
	envMemberName     = "ETCD_MEMBER_NAME"
	envInitialCluster = "ETCD_INITIAL_CLUSTER"
	envInitialToken   = "ETCD_INITIAL_CLUSTER_TOKEN"
	envPeerURLs       = "ETCD_PEER_URLS" // comma-separated
	envEtcdVersion    = "ETCD_VERSION"   // cluster's spec.version, for the restore version-compat pre-flight
)

// destination captures the resolved snapshot destination / restore source.
type destination struct {
	kind        string // "s3" | "pvc"
	s3Endpoint  string
	s3Bucket    string
	s3Key       string // object-key prefix
	s3Region    string
	s3PathStyle bool
	pvcMount    string
	pvcSubPath  string
}

func loadDestination() (destination, error) {
	d := destination{
		kind:        os.Getenv(envDestKind),
		s3Endpoint:  os.Getenv(envS3Endpoint),
		s3Bucket:    os.Getenv(envS3Bucket),
		s3Key:       os.Getenv(envS3Key),
		s3Region:    os.Getenv(envS3Region),
		s3PathStyle: os.Getenv(envS3PathStyle) == "true",
		pvcMount:    os.Getenv(envPVCMountPath),
		pvcSubPath:  os.Getenv(envPVCSubPath),
	}
	switch d.kind {
	case "s3":
		if d.s3Bucket == "" || d.s3Endpoint == "" {
			return d, fmt.Errorf("s3 destination requires %s and %s", envS3Endpoint, envS3Bucket)
		}
	case "pvc":
		if d.pvcMount == "" {
			return d, fmt.Errorf("pvc destination requires %s", envPVCMountPath)
		}
	default:
		return d, fmt.Errorf("unknown %s=%q (want s3|pvc)", envDestKind, d.kind)
	}
	return d, nil
}

// objectKey is the S3 object key: "<key-prefix>/<name>.db" (prefix optional).
func (d destination) objectKey(name string) string {
	file := name + ".db"
	switch {
	case d.s3Key == "":
		return file
	case strings.HasSuffix(d.s3Key, "/"):
		return d.s3Key + file
	default:
		return d.s3Key + "/" + file
	}
}

// uri renders the snapshot location for status/marker.
func (d destination) uri(name string) string {
	if d.kind == "s3" {
		return fmt.Sprintf("s3://%s/%s", d.s3Bucket, d.objectKey(name))
	}
	return "file://" + d.localPath(name)
}

// localPath is the on-disk path for a PVC destination/source.
func (d destination) localPath(name string) string {
	base := d.pvcMount
	if d.pvcSubPath != "" {
		base = base + "/" + d.pvcSubPath
	}
	return base + "/" + name + ".db"
}

// s3Client builds an S3 client honoring a custom endpoint, region and
// path-style addressing. Credentials come from AWS_ACCESS_KEY_ID /
// AWS_SECRET_ACCESS_KEY in the environment (loaded by the default config
// chain).
func (d destination) s3Client(ctx context.Context) (*s3.Client, error) {
	region := d.s3Region
	if region == "" {
		region = "us-east-1" // many S3-compatible endpoints ignore it but the SDK needs one
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		if d.s3Endpoint != "" {
			o.BaseEndpoint = aws.String(d.s3Endpoint)
		}
		o.UsePathStyle = d.s3PathStyle
	}), nil
}

// etcdClient dials the cluster with the TLS/auth material the controller
// mounted/passed. Mirrors controllers/etcd_client.go's dial config.
func etcdClient() (*clientv3.Client, error) {
	eps := strings.Split(os.Getenv(envEndpoints), ",")
	if len(eps) == 0 || eps[0] == "" {
		return nil, fmt.Errorf("%s is empty", envEndpoints)
	}
	cfg := clientv3.Config{
		Endpoints:   eps,
		DialTimeout: 10 * time.Second,
		Username:    os.Getenv(envUsername),
		Password:    os.Getenv(envPassword),
	}
	tlsCfg, err := loadTLS()
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		cfg.TLS = tlsCfg
	}
	return clientv3.New(cfg)
}

func loadTLS() (*tls.Config, error) {
	caPath := os.Getenv(envTLSCAPath)
	if caPath == "" {
		return nil, nil // plaintext cluster
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA %s is not valid PEM", caPath)
	}
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	certPath, keyPath := os.Getenv(envTLSCertPath), os.Getenv(envTLSKeyPath)
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
