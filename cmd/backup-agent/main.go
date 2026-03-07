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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	endpoints := os.Getenv("ETCD_ENDPOINTS")
	if endpoints == "" {
		return fmt.Errorf("ETCD_ENDPOINTS is required")
	}

	tlsConfig, err := buildTLSConfig()
	if err != nil {
		return fmt.Errorf("failed to build TLS config: %w", err)
	}

	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(endpoints, ","),
		DialTimeout: 10 * time.Second,
		TLS:         tlsConfig,
	})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	defer func() {
		_ = etcdClient.Close()
	}()

	fmt.Println("taking etcd snapshot...")
	reader, err := etcdClient.Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("failed to take snapshot: %w", err)
	}
	defer func() {
		_ = reader.Close()
	}()

	dest := os.Getenv("BACKUP_DESTINATION")
	switch dest {
	case "s3":
		return uploadToS3(ctx, reader)
	case "pvc":
		return writeToPVC(reader)
	default:
		return fmt.Errorf("unknown BACKUP_DESTINATION: %q (expected 's3' or 'pvc')", dest)
	}
}

func buildTLSConfig() (*tls.Config, error) {
	if os.Getenv("ETCD_TLS_ENABLED") != "true" {
		return nil, nil
	}

	tlsConfig := &tls.Config{}

	certPath := os.Getenv("ETCD_TLS_CERT_PATH")
	keyPath := os.Getenv("ETCD_TLS_KEY_PATH")
	if certPath != "" && keyPath != "" {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	caPath := os.Getenv("ETCD_TLS_CA_PATH")
	if caPath != "" {
		caCert, err := os.ReadFile(caPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = pool
	}
	// If no CA path is provided, use system root CAs (default behavior).

	return tlsConfig, nil
}

func uploadToS3(ctx context.Context, reader io.Reader) error {
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

	fmt.Printf("uploading snapshot to s3://%s/%s\n", bucket, key)
	_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   reader,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	fmt.Println("snapshot uploaded successfully")
	return nil
}

func writeToPVC(reader io.Reader) error {
	backupPath := os.Getenv("PVC_BACKUP_PATH")
	if backupPath == "" {
		return fmt.Errorf("PVC_BACKUP_PATH is required")
	}

	if err := os.MkdirAll(filepath.Dir(backupPath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directories for %s: %w", backupPath, err)
	}

	fmt.Printf("writing snapshot to %s\n", backupPath)
	f, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", backupPath, err)
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

	fmt.Printf("snapshot written successfully (%d bytes)\n", written)
	return nil
}
