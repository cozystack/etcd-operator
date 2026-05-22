/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controllers

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/lllamnyp/etcd-operator/api/v1alpha2"
)

// EtcdClusterClient is the subset of the etcd v3 client used by the operator.
// Defined as an interface so tests can substitute a fake without dialing a real
// etcd cluster. *clientv3.Client satisfies this via its embedded Cluster
// interface.
type EtcdClusterClient interface {
	MemberList(ctx context.Context, opts ...clientv3.OpOption) (*clientv3.MemberListResponse, error)
	MemberAdd(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error)
	MemberAddAsLearner(ctx context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error)
	MemberPromote(ctx context.Context, id uint64) (*clientv3.MemberPromoteResponse, error)
	MemberRemove(ctx context.Context, id uint64) (*clientv3.MemberRemoveResponse, error)
	Close() error
}

// EtcdClientFactory builds an EtcdClusterClient for a set of endpoints.
// Reconcilers hold a factory rather than a concrete client so tests can inject
// a fake. Production uses DefaultEtcdClientFactory. tlsConfig is nil for
// plaintext clusters and non-nil for TLS-enabled clusters; the operator
// builds it from the cluster's spec.tls.client material.
type EtcdClientFactory func(ctx context.Context, endpoints []string, tlsConfig *tls.Config) (EtcdClusterClient, error)

// DefaultEtcdClientFactory returns a real clientv3.Client.
func DefaultEtcdClientFactory(_ context.Context, endpoints []string, tlsConfig *tls.Config) (EtcdClusterClient, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	}
	if tlsConfig != nil {
		cfg.TLS = tlsConfig
	}
	return clientv3.New(cfg)
}

// buildOperatorTLSConfig assembles the *tls.Config the operator's etcd
// client should dial with, based on the cluster's spec.tls.client. Returns
// (nil, nil) when the cluster has no client TLS configured. Failure modes
// (missing ca.crt, malformed PEM, missing operator-client secret on mTLS
// clusters) surface as errors so the caller can keep the connection
// closed rather than dialling with an incomplete config.
//
// The CA bundle pulled from the server secret's ca.crt is used both for
// RootCAs (operator verifies the server) and, in mTLS mode, mirrors what
// the etcd server has mounted as --trusted-ca-file. That mirroring is
// the user's responsibility — see the EtcdClusterTLS docstring.
func buildOperatorTLSConfig(ctx context.Context, c client.Reader, cluster *lll.EtcdCluster) (*tls.Config, error) {
	if cluster == nil || cluster.Spec.TLS == nil || cluster.Spec.TLS.Client == nil {
		return nil, nil
	}
	ns := cluster.Namespace
	serverName := serverSecretName(cluster)
	if serverName == "" {
		return nil, nil
	}
	serverSec := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: serverName}, serverSec); err != nil {
		return nil, fmt.Errorf("read server TLS secret %s/%s: %w", ns, serverName, err)
	}
	caPEM := serverSec.Data["ca.crt"]
	if len(caPEM) == 0 {
		return nil, fmt.Errorf("server TLS secret %s/%s missing ca.crt", ns, serverName)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("server TLS secret %s/%s: ca.crt is not valid PEM", ns, serverName)
	}
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if opName := operatorClientSecretName(cluster); opName != "" {
		opSec := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: opName}, opSec); err != nil {
			return nil, fmt.Errorf("read operator client TLS secret %s/%s: %w", ns, opName, err)
		}
		cert, err := tls.X509KeyPair(opSec.Data["tls.crt"], opSec.Data["tls.key"])
		if err != nil {
			return nil, fmt.Errorf("operator client TLS secret %s/%s: %w", ns, opName, err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}
