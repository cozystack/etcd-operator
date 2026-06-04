/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

// Package etcdclient holds the etcd clientv3 construction shared between the
// operator (controllers) and the kubectl-etcd plugin. The two consumers reach
// etcd over different transports — the operator dials the cluster Service,
// the plugin port-forwards to a Pod — but the client config, TLS assembly,
// and basic-auth credential handling are identical and must not drift.
package etcdclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
)

// dialTimeout is the connect deadline for every etcd dial, operator and
// plugin alike.
const dialTimeout = 5 * time.Second

// New builds a clientv3.Client with the project's standard dial settings.
// tlsConfig is nil for plaintext endpoints. An empty username makes clientv3
// skip the Authenticate RPC entirely — the anonymous-dial path used before
// `auth enable` has run and on clusters that never enable auth.
func New(endpoints []string, tlsConfig *tls.Config, username, password string) (*clientv3.Client, error) {
	cfg := clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		Username:    username,
		Password:    password,
	}
	if tlsConfig != nil {
		cfg.TLS = tlsConfig
	}
	return clientv3.New(cfg)
}

// TLSConfig assembles the client-side *tls.Config from PEM material: caPEM
// (required) verifies the etcd server; certPEM/keyPEM (optional, supplied
// together) present a client certificate for mTLS clusters. Callers fetch
// the PEM bytes from whatever Secret store their transport uses and wrap
// returned errors with the Secret's identity.
func TLSConfig(caPEM, certPEM, keyPEM []byte) (*tls.Config, error) {
	if len(caPEM) == 0 {
		return nil, fmt.Errorf("CA bundle is empty")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA bundle is not valid PEM")
	}
	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if len(certPEM) > 0 || len(keyPEM) > 0 {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("load client keypair: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// BasicAuthCredentials extracts the etcd username/password from a
// kubernetes.io/basic-auth Secret's data. The password key is required and
// must be non-empty; the username defaults to "root" when omitted, since
// etcd requires a user named root to enable auth in the first place.
func BasicAuthCredentials(data map[string][]byte) (username, password string, err error) {
	pw := data[corev1.BasicAuthPasswordKey]
	if len(pw) == 0 {
		return "", "", fmt.Errorf("missing a non-empty %q key", corev1.BasicAuthPasswordKey)
	}
	user := string(data[corev1.BasicAuthUsernameKey])
	if user == "" {
		user = "root"
	}
	return user, string(pw), nil
}
