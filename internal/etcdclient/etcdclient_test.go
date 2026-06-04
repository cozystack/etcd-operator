/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package etcdclient

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

// selfSignedPEM generates a throwaway self-signed cert+key pair for
// exercising the PEM-assembly paths without fixture files.
func selfSignedPEM(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM
}

// TestTLSConfig covers the shared TLS assembly: CA-only (server TLS), CA +
// keypair (mTLS), and the failure modes both the operator and the plugin
// rely on surfacing as errors rather than half-built configs.
func TestTLSConfig(t *testing.T) {
	certPEM, keyPEM := selfSignedPEM(t)

	t.Run("ca only", func(t *testing.T) {
		cfg, err := TLSConfig(certPEM, nil, nil)
		if err != nil {
			t.Fatalf("TLSConfig: %v", err)
		}
		if cfg.RootCAs == nil {
			t.Error("RootCAs not populated")
		}
		if len(cfg.Certificates) != 0 {
			t.Errorf("expected no client certificates, got %d", len(cfg.Certificates))
		}
		if cfg.MinVersion != tls.VersionTLS12 {
			t.Errorf("MinVersion = %x, want TLS 1.2", cfg.MinVersion)
		}
	})

	t.Run("ca plus keypair", func(t *testing.T) {
		cfg, err := TLSConfig(certPEM, certPEM, keyPEM)
		if err != nil {
			t.Fatalf("TLSConfig: %v", err)
		}
		if len(cfg.Certificates) != 1 {
			t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
		}
	})

	t.Run("empty ca", func(t *testing.T) {
		if _, err := TLSConfig(nil, nil, nil); err == nil {
			t.Error("expected error for empty CA bundle")
		}
	})

	t.Run("garbage ca", func(t *testing.T) {
		if _, err := TLSConfig([]byte("not pem"), nil, nil); err == nil {
			t.Error("expected error for non-PEM CA bundle")
		}
	})

	t.Run("broken keypair", func(t *testing.T) {
		if _, err := TLSConfig(certPEM, certPEM, []byte("not a key")); err == nil {
			t.Error("expected error for unparsable keypair")
		}
	})
}

// TestBasicAuthCredentials covers the shared kubernetes.io/basic-auth
// parsing: password required, username defaulting to root.
func TestBasicAuthCredentials(t *testing.T) {
	t.Run("username defaults to root", func(t *testing.T) {
		user, pass, err := BasicAuthCredentials(map[string][]byte{"password": []byte("s3cr3t")})
		if err != nil {
			t.Fatalf("BasicAuthCredentials: %v", err)
		}
		if user != "root" || pass != "s3cr3t" {
			t.Errorf("got (%q, %q), want (root, s3cr3t)", user, pass)
		}
	})

	t.Run("explicit username honored", func(t *testing.T) {
		user, _, err := BasicAuthCredentials(map[string][]byte{
			"username": []byte("alice"), "password": []byte("pw"),
		})
		if err != nil {
			t.Fatalf("BasicAuthCredentials: %v", err)
		}
		if user != "alice" {
			t.Errorf("username = %q, want alice", user)
		}
	})

	t.Run("missing password errors", func(t *testing.T) {
		if _, _, err := BasicAuthCredentials(map[string][]byte{"username": []byte("root")}); err == nil {
			t.Error("expected error for missing password")
		}
	})

	t.Run("empty password errors", func(t *testing.T) {
		if _, _, err := BasicAuthCredentials(map[string][]byte{"password": {}}); err == nil {
			t.Error("expected error for empty password")
		}
	})
}
