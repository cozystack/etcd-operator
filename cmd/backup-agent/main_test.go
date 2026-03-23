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
	"path/filepath"
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
