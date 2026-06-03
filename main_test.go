/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverClusterDomain(t *testing.T) {
	cases := []struct {
		name     string
		contents string
		want     string
	}{
		{
			name: "standard kubelet-injected resolv.conf",
			contents: `search default.svc.cluster.local svc.cluster.local cluster.local
nameserver 10.96.0.10
options ndots:5
`,
			want: "cluster.local",
		},
		{
			name: "cozystack",
			contents: `search myns.svc.cozy.local svc.cozy.local cozy.local
nameserver 10.96.0.10
options ndots:5
`,
			want: "cozy.local",
		},
		{
			name: "multi-segment cluster domain",
			contents: `search myns.svc.example.internal svc.example.internal example.internal
nameserver 10.96.0.10
`,
			want: "example.internal",
		},
		{
			name: "comments and blank lines",
			contents: `# This is the cluster resolv.conf injected by kubelet.

search   myns.svc.cluster.local   svc.cluster.local   cluster.local
nameserver 10.96.0.10
`,
			want: "cluster.local",
		},
		{
			name: "host-style resolv.conf — no .svc suffix",
			contents: `search example.com
nameserver 8.8.8.8
`,
			want: "",
		},
		{
			name: "no search line at all",
			contents: `nameserver 8.8.8.8
`,
			want: "",
		},
		{
			name:     "empty file",
			contents: "",
			want:     "",
		},
		{
			name: "svc-only entry (degenerate, should not match)",
			contents: `search svc.
nameserver 10.96.0.10
`,
			want: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "resolv.conf")
			if err := os.WriteFile(path, []byte(tc.contents), 0o644); err != nil {
				t.Fatalf("write fixture: %v", err)
			}
			got := discoverClusterDomain(path)
			if got != tc.want {
				t.Fatalf("discoverClusterDomain = %q; want %q", got, tc.want)
			}
		})
	}
}

// TestDiscoverClusterDomain_MissingFile covers the running-outside-a-pod
// case (no /etc/resolv.conf at all, or the operator binary running on a
// developer laptop): the function returns "" so callers fall back to
// the explicit --cluster-domain flag or the default.
func TestDiscoverClusterDomain_MissingFile(t *testing.T) {
	if got := discoverClusterDomain("/no/such/file/anywhere"); got != "" {
		t.Fatalf("missing file: got %q; want empty", got)
	}
}

func TestOperatorImageError(t *testing.T) {
	if err := operatorImageError(placeholderOperatorImage); err == nil {
		t.Errorf("operatorImageError(%q) = nil, want error (placeholder must be rejected)", placeholderOperatorImage)
	}
	// A real image ref and empty (snapshots simply unavailable) are both allowed.
	for _, img := range []string{"registry.example.com/etcd-operator:v1.2.3", ""} {
		if err := operatorImageError(img); err != nil {
			t.Errorf("operatorImageError(%q) = %v, want nil", img, err)
		}
	}
}
