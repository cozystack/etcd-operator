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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// The admission-side copy of deniedExtraArgs is a CEL rule on
// EtcdClusterSpec.ExtraArgs, written out literally in a kubebuilder marker.
// This drives every denylisted key through a real apiserver with the
// generated CRDs installed, so the two lists cannot drift. `make test` sets
// KUBEBUILDER_ASSETS; without it the test skips.
func TestExtraArgsAdmission_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; envtest harness not initialized")
	}
	env := &envtest.Environment{
		Scheme:                testScheme(t),
		CRDDirectoryPaths:     []string{filepath.Join("..", "charts", "etcd-operator", "crd-bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("envtest start: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	k8s, err := client.New(cfg, client.Options{Scheme: testScheme(t)})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := context.Background()

	cluster := func(name string, extra map[string]string) *lll.EtcdCluster {
		return &lll.EtcdCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
			Spec: lll.EtcdClusterSpec{
				Replicas:  ptrInt32(3),
				Version:   "3.6.11",
				Storage:   lll.StorageSpec{Size: resource.MustParse("128Mi")},
				ExtraArgs: extra,
			},
		}
	}
	reject := func(t *testing.T, extra map[string]string, wantMsg string) {
		t.Helper()
		c := cluster("x", extra)
		err := k8s.Create(ctx, c)
		if err == nil {
			_ = k8s.Delete(ctx, c)
			t.Fatalf("apiserver accepted extraArgs %v; expected rejection", extra)
		}
		if !strings.Contains(err.Error(), wantMsg) {
			t.Fatalf("rejection for %v does not mention %q: %v", extra, wantMsg, err)
		}
	}

	t.Run("allowed flags", func(t *testing.T) {
		c := cluster("extra-ok", map[string]string{
			"log-level":                          "debug",
			"wrapper-mode":                       "dry-run",
			"experimental-initial-corrupt-check": "true",
			"empty-value":                        "",
		})
		if err := k8s.Create(ctx, c); err != nil {
			t.Fatalf("apiserver rejected allowed extraArgs: %v", err)
		}
		_ = k8s.Delete(ctx, c)
	})

	for flag, reason := range deniedExtraArgs {
		t.Run("denied "+flag, func(t *testing.T) {
			want := "--" + flag + " is managed by the operator"
			if field, typed := strings.CutPrefix(reason, "set it through "); typed {
				want = "--" + flag + " is a typed option; set " + field + " instead"
			}
			reject(t, map[string]string{"log-level": "debug", flag: "x"}, want)
		})
	}

	for _, key := range []string{"--log-level", "Log-Level", "log_level", "log-level=debug", "-log", ""} {
		t.Run("malformed key "+key, func(t *testing.T) {
			reject(t, map[string]string{key: "x"}, "spec.extraArgs keys are etcd flag names without the leading dashes")
		})
	}
}
