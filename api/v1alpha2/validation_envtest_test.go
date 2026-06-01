/*
Copyright 2023 Timofey Larkin.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha2_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// envtest harness for CEL CRD validation rules. CEL XValidation is
// apiserver-enforced (no separate webhook process), so the only way to
// test these contracts is to spin up a real apiserver, install the
// CRDs, and observe what kubectl-equivalent operations the apiserver
// accepts or rejects.
//
// KUBEBUILDER_ASSETS must point at envtest binaries; `make test` sets
// this automatically. Raw `go test ./api/...` requires it set
// (otherwise this package skips, so it doesn't fail dev IDE runs).

var (
	testEnv *envtest.Environment
	k8s     ctrlclient.Client
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Fprintln(os.Stderr, "skipping v1alpha2 CEL envtest: KUBEBUILDER_ASSETS not set")
		os.Exit(0)
	}

	scheme := k8sruntime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(lll.AddToScheme(scheme))

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{crdBasesDir()},
		ErrorIfCRDPathMissing: true,
		Scheme:                scheme,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "envtest start: %v\n", err)
		os.Exit(1)
	}

	k8s, err = ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		_ = testEnv.Stop()
		fmt.Fprintf(os.Stderr, "client construct: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	_ = testEnv.Stop()
	os.Exit(code)
}

// crdBasesDir resolves config/crd/bases relative to this test file —
// go test's CWD is the package directory and the CRDs live two levels up.
func crdBasesDir() string {
	_, here, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(here), "..", "..", "config", "crd", "bases")
}

func ptr32(v int32) *int32 { return &v }

// validCluster returns a baseline EtcdCluster spec that passes every
// CEL rule. Tests mutate one field and assert the apiserver's
// accept/reject response on that field.
func validCluster(name string) *lll.EtcdCluster {
	return &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptr32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: resource.MustParse("128Mi")},
		},
	}
}
