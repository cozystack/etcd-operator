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
	"errors"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

const (
	testRootSecretName = "test-root-creds"
	testRootPassword   = "s3cret-pw"
)

// rootCredsSecret returns a kubernetes.io/basic-auth Secret holding the etcd
// root credentials the auth tests reference.
func rootCredsSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: testRootSecretName, Namespace: "ns"},
		Type:       corev1.SecretTypeBasicAuth,
		Data: map[string][]byte{
			corev1.BasicAuthUsernameKey: []byte("root"),
			corev1.BasicAuthPasswordKey: []byte(testRootPassword),
		},
	}
}

// enabledAuthSpec is the spec.auth a cluster needs for auth: enabled
// plus the required credentials Secret ref.
func enabledAuthSpec() *lll.AuthSpec {
	return &lll.AuthSpec{
		Enabled:                  true,
		RootCredentialsSecretRef: &corev1.LocalObjectReference{Name: testRootSecretName},
	}
}

// authClusterObjects builds a converged 3-member cluster (ClusterID latched,
// Observed=3, all members Ready) suitable for exercising reconcileAuth. TLS is
// intentionally left unset: at the unit level CEL is not enforced and a nil TLS
// keeps buildOperatorTLSConfig a no-op (the requires-TLS contract is covered by
// the envtest CEL cases). The caller mutates Spec.Auth / Status.AuthEnabled
// per scenario and appends rootCredsSecret() when it wants the Secret present.
// ready controls whether the members report MemberReady=True.
func authClusterObjects(t *testing.T, ready bool) (*lll.EtcdCluster, []client.Object) {
	t.Helper()
	cluster := &lll.EtcdCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns"},
		Spec: lll.EtcdClusterSpec{
			Replicas: ptrInt32(3),
			Version:  "3.5.17",
			Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
		},
		Status: lll.EtcdClusterStatus{
			ClusterToken: "test",
			ClusterID:    "deadbeef",
			Observed: &lll.ObservedClusterSpec{
				Replicas: 3,
				Version:  "3.5.17",
				Storage:  lll.StorageSpec{Size: quickQty(t, "1Gi")},
			},
			ProgressDeadline: &metav1.Time{Time: metav1.Now().Add(60 * 60 * 1e9)},
		},
	}
	objs := []client.Object{cluster}
	for i := 0; i < 3; i++ {
		conds := []metav1.Condition{}
		if ready {
			conds = append(conds, metav1.Condition{
				Type: lll.MemberReady, Status: metav1.ConditionTrue, Reason: "PodReady",
				LastTransitionTime: metav1.Now(),
			})
		}
		objs = append(objs, &lll.EtcdMember{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("test-%d", i),
				Namespace: "ns",
				Labels:    memberLabels("test", fmt.Sprintf("test-%d", i)),
			},
			Spec: lll.EtcdMemberSpec{
				ClusterName: "test", Version: "3.5.17",
				Storage: lll.StorageSpec{Size: quickQty(t, "1Gi")}, InitialCluster: "x", ClusterToken: "test",
			},
			Status: lll.EtcdMemberStatus{PodName: fmt.Sprintf("test-%d", i), MemberID: "abc", Conditions: conds},
		})
	}
	return cluster, objs
}

func reconcileOnce(t *testing.T, r *EtcdClusterReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "test", Namespace: "ns"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

// Happy path: a converged cluster with auth enabled gets the root user
// provisioned with the Secret's password, auth turned on, and
// status.authEnabled latched.
func TestReconcileAuth_EnablesWhenConverged(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if got := fe.userAddCalls; len(got) != 1 || got[0] != "root" {
		t.Fatalf("userAddCalls = %v, want [root]", got)
	}
	if got := fe.userAddPasswords; len(got) != 1 || got[0] != testRootPassword {
		t.Fatalf("userAddPasswords = %v, want [%s] (from the Secret)", got, testRootPassword)
	}
	if got := fe.grantCalls; len(got) != 1 || got[0] != [2]string{"root", "root"} {
		t.Fatalf("grantCalls = %v, want [[root root]]", got)
	}
	if fe.authEnableCalls != 1 {
		t.Fatalf("authEnableCalls = %d, want 1", fe.authEnableCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if !cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled = false, want true")
	}
}

// Crash recovery: etcd already has auth on but the status write was lost. The
// operator must NOT re-run the provisioning RPCs and must re-latch status.
func TestReconcileAuth_AlreadyEnabledInEtcdLatchesStatus(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	fe.authEnabled = true // etcd already enabled from a prior, half-recorded run
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 0 || len(fe.userAddCalls) != 0 || len(fe.grantCalls) != 0 {
		t.Fatalf("auth provisioning re-run on already-enabled cluster: enable=%d add=%v grant=%v",
			fe.authEnableCalls, fe.userAddCalls, fe.grantCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if !cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled = false, want true (should re-latch)")
	}
}

// Crash recovery via the permission-denied path: on an etcd build that guards
// AuthStatus behind auth, an anonymous probe against an already-auth-enabled
// cluster returns "permission denied". The operator must treat that as
// "auth is already on", NOT re-provision, and latch status. This is the
// crash-after-AuthEnable-before-status-write recovery; it exercises the
// isAuthRequiredErr branch (distinct from the st.Enabled==true path).
func TestReconcileAuth_AuthStatusPermissionDeniedLatches(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	fe.authStatusErr = errors.New("etcdserver: permission denied")
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 0 || len(fe.userAddCalls) != 0 || len(fe.grantCalls) != 0 {
		t.Fatalf("re-provisioned on a permission-denied (already-enabled) cluster: enable=%d add=%v grant=%v",
			fe.authEnableCalls, fe.userAddCalls, fe.grantCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if !cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled = false, want true (permission-denied must latch)")
	}
}

// A UserGrantRole failure is a genuine error (etcd grants idempotently with no
// error, so there is no "already granted" sentinel to tolerate). The operator
// must NOT proceed to AuthEnable and must NOT latch status — it retries.
func TestReconcileAuth_GrantRoleErrorRetries(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	// Even the message the old code used to swallow must now be surfaced.
	fe.grantErr = errors.New("etcdserver: role is already granted to the user")
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 0 {
		t.Fatalf("authEnableCalls = %d, want 0 (grant failure must abort before AuthEnable)", fe.authEnableCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled latched despite a UserGrantRole failure")
	}
}

// Once status.authEnabled has latched, reconcileAuth is a pure no-op — it does
// not even dial for auth purposes.
func TestReconcileAuth_StatusAlreadyLatchedIsNoop(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	cluster.Status.AuthEnabled = true
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 0 || len(fe.userAddCalls) != 0 {
		t.Fatalf("auth calls made despite latched status: enable=%d add=%v", fe.authEnableCalls, fe.userAddCalls)
	}
}

// A pre-existing root user must not abort provisioning: the "already exists"
// error is tolerated and the operator proceeds to grant + enable.
func TestReconcileAuth_UserAlreadyExistsTolerated(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	fe.userAddErr = errors.New("etcdserver: user name already exists")
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 1 {
		t.Fatalf("authEnableCalls = %d, want 1 (UserAdd already-exists must be tolerated)", fe.authEnableCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if !cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled = false, want true")
	}
}

// auth enabled + converged but the referenced Secret is missing: the operator
// must NOT enable auth (it would lock itself out) and must not latch status.
func TestReconcileAuth_MissingSecretDoesNotEnable(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	// Deliberately do NOT append rootCredsSecret().
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 0 || len(fe.userAddCalls) != 0 {
		t.Fatalf("auth enabled despite missing credentials secret: enable=%d add=%v", fe.authEnableCalls, fe.userAddCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled latched without a credentials secret")
	}
}

// After auth is enabled (status latched), every operator dial must present the
// root credentials read from the Secret. Drives the converged-promote dial path
// and captures creds.
func TestReconcileAuth_SendsRootCredsAfterEnable(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	cluster.Spec.Auth = enabledAuthSpec()
	cluster.Status.AuthEnabled = true
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	var cap capturedDial
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: capturingFactory(fe, &cap)}

	reconcileOnce(t, r)

	if !cap.called {
		t.Fatalf("factory was never dialled")
	}
	if cap.username != "root" || cap.password != testRootPassword {
		t.Fatalf("dial creds = %q/%q, want root/%s", cap.username, cap.password, testRootPassword)
	}
}

// During the bootstrap window — auth requested but status not yet
// latched — dials must remain anonymous (clientv3 skips Authenticate) and no
// Secret is read. Use a not-yet-ready cluster so reconcileAuth's gate keeps
// auth off, isolating the pre-enable dial from the promote path.
func TestReconcileAuth_NoCredsBeforeEnable(t *testing.T) {
	cluster, objs := authClusterObjects(t, false) // members not Ready ⇒ not converged
	cluster.Spec.Auth = enabledAuthSpec()
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	var cap capturedDial
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: capturingFactory(fe, &cap)}

	reconcileOnce(t, r)

	if cap.called && (cap.username != "" || cap.password != "") {
		t.Fatalf("dial creds = %q/%q, want empty (auth not enabled yet)", cap.username, cap.password)
	}
	if fe.authEnableCalls != 0 {
		t.Fatalf("authEnableCalls = %d, want 0 (cluster not converged)", fe.authEnableCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled latched on a non-converged cluster")
	}
}

// No auth block ⇒ no auth RPCs ever.
func TestReconcileAuth_DisabledMakesNoAuthCalls(t *testing.T) {
	cluster, objs := authClusterObjects(t, true)
	// cluster.Spec.Auth left nil
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 0 || len(fe.userAddCalls) != 0 {
		t.Fatalf("auth calls made with no auth spec: enable=%d add=%v", fe.authEnableCalls, fe.userAddCalls)
	}
	mustGet(t, c, "test", "ns", cluster)
	if cluster.Status.AuthEnabled {
		t.Fatalf("status.authEnabled set with no auth spec")
	}
}

// auth enabled but the cluster has not converged (a member not Ready): the
// gate must keep auth off until convergence.
func TestReconcileAuth_NotConvergedSkips(t *testing.T) {
	cluster, objs := authClusterObjects(t, false)
	cluster.Spec.Auth = enabledAuthSpec()
	objs = append(objs, rootCredsSecret())
	c, _ := newTestClient(t, objs...)
	fe := newFakeEtcd(0xdeadbeef)
	r := &EtcdClusterReconciler{Client: c, Scheme: testScheme(t), EtcdClientFactory: factoryReturning(fe)}

	reconcileOnce(t, r)

	if fe.authEnableCalls != 0 {
		t.Fatalf("authEnableCalls = %d, want 0 (not converged)", fe.authEnableCalls)
	}
}
