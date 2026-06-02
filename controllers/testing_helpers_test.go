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
	"testing"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	lll "github.com/cozystack/etcd-operator/api/v1alpha2"
)

// fakeEtcd is a deterministic in-memory stand-in for clientv3.Client used to
// drive cluster-level operations in unit tests. Each method records the calls
// it received and a presetable error; tests assert on the recorded calls and
// on the resulting state.
type fakeEtcd struct {
	clusterID uint64
	members   []*etcdserverpb.Member

	listErr    error
	addErr     error
	removeErr  error
	promoteErr error

	addCalls        []string
	addLearnerCalls []string
	promoteCalls    []uint64
	removeCalls     []uint64
	closed          bool

	// Auth surface.
	authEnabled   bool
	authStatusErr error
	userAddErr    error
	grantErr      error
	authEnableErr error

	userAddCalls     []string
	userAddPasswords []string
	grantCalls       [][2]string
	authEnableCalls  int
}

func newFakeEtcd(clusterID uint64, members ...*etcdserverpb.Member) *fakeEtcd {
	return &fakeEtcd{clusterID: clusterID, members: members}
}

func (f *fakeEtcd) MemberList(_ context.Context, _ ...clientv3.OpOption) (*clientv3.MemberListResponse, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	return &clientv3.MemberListResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: f.clusterID},
		Members: f.members,
	}, nil
}

func (f *fakeEtcd) MemberAdd(_ context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	if f.addErr != nil {
		return nil, f.addErr
	}
	f.addCalls = append(f.addCalls, peerAddrs...)
	id := uint64(0xaa00) + uint64(len(f.members)+1)
	m := &etcdserverpb.Member{ID: id, PeerURLs: peerAddrs}
	f.members = append(f.members, m)
	return &clientv3.MemberAddResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: f.clusterID},
		Member:  m,
		Members: f.members,
	}, nil
}

func (f *fakeEtcd) MemberAddAsLearner(_ context.Context, peerAddrs []string) (*clientv3.MemberAddResponse, error) {
	if f.addErr != nil {
		return nil, f.addErr
	}
	f.addLearnerCalls = append(f.addLearnerCalls, peerAddrs...)
	id := uint64(0xbb00) + uint64(len(f.members)+1)
	m := &etcdserverpb.Member{ID: id, PeerURLs: peerAddrs, IsLearner: true}
	f.members = append(f.members, m)
	return &clientv3.MemberAddResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: f.clusterID},
		Member:  m,
		Members: f.members,
	}, nil
}

func (f *fakeEtcd) MemberPromote(_ context.Context, id uint64) (*clientv3.MemberPromoteResponse, error) {
	if f.promoteErr != nil {
		return nil, f.promoteErr
	}
	f.promoteCalls = append(f.promoteCalls, id)
	for _, m := range f.members {
		if m.ID == id {
			m.IsLearner = false
		}
	}
	return &clientv3.MemberPromoteResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: f.clusterID},
		Members: f.members,
	}, nil
}

func (f *fakeEtcd) MemberRemove(_ context.Context, id uint64) (*clientv3.MemberRemoveResponse, error) {
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	f.removeCalls = append(f.removeCalls, id)
	var kept []*etcdserverpb.Member
	for _, m := range f.members {
		if m.ID != id {
			kept = append(kept, m)
		}
	}
	f.members = kept
	return &clientv3.MemberRemoveResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: f.clusterID},
		Members: f.members,
	}, nil
}

func (f *fakeEtcd) AuthStatus(_ context.Context) (*clientv3.AuthStatusResponse, error) {
	if f.authStatusErr != nil {
		return nil, f.authStatusErr
	}
	return &clientv3.AuthStatusResponse{
		Header:  &etcdserverpb.ResponseHeader{ClusterId: f.clusterID},
		Enabled: f.authEnabled,
	}, nil
}

func (f *fakeEtcd) AuthEnable(_ context.Context) (*clientv3.AuthEnableResponse, error) {
	if f.authEnableErr != nil {
		return nil, f.authEnableErr
	}
	f.authEnableCalls++
	f.authEnabled = true
	return &clientv3.AuthEnableResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: f.clusterID}}, nil
}

func (f *fakeEtcd) UserAdd(_ context.Context, name, password string) (*clientv3.AuthUserAddResponse, error) {
	if f.userAddErr != nil {
		return nil, f.userAddErr
	}
	f.userAddCalls = append(f.userAddCalls, name)
	f.userAddPasswords = append(f.userAddPasswords, password)
	return &clientv3.AuthUserAddResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: f.clusterID}}, nil
}

func (f *fakeEtcd) UserGrantRole(_ context.Context, user, role string) (*clientv3.AuthUserGrantRoleResponse, error) {
	if f.grantErr != nil {
		return nil, f.grantErr
	}
	f.grantCalls = append(f.grantCalls, [2]string{user, role})
	return &clientv3.AuthUserGrantRoleResponse{Header: &etcdserverpb.ResponseHeader{ClusterId: f.clusterID}}, nil
}

func (f *fakeEtcd) Close() error { f.closed = true; return nil }

func factoryReturning(c EtcdClusterClient) EtcdClientFactory {
	return func(_ context.Context, _ []string, _ *tls.Config, _, _ string) (EtcdClusterClient, error) {
		return c, nil
	}
}

// capturingFactory returns the given client and records the username/password
// it was last dialled with, so tests can assert whether credentials were sent.
type capturedDial struct {
	username string
	password string
	called   bool
}

func capturingFactory(c EtcdClusterClient, capture *capturedDial) EtcdClientFactory {
	return func(_ context.Context, _ []string, _ *tls.Config, username, password string) (EtcdClusterClient, error) {
		capture.username = username
		capture.password = password
		capture.called = true
		return c, nil
	}
}

// failingFactory returns an error from every Build call, simulating an
// unreachable etcd cluster.
func failingFactory(err error) EtcdClientFactory {
	return func(_ context.Context, _ []string, _ *tls.Config, _, _ string) (EtcdClusterClient, error) {
		return nil, err
	}
}

// testScheme returns a runtime scheme registered with corev1 and v1alpha2.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(client-go): %v", err)
	}
	if err := lll.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme(v1alpha2): %v", err)
	}
	return s
}

// newTestClient builds a controller-runtime fake client with the v1alpha2
// types pre-registered with status subresources.
func newTestClient(t *testing.T, objs ...client.Object) (client.Client, *runtime.Scheme) {
	t.Helper()
	s := testScheme(t)
	// Register the status subresource for our CRs. Without this,
	// Status().Update() on a fake-client object (from v0.15+) writes to a
	// separate store the regular Get reads from, producing surprising
	// behaviour. The cluster controller's locking-pattern tests in
	// particular depend on Status().Update being a real partial-update
	// path.
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&lll.EtcdCluster{}, &lll.EtcdMember{}, &lll.EtcdBackup{}).
		Build()
	return c, s
}

// erroringGetClient wraps a client.Client and returns a preset error for
// any Get call whose target Kind matches. Used to drive the
// transient-apiserver-error code paths.
type erroringGetClient struct {
	client.Client
	failOnKind string
	err        error
}

func (e *erroringGetClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if e.failOnKind != "" {
		switch obj.(type) {
		case *lll.EtcdCluster:
			if e.failOnKind == "EtcdCluster" {
				return e.err
			}
		}
	}
	return e.Client.Get(ctx, key, obj, opts...)
}

func mustGet[T client.Object](t *testing.T, c client.Client, name, ns string, into T) T {
	t.Helper()
	if err := c.Get(context.Background(), types.NamespacedName{Name: name, Namespace: ns}, into); err != nil {
		t.Fatalf("Get(%s/%s): %v", ns, name, err)
	}
	return into
}

func ptrInt32(i int32) *int32 { return &i }

// quickQty parses a small Kubernetes quantity literal ("1Gi", "100m") and
// fatals on error — fine for fixtures.
func quickQty(t *testing.T, s string) resource.Quantity {
	t.Helper()
	q, err := resource.ParseQuantity(s)
	if err != nil {
		t.Fatalf("ParseQuantity(%q): %v", s, err)
	}
	return q
}

// readyPodCondition returns a fake Pod set to Ready=True. Tests use it to
// drive the EtcdMember reconciler past its podReady gate.
func readyPodCondition() corev1.PodCondition {
	return corev1.PodCondition{
		Type:               corev1.PodReady,
		Status:             corev1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}
}
