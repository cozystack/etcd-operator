/*
Copyright 2024 The etcd-operator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/cozystack/etcd-operator/internal/etcdclient"
	"github.com/cozystack/etcd-operator/internal/migrate"
	"github.com/cozystack/etcd-operator/internal/portforward"
)

// inspectCluster gathers everything BuildAdoption needs from one LIVE legacy
// cluster: the etcd member list + cluster ID + auth status (read-only RPCs
// over a port-forward, authenticated the same way the legacy operator
// dialed), and the matching pods/PVCs from the apiserver. Read-only — safe
// in dry-run, where it makes the rendered plan concrete instead of
// placeholder-ridden.
func inspectCluster(ctx context.Context, restCfg *rest.Config, kube kubernetes.Interface, lc legacyCluster) (migrate.ClusterFacts, error) {
	var facts migrate.ClusterFacts

	pods, err := kube.CoreV1().Pods(lc.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/name=etcd,app.kubernetes.io/instance=" + lc.Name,
	})
	if err != nil {
		return facts, fmt.Errorf("list etcd pods: %w", err)
	}
	podByName := map[string]*corev1.Pod{}
	var dialPod string
	for i := range pods.Items {
		p := &pods.Items[i]
		podByName[p.Name] = p
		if dialPod == "" && p.Status.Phase == corev1.PodRunning {
			dialPod = p.Name
		}
	}
	if dialPod == "" {
		return facts, fmt.Errorf("no Running etcd pod found for legacy cluster %s/%s", lc.Namespace, lc.Name)
	}

	localPort, stop, err := portforward.ForwardToPod(restCfg, lc.Namespace, dialPod, 2379)
	if err != nil {
		return facts, fmt.Errorf("port-forward to %s: %w", dialPod, err)
	}
	defer stop()

	tlsCfg, err := legacyOperatorTLSConfig(ctx, kube, lc)
	if err != nil {
		return facts, err
	}
	cli, err := etcdclient.New([]string{fmt.Sprintf("localhost:%d", localPort)}, tlsCfg, "", "")
	if err != nil {
		return facts, fmt.Errorf("dial legacy etcd: %w", err)
	}
	defer func() { _ = cli.Close() }()

	listCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := cli.MemberList(listCtx)
	if err != nil {
		return facts, fmt.Errorf("etcd MemberList: %w", err)
	}
	facts.ClusterIDHex = fmt.Sprintf("%016x", resp.Header.ClusterId)

	for _, m := range resp.Members {
		fact := migrate.MemberFact{
			Name:      m.Name,
			IDHex:     fmt.Sprintf("%016x", m.ID),
			IsLearner: m.IsLearner,
		}
		if len(m.PeerURLs) > 0 {
			fact.PeerURL = m.PeerURLs[0]
		}
		// Every member must be backed by a same-name pod: the legacy
		// operator ran members with --name=$(POD_NAME), and the adopted
		// EtcdMember CR name doubles as the pod lookup key. A missing pod
		// surfaces as PodUID=="" and BuildAdoption turns it into a plan
		// error.
		if pod, ok := podByName[m.Name]; ok && pod.Status.Phase == corev1.PodRunning {
			fact.PodUID = string(pod.UID)
		}
		facts.Members = append(facts.Members, fact)
	}
	if len(facts.Members) == 0 {
		return facts, fmt.Errorf("etcd reported an empty member list")
	}

	authCtx, cancel2 := context.WithTimeout(ctx, 10*time.Second)
	defer cancel2()
	status, err := cli.AuthStatus(authCtx)
	if err != nil {
		return facts, fmt.Errorf("etcd AuthStatus: %w", err)
	}
	facts.AuthEnabled = status.Enabled

	return facts, nil
}

// verifyAdoptionPVCs checks that every adopted member's PVC exists before
// anything is mutated. ensurePVC on the new member controller hard-fails on
// a missing or foreign-owned PVC, so catch it at plan time with a precise
// message instead.
func verifyAdoptionPVCs(ctx context.Context, kube kubernetes.Interface, namespace string, plan *migrate.ResourcePlan) {
	for _, ma := range plan.Adoption.Members {
		pvc, err := kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, ma.PVCName, metav1.GetOptions{})
		if err != nil {
			plan.Errors = append(plan.Errors, fmt.Sprintf("PVC %q for member %q: %v", ma.PVCName, ma.Member.Name, err))
			continue
		}
		for _, o := range pvc.OwnerReferences {
			if o.Controller != nil && *o.Controller && o.Kind != "EtcdMember" {
				plan.Errors = append(plan.Errors, fmt.Sprintf(
					"PVC %q is controller-owned by %s %q; refusing to re-own it", ma.PVCName, o.Kind, o.Name))
			}
		}
	}
	if len(plan.Errors) > 0 {
		plan.Action = migrate.ActionError
		plan.DeleteRef = nil
	}
}

// legacyOperatorTLSConfig assembles the client TLS config the legacy
// operator dialed with: CA from serverTrustedCASecret (falling back to the
// server secret's ca.crt), identity from clientSecret. ServerName pins the
// expected SAN — the legacy client Service DNS — because the port-forward
// connects to localhost, which is never in the cert.
func legacyOperatorTLSConfig(ctx context.Context, kube kubernetes.Interface, lc legacyCluster) (*tls.Config, error) {
	if lc.Spec.Security == nil || lc.Spec.Security.TLS.ServerSecret == "" {
		return nil, nil // plaintext legacy cluster
	}
	t := lc.Spec.Security.TLS

	caSecret := t.ServerTrustedCASecret
	if caSecret == "" {
		caSecret = t.ServerSecret
	}
	caData, err := secretKey(ctx, kube, lc.Namespace, caSecret, "ca.crt")
	if err != nil {
		return nil, err
	}

	var certPEM, keyPEM []byte
	if t.ClientSecret != "" {
		if certPEM, err = secretKey(ctx, kube, lc.Namespace, t.ClientSecret, "tls.crt"); err != nil {
			return nil, err
		}
		if keyPEM, err = secretKey(ctx, kube, lc.Namespace, t.ClientSecret, "tls.key"); err != nil {
			return nil, err
		}
	}

	tlsCfg, err := etcdclient.TLSConfig(caData, certPEM, keyPEM)
	if err != nil {
		return nil, fmt.Errorf("build TLS config from legacy secrets: %w", err)
	}
	tlsCfg.ServerName = fmt.Sprintf("%s.%s.svc", lc.Name, lc.Namespace)
	return tlsCfg, nil
}

// secretKey fetches one key of one Secret, with precise errors.
func secretKey(ctx context.Context, kube kubernetes.Interface, namespace, name, key string) ([]byte, error) {
	sec, err := kube.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read secret %s/%s: %w", namespace, name, err)
	}
	data := sec.Data[key]
	if len(data) == 0 {
		return nil, fmt.Errorf("secret %s/%s has no %q key", namespace, name, key)
	}
	return data, nil
}
