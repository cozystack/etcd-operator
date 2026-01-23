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

package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
	"github.com/aenix-io/etcd-operator/internal/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func CreateOrUpdateClusterStateConfigMap(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {
	configMap, err := factory.GetClusterStateConfigMap(ctx, cluster, rclient)
	if err != nil {
		return err
	}
	return reconcileOwnedResource(ctx, rclient, configMap)
}

func CreateOrUpdatePdb(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {

	if cluster.Spec.PodDisruptionBudgetTemplate == nil {
		ctx = log.WithValues(ctx, "group", "policy/v1", "kind", "PodDisruptionBudget", "name", cluster.Name)
		return deleteOwnedResource(ctx, rclient, &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: cluster.Namespace,
				Name:      cluster.Name,
			},
		})
	}

	pdb, err := factory.GetPdb(ctx, cluster, rclient)
	if err != nil {
		return err
	}

	return reconcileOwnedResource(ctx, rclient, pdb)
}

func CreateOrUpdateStatefulSet(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {
	statefulSet, err := factory.GetStatefulSet(ctx, cluster, rclient)
	if err != nil {
		return err
	}
	return reconcileOwnedResource(ctx, rclient, statefulSet)
}

func CreateOrUpdateHeadlessService(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {
	svc, err := factory.GetHeadlessService(ctx, cluster, rclient)
	if err != nil {
		return err
	}
	return reconcileOwnedResource(ctx, rclient, svc)
}

func CreateOrUpdateClientService(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {
	svc, err := factory.GetClientService(ctx, cluster, rclient)
	if err != nil {
		return err
	}
	return reconcileOwnedResource(ctx, rclient, svc)
}

//nolint:unused
func GetPersistentVolumeClaims(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	cli client.Client,
) ([]corev1.PersistentVolumeClaim, error) {
	labels := factory.PVCLabels(cluster)
	pvcs := corev1.PersistentVolumeClaimList{}
	err := cli.List(ctx, &pvcs, client.MatchingLabels(labels))
	if err != nil {
		return nil, err
	}
	return pvcs.Items, nil
}

// UpdatePersistentVolumeClaims checks and updates the sizes of PVCs in an EtcdCluster if the specified storage size is larger than the current.
func UpdatePersistentVolumeClaims(ctx context.Context, cluster *etcdaenixiov1alpha1.EtcdCluster, rclient client.Client) error {
	labelSelector := labels.SelectorFromSet(labels.Set{
		"app.kubernetes.io/instance": cluster.Name,
	})
	listOptions := &client.ListOptions{
		Namespace:     cluster.Namespace,
		LabelSelector: labelSelector,
	}

	// List all PVCs in the same namespace as the cluster using the label selector
	pvcList := &corev1.PersistentVolumeClaimList{}
	err := rclient.List(ctx, pvcList, listOptions)
	if err != nil {
		return fmt.Errorf("failed to list PVCs: %w", err)
	}

	// Desired size from the cluster spec
	expectedPrefix := fmt.Sprintf("data-%s-", cluster.Name)
	desiredSize := cluster.Spec.Storage.VolumeClaimTemplate.Spec.Resources.Requests[corev1.ResourceStorage]

	for _, pvc := range pvcList.Items {
		// Skip if PVC name does not match expected prefix
		if !strings.HasPrefix(pvc.Name, expectedPrefix) {
			continue
		}

		// Skip if specified StorageClass does not support volume expansion
		if pvc.Spec.StorageClassName != nil {
			sc := &storagev1.StorageClass{}
			scName := *pvc.Spec.StorageClassName
			err := rclient.Get(ctx, types.NamespacedName{Name: scName}, sc)
			if err != nil {
				return fmt.Errorf("failed to get StorageClass '%s' for PVC '%s': %w", scName, pvc.Name, err)
			}
			if sc.AllowVolumeExpansion == nil || !*sc.AllowVolumeExpansion {
				continue
			}
		}

		currentSize := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		// Only patch if the desired size is greater than the current size
		if desiredSize.Cmp(currentSize) == 1 {
			newSizePatch := []byte(fmt.Sprintf(`{"spec": {"resources": {"requests": {"storage": "%s"}}}}`, desiredSize.String()))
			err = rclient.Patch(ctx, &pvc, client.RawPatch(types.StrategicMergePatchType, newSizePatch))
			if err != nil {
				return fmt.Errorf("failed to patch PVC %s for updated size: %w", pvc.Name, err)
			}
		}
	}

	return nil
}

func GetClientConfigFromCluster(
	ctx context.Context, cluster *etcdaenixiov1alpha1.EtcdCluster, cli client.Client) (*clientv3.Config, error) {
	cfg, err := configFromCluster(ctx, cluster, cli)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

func configFromCluster(ctx context.Context, cluster *etcdaenixiov1alpha1.EtcdCluster, cli client.Client) (clientv3.Config, error) {
	ep := v1.Endpoints{}
	err := cli.Get(ctx, types.NamespacedName{Name: factory.GetHeadlessServiceName(cluster), Namespace: cluster.Namespace}, &ep)
	if client.IgnoreNotFound(err) != nil {
		return clientv3.Config{}, err
	}
	if err != nil {
		return clientv3.Config{Endpoints: []string{}}, nil
	}

	names := map[string]struct{}{}
	urls := make([]string, 0, 8)
	for _, v := range ep.Subsets {
		for _, addr := range v.Addresses {
			names[addr.Hostname] = struct{}{}
		}
		for _, addr := range v.NotReadyAddresses {
			names[addr.Hostname] = struct{}{}
		}
	}
	for name := range names {
		urls = append(urls, fmt.Sprintf("%s.%s.%s.svc:%s", name, ep.Name, ep.Namespace, "2379"))
	}
	etcdClient := clientv3.Config{Endpoints: urls, DialTimeout: 5 * time.Second}

	if s := cluster.Spec.Security; s != nil {
		if s.TLS.ClientSecret == "" {
			return clientv3.Config{}, fmt.Errorf("cannot configure client: security enabled, but client certificates not set")
		}
		clientSecret := &v1.Secret{}
		err := cli.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: s.TLS.ClientSecret}, clientSecret)
		if err != nil {
			return clientv3.Config{}, fmt.Errorf("cannot configure client: failed to get client certificate: %w", err)
		}
		cert, err := parseTLSSecret(clientSecret)
		if err != nil {
			return clientv3.Config{}, fmt.Errorf("cannot configure client: failed to extract keypair from client secret: %w", err)
		}
		etcdClient.TLS = &tls.Config{}
		etcdClient.TLS.Certificates = []tls.Certificate{cert}
		etcdClient.TLS.GetClientCertificate = func(cri *tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &etcdClient.TLS.Certificates[0], nil
		}
		caSecret := &v1.Secret{}
		err = cli.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: s.TLS.ServerSecret}, caSecret)
		if err != nil {
			return clientv3.Config{}, fmt.Errorf("cannot configure client: failed to get server CA secret: %w", err)
		}
		ca, err := parseTLSSecretCA(caSecret)
		if err != nil {
			return clientv3.Config{}, fmt.Errorf("cannot configure client: failed to extract Root CA from server secret: %w", err)
		}
		pool := x509.NewCertPool()
		pool.AddCert(ca)
		etcdClient.TLS.RootCAs = pool
	}

	return etcdClient, nil
}

func parseTLSSecret(secret *v1.Secret) (cert tls.Certificate, err error) {
	certPem, ok := secret.Data["tls.crt"]
	if !ok {
		return tls.Certificate{}, fmt.Errorf("tls.crt not found in secret")
	}
	keyPem, ok := secret.Data["tls.key"]
	if !ok {
		return tls.Certificate{}, fmt.Errorf("tls.key not found in secret")
	}
	cert, err = tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		err = fmt.Errorf("failed to load x509 key pair: %w", err)
	}
	return
}

func parseTLSSecretCA(secret *v1.Secret) (ca *x509.Certificate, err error) {
	caPemBytes, ok := secret.Data["ca.crt"]
	if !ok {
		err = fmt.Errorf("secret does not contain ca.crt")
		return
	}
	block, _ := pem.Decode(caPemBytes)
	if block == nil {
		err = fmt.Errorf("failed to decode PEM bytes")
		return
	}
	ca, err = x509.ParseCertificate(block.Bytes)
	return
}
func reconcileOwnedResource(ctx context.Context, c client.Client, resource client.Object) error {
	if resource == nil {
		return fmt.Errorf("resource cannot be nil")
	}
	log.Debug(ctx, "reconciling owned resource")

	base := resource.DeepCopyObject().(client.Object)
	err := c.Get(ctx, client.ObjectKeyFromObject(resource), base)
	if err == nil {
		log.Debug(ctx, "updating owned resource")
		resource.SetAnnotations(labels.Merge(base.GetAnnotations(), resource.GetAnnotations()))
		resource.SetResourceVersion(base.GetResourceVersion())
		log.Debug(ctx, "owned resource annotations merged", "annotations", resource.GetAnnotations())
		return c.Update(ctx, resource)
	}
	if errors.IsNotFound(err) {
		log.Debug(ctx, "creating new owned resource")
		return c.Create(ctx, resource)
	}
	return fmt.Errorf("error getting owned resource: %w", err)
}

func deleteOwnedResource(ctx context.Context, c client.Client, resource client.Object) error {
	if resource == nil {
		return fmt.Errorf("resource cannot be nil")
	}
	log.Debug(ctx, "deleting owned resource")
	return client.IgnoreNotFound(c.Delete(ctx, resource))
}
