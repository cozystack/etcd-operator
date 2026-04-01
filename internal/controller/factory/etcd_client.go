package factory

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"time"

	"github.com/aenix-io/etcd-operator/api/v1alpha1"
	clientv3 "go.etcd.io/etcd/client/v3"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func NewEtcdClientSet(ctx context.Context, cluster *v1alpha1.EtcdCluster, cli client.Client) (*clientv3.Client, []*clientv3.Client, error) {
	cfg, err := configFromCluster(ctx, cluster, cli)
	if err != nil {
		return nil, nil, err
	}
	if len(cfg.Endpoints) == 0 {
		return nil, nil, nil
	}
	eps := cfg.Endpoints
	clusterClient, err := clientv3.New(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("error building etcd cluster client: %w", err)
	}
	singleClients := make([]*clientv3.Client, len(eps))
	for i, ep := range eps {
		cfg.Endpoints = []string{ep}
		singleClients[i], err = clientv3.New(cfg)
		if err != nil {
			// nolint:errcheck
			clusterClient.Close()
			return nil, nil, fmt.Errorf("error building etcd single-endpoint client for endpoint %s: %w", ep, err)
		}
	}
	return clusterClient, singleClients, nil
}

func configFromCluster(ctx context.Context, cluster *v1alpha1.EtcdCluster, cli client.Client) (clientv3.Config, error) {
	var sliceList discoveryv1.EndpointSliceList
	serviceName := GetHeadlessServiceName(cluster)
	err := cli.List(ctx, &sliceList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(labels.Set{
			discoveryv1.LabelServiceName: serviceName,
		})},
	)
	if err != nil {
		return clientv3.Config{}, err
	}
	if len(sliceList.Items) == 0 {
		return clientv3.Config{Endpoints: []string{}}, nil
	}

	names := map[string]struct{}{}
	urls := make([]string, 0, 8)
	for _, slice := range sliceList.Items {
		for _, ep := range slice.Endpoints {
			if ep.Hostname != nil {
				names[*ep.Hostname] = struct{}{}
			}
		}
	}
	for name := range names {
		urls = append(urls, fmt.Sprintf("%s.%s.%s.svc:%s", name, serviceName, cluster.Namespace, "2379"))
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
