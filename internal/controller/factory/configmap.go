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

package factory

import (
	"context"
	"fmt"
	"strings"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/log"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func GetClusterStateConfigMapName(cluster *etcdaenixiov1alpha1.EtcdCluster) string {
	return cluster.Name + "-cluster-state"
}

func GetClusterStateConfigMap(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) (*corev1.ConfigMap, error) {
	var err error
	state := "new"
	if isEtcdClusterReady(cluster) {
		state = "existing"
	}
	configMap := TemplateClusterStateConfigMap(cluster, state, int(*cluster.Spec.Replicas))
	ctx, err = contextWithGVK(ctx, configMap, rclient.Scheme())
	if err != nil {
		return nil, err
	}

	log.Debug(ctx, "configmap data generated", "data", configMap.Data)

	if err := ctrl.SetControllerReference(cluster, configMap, rclient.Scheme()); err != nil {
		return nil, fmt.Errorf("cannot set controller reference: %w", err)
	}

	return configMap, nil
}

// isEtcdClusterReady returns true if condition "Ready" has progressed
// from reason v1alpha1.EtcdCondTypeWaitingForFirstQuorum.
func isEtcdClusterReady(cluster *etcdaenixiov1alpha1.EtcdCluster) bool {
	cond := meta.FindStatusCondition(cluster.Status.Conditions, etcdaenixiov1alpha1.EtcdConditionReady)
	return cond != nil && (cond.Reason == string(etcdaenixiov1alpha1.EtcdCondTypeStatefulSetReady) ||
		cond.Reason == string(etcdaenixiov1alpha1.EtcdCondTypeStatefulSetNotReady))
}

func TemplateClusterStateConfigMap(
	cluster *etcdaenixiov1alpha1.EtcdCluster, state string, replicas int) *corev1.ConfigMap {

	initialClusterMembers := make([]string, replicas)
	clusterService := fmt.Sprintf(
		"%s.%s.svc:2380",
		GetHeadlessServiceName(cluster), cluster.Namespace)

	for i := 0; i < replicas; i++ {
		podName := fmt.Sprintf("%s-%d", cluster.Name, i)
		initialClusterMembers[i] = fmt.Sprintf("%s=https://%s.%s",
			podName, podName, clusterService,
		)
	}
	initialCluster := strings.Join(initialClusterMembers, ",")

	configMap := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-cluster-state", cluster.Name),
			Namespace: cluster.Namespace,
		},
		Data: map[string]string{
			"ETCD_INITIAL_CLUSTER_STATE": state,
			"ETCD_INITIAL_CLUSTER":       initialCluster,
			"ETCD_INITIAL_CLUSTER_TOKEN": fmt.Sprintf("%s-%s", cluster.Name, cluster.Namespace),
		},
	}
	return configMap
}
