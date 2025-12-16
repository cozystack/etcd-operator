package controller

import (
	"context"
	"fmt"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller/factory"
	"github.com/aenix-io/etcd-operator/internal/log"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/api/policy/v1"
)

func CreateOrUpdateClusterStateConfigMap(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {
	configMap, err := factory.GetClusterStateConfigMap(ctx, cluster, rclient)
	if  err != nil{
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
		return deleteOwnedResource(ctx, rclient, &v1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: cluster.Namespace,
				Name:      cluster.Name,
			},
		})
	}

	pdb, err := factory.GetPdb(ctx, cluster, rclient)
	if err != nil{
		return err
	}

	return reconcileOwnedResource(ctx, rclient, pdb)
}

func CreateOrUpdateStatefulSet(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {
	statefulSet, err := factory.GetStatefulSet(ctx, cluster, rclient);
	if err != nil{
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
	if err != nil{
		return nil
	}
	return reconcileOwnedResource(ctx, rclient, svc)
}

func CreateOrUpdateClientService(
	ctx context.Context,
	cluster *etcdaenixiov1alpha1.EtcdCluster,
	rclient client.Client,
) error {
	svc, err := factory.GetClientService(ctx, cluster, rclient)
	if err != nil{
		return nil
	}
	return reconcileOwnedResource(ctx, rclient, svc)
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
