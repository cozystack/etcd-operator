package v1alpha1

import (
	"context"
	"errors"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func SetupIndexers(ctx context.Context, mgr ctrl.Manager) error {
	var err error
	for _, f := range []func(context.Context, ctrl.Manager) error{
		setupEtcdClusterRefIndexer,
	} {
		err = errors.Join(err, f(ctx, mgr))
	}
	return err
}

const EtcdClusterRefIndex = "etcdcluster_ref"

func setupEtcdClusterRefIndexer(ctx context.Context, mgr ctrl.Manager) error {
	return mgr.GetFieldIndexer().IndexField(ctx, &EtcdBackupSchedule{}, EtcdClusterRefIndex, extractEtcdClusterRefFromEtcdBackupSchedule)
}

func extractEtcdClusterRefFromEtcdBackupSchedule(raw client.Object) []string {
	etcdBackupSchedule, ok := raw.(*EtcdBackupSchedule)
	if !ok {
		return nil
	}
	return []string{etcdBackupSchedule.Spec.ClusterRef.Name}
}
