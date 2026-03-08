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

package main

import (
	"context"
	"crypto/tls"
	"os"
	"syscall"

	"github.com/aenix-io/etcd-operator/cmd/app"
	"github.com/aenix-io/etcd-operator/internal/log"
	"github.com/aenix-io/etcd-operator/internal/signal"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	etcdaenixiov1alpha1 "github.com/aenix-io/etcd-operator/api/v1alpha1"
	"github.com/aenix-io/etcd-operator/internal/controller"
	// +kubebuilder:scaffold:imports
)

var (
	scheme  = runtime.NewScheme()
	signals = []os.Signal{os.Interrupt, syscall.SIGTERM}
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(etcdaenixiov1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

func main() {
	flags := app.ParseCmdLine()
	ctx := signal.NotifyContext(log.Setup(context.TODO(), app.LogParameters(flags)), signals...)
	ctrl.SetLogger(logr.FromContextOrDiscard(ctx))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancelation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		log.Info(ctx, "disabling http/2")
		c.NextProtos = []string{"http/1.1"}
	}

	tlsOpts := make([]func(*tls.Config), 0)
	if !flags.EnableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	webhookServer := webhook.NewServer(webhook.Options{
		TLSOpts: tlsOpts,
	})

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: scheme,
		Metrics: metricsserver.Options{
			BindAddress:   flags.MetricsAddress,
			SecureServing: flags.SecureMetrics,
			TLSOpts:       tlsOpts,
		},
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: flags.ProbeAddress,
		LeaderElection:         flags.LeaderElection,
		LeaderElectionID:       "1b04a718.etcd.aenix.io",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		log.Error(ctx, err, "unable to setup manager")
		os.Exit(1)
	}

	operatorImage := os.Getenv("OPERATOR_IMAGE")
	if operatorImage == "" {
		log.Info(ctx, "OPERATOR_IMAGE not set; backup, restore, and scheduled backup features will be unavailable")
	}

	if err = (&controller.EtcdClusterReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		OperatorImage: operatorImage,
	}).SetupWithManager(mgr); err != nil {
		log.Error(ctx, err, "unable to create controller", "controller", "EtcdCluster")
		os.Exit(1)
	}
	if err = (&controller.EtcdBackupReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		OperatorImage: operatorImage,
	}).SetupWithManager(mgr); err != nil {
		log.Error(ctx, err, "unable to create controller", "controller", "EtcdBackup")
		os.Exit(1)
	}
	if err = (&controller.EtcdBackupScheduleReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		OperatorImage: operatorImage,
	}).SetupWithManager(mgr); err != nil {
		log.Error(ctx, err, "unable to create controller", "controller", "EtcdBackupSchedule")
		os.Exit(1)
	}
	if !flags.DisableWebhooks {
		if err = (&etcdaenixiov1alpha1.EtcdCluster{}).SetupWebhookWithManager(mgr); err != nil {
			log.Error(ctx, err, "unable to create webhook", "webhook", "EtcdCluster")
			os.Exit(1)
		}
		if err = (&etcdaenixiov1alpha1.EtcdBackup{}).SetupWebhookWithManager(mgr); err != nil {
			log.Error(ctx, err, "unable to create webhook", "webhook", "EtcdBackup")
			os.Exit(1)
		}
		if err = (&etcdaenixiov1alpha1.EtcdBackupSchedule{}).SetupWebhookWithManager(mgr); err != nil {
			log.Error(ctx, err, "unable to create webhook", "webhook", "EtcdBackupSchedule")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if err = mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(ctx, err, "unable to set up health check")
		os.Exit(1)
	}
	if err = mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(ctx, err, "unable to set up ready check")
		os.Exit(1)
	}

	log.Info(ctx, "starting manager")
	if err = mgr.Start(ctx); err != nil {
		log.Error(ctx, err, "problem running manager")
		os.Exit(1)
	}
}
