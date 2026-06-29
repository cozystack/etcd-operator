/*
Copyright 2023 Timofey Larkin.

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
	"flag"
	"fmt"
	"os"
	"strings"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	etcdv1alpha2 "github.com/cozystack/etcd-operator/api/v1alpha2"
	"github.com/cozystack/etcd-operator/controllers"
	"github.com/cozystack/etcd-operator/internal/agent"
	//+kubebuilder:scaffold:imports
)

const defaultClusterDomain = "cluster.local"

// placeholderOperatorImage is the un-set sentinel image ref. The Helm chart
// always renders a real repository:tag (and keeps image == OPERATOR_IMAGE), so
// this only trips when the operator is run with OPERATOR_IMAGE explicitly left
// at the placeholder — running on it means snapshot/restore Pods would
// ImagePullBackOff forever.
const placeholderOperatorImage = "controller:latest"

// operatorImageError rejects the un-substituted image placeholder so the
// operator fails fast at startup rather than silently at the first snapshot.
func operatorImageError(img string) error {
	if img == placeholderOperatorImage {
		return fmt.Errorf("operator image is the un-substituted placeholder %q; set OPERATOR_IMAGE / --operator-image to the real operator image (deploy with `make deploy IMG=...`, or `helm install --set image.repository=<repo> --set image.tag=<tag>`), otherwise snapshot/restore Pods will ImagePullBackOff", img)
	}
	return nil
}

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(etcdv1alpha2.AddToScheme(scheme))
	//+kubebuilder:scaffold:scheme
}

func main() {
	// Subcommand dispatch: the operator image doubles as the snapshot/restore
	// agent, invoked as `manager snapshot-agent` / `manager restore-agent` in a
	// Job (snapshot) or an initContainer (restore). These run and exit before
	// any manager setup.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "snapshot-agent":
			// Bound the run so a hung dial/snapshot against a parked or
			// unreachable cluster fails with a clear error instead of blocking
			// forever (the snapshot Job's ActiveDeadlineSeconds is the outer
			// backstop).
			ctx, cancel := context.WithTimeout(context.Background(), agent.SnapshotTimeout)
			defer cancel()
			if err := agent.RunSnapshot(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "snapshot agent failed:", err)
				os.Exit(1)
			}
			return
		case "restore-agent":
			// Bound the run so a slow/black-holed S3 endpoint fails with a clear
			// error instead of hanging the init container (and cluster bootstrap)
			// forever — there is no Job deadline here, so this is the only guard.
			ctx, cancel := context.WithTimeout(context.Background(), agent.RestoreTimeout)
			defer cancel()
			if err := agent.RunRestore(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "restore agent failed:", err)
				os.Exit(1)
			}
			return
		}
	}

	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string
	var clusterDomain string
	var operatorImage string
	var etcdImageRepository string
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.StringVar(&clusterDomain, "cluster-domain", "",
		"DNS suffix used by the Kubernetes cluster (e.g. 'cluster.local', 'cozy.local'). "+
			"Threaded into cert-manager-emitted Certificate SANs so the FQDN form "+
			"matches what kube-dns returns for peer reverse-DNS verification. "+
			"When unset, auto-discovered from /etc/resolv.conf's search list (the "+
			"path kubelet uses to inject cluster DNS into normal pods); falls back "+
			"to 'cluster.local' if auto-discovery yields nothing — set explicitly "+
			"for hostNetwork or dnsPolicy:None pods, or any other environment "+
			"where the operator's pod doesn't see kube-dns search paths.")
	flag.StringVar(&operatorImage, "operator-image", os.Getenv("OPERATOR_IMAGE"),
		"Operator image reference. The snapshot/restore agents run from this same "+
			"image (Job / initContainer). Defaults to $OPERATOR_IMAGE; required for "+
			"EtcdSnapshot and spec.bootstrap.restore to function.")
	flag.StringVar(&etcdImageRepository, "etcd-image-repository", os.Getenv("ETCD_IMAGE_REPOSITORY"),
		"Operator-wide default etcd image repository (registry host + path, no tag), "+
			"e.g. 'registry.internal/mirror/etcd'. Used for every member Pod — point "+
			"air-gapped deployments at a mirror once; the tag is always v<spec.version>. "+
			"Defaults to $ETCD_IMAGE_REPOSITORY; when empty the built-in "+
			"quay.io/coreos/etcd is used.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Refuse to run with the un-substituted image placeholder: snapshot/restore
	// Pods would ImagePullBackOff forever otherwise. Fail loudly at startup
	// instead of silently at first snapshot. (Empty is allowed — snapshots are then
	// simply unavailable and the controllers fail loudly if one is attempted.)
	if err := operatorImageError(operatorImage); err != nil {
		setupLog.Error(err, "invalid operator image configuration")
		os.Exit(1)
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		WebhookServer:          webhook.NewServer(webhook.Options{Port: 9443}),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "aa20b3a9.etcd-operator.cozystack.io",
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
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if clusterDomain == "" {
		clusterDomain = discoverClusterDomain("/etc/resolv.conf")
	}
	if clusterDomain == "" {
		clusterDomain = defaultClusterDomain
		setupLog.Info("cluster domain not provided and could not be auto-discovered from /etc/resolv.conf; using default — set --cluster-domain explicitly if your cluster uses a different suffix", "clusterDomain", clusterDomain)
	} else {
		setupLog.Info("cluster domain resolved", "clusterDomain", clusterDomain)
	}

	certManagerAvailable, err := detectCertManager(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to probe discovery API for cert-manager")
		os.Exit(1)
	}
	if certManagerAvailable {
		setupLog.Info("cert-manager.io/v1 API detected; spec.tls.{client,peer}.certManager will be honored")
	} else {
		setupLog.Info("cert-manager.io/v1 API not detected; Reconcile will short-circuit clusters using spec.tls.{client,peer}.certManager with Available=False/CertManagerNotInstalled")
	}

	if err = (&controllers.EtcdClusterReconciler{
		Client:               mgr.GetClient(),
		Scheme:               mgr.GetScheme(),
		CertManagerAvailable: certManagerAvailable,
		ClusterDomain:        clusterDomain,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdCluster")
		os.Exit(1)
	}
	if err = (&controllers.EtcdMemberReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		OperatorImage:       operatorImage,
		EtcdImageRepository: etcdImageRepository,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdMember")
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		setupLog.Error(err, "unable to build clientset")
		os.Exit(1)
	}
	if err = (&controllers.EtcdSnapshotReconciler{
		Client:        mgr.GetClient(),
		Scheme:        mgr.GetScheme(),
		APIReader:     mgr.GetAPIReader(),
		Clientset:     clientset,
		OperatorImage: operatorImage,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "EtcdSnapshot")
		os.Exit(1)
	}
	//+kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// discoverClusterDomain reads the resolv.conf at the given path and
// extracts the cluster DNS suffix from its search list. In a normal
// k8s pod kubelet writes a search line of the form
// "<ns>.svc.<cluster-domain> svc.<cluster-domain> <cluster-domain>";
// the first entry starting with "svc." gives us the suffix.
//
// Returns "" when:
//   - the file can't be read (e.g. running outside a pod),
//   - no search line exists (uncommon outside cluster contexts),
//   - no search entry starts with "svc." (hostNetwork pods see the
//     host's resolv.conf, which doesn't carry the cluster suffix).
//
// Callers fall back to defaultClusterDomain or to the --cluster-domain
// flag in those cases.
func discoverClusterDomain(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		// strip trailing comments + whitespace
		if i := strings.Index(line, "#"); i >= 0 {
			line = line[:i]
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "search" {
			continue
		}
		for _, entry := range fields[1:] {
			if strings.HasPrefix(entry, "svc.") && len(entry) > len("svc.") {
				return strings.TrimPrefix(entry, "svc.")
			}
		}
	}
	return ""
}

// detectCertManager probes the apiserver's discovery API for the
// cert-manager.io/v1 Certificate kind. We detect once at startup rather
// than lazily on first use because:
//
//   - Lazily-detected absence trips controller-runtime's cached client into
//     a permanent reflector retry loop (informer LIST keeps returning
//     NoKindMatch). One-shot detection lets us skip the cache entirely
//     when the CRD is missing, and use the cache normally when it isn't.
//   - The signal is environmental: cert-manager is either installed or
//     it isn't. Installing it after the operator starts requires an
//     operator restart for the flag to flip — acceptable for v1, the
//     standard deploy story has cert-manager as a Helm dep / prereq.
func detectCertManager(cfg *rest.Config) (bool, error) {
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return false, err
	}
	resources, err := dc.ServerResourcesForGroupVersion("cert-manager.io/v1")
	if err != nil {
		// IsNotFound — group/version isn't registered. That's the "no
		// cert-manager" case, not an error.
		if apierrors.IsNotFound(err) || discovery.IsGroupDiscoveryFailedError(err) {
			return false, nil
		}
		return false, err
	}
	for _, r := range resources.APIResources {
		if r.Kind == "Certificate" {
			return true, nil
		}
	}
	return false, nil
}
