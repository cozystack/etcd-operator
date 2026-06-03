package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth" // Import all auth providers
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
	"k8s.io/client-go/util/homedir"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "kubectl-etcd",
		Short: "Kubectl etcd plugin",
		Long:  `Manage etcd pods spawned by etcd-operator`,
		// Subcommands report failures by returning an error (RunE); a runtime
		// failure is not a usage error, so don't dump the help text for it.
		SilenceUsage: true,
	}

	// Initialize configuration
	config := initializeConfig(rootCmd)

	// Register subcommands
	rootCmd.AddCommand(
		createStatusCmd(config),
		createDefragCmd(config),
		createCompactCmd(config),
		createAlarmCmd(config),
		createForfeitLeadershipCmd(config),
		createLeaveCmd(config),
		createMembersCmd(config),
		createRemoveMemberCmd(config),
		createAddMemberCmd(config),
		createSnapshotCmd(config),
	)

	// Execute the root command. cobra prints the returned error; we only need to
	// translate it into a non-zero exit code so scripts/CI can gate on it.
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func createStatusCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Get the status of etcd cluster member",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			status, err := etcdClient.Status(ctx, etcdClient.Endpoints()[0])
			if err != nil {
				return fmt.Errorf("failed to get etcd status: %w", err)
			}

			fmt.Println(statusHeader())
			fmt.Println(statusRow(status))
			return nil
		},
	}
}

// statusHeader is the column header for `status` output.
func statusHeader() string {
	return fmt.Sprintf("%-17s %-9s %-15s %-18s %-11s %-20s %-8s %-s",
		"MEMBER", "DB SIZE", "IN USE", "LEADER", "RAFT INDEX", "RAFT APPLIED INDEX", "LEARNER", "ERRORS")
}

// statusRow formats a single member's status. It populates every advertised
// column, including ERRORS (the member's reported errors, if any).
func statusRow(status *clientv3.StatusResponse) string {
	inUse := fmt.Sprintf("%s (%s)", humanize.Bytes(uint64(status.DbSizeInUse)), inUsePercent(status.DbSize, status.DbSizeInUse))
	return fmt.Sprintf("%-17x %-9s %-15s %-18x %-11d %-20d %-8v %-s",
		status.Header.MemberId, humanize.Bytes(uint64(status.DbSize)),
		inUse, status.Leader, status.RaftIndex, status.RaftAppliedIndex, status.IsLearner,
		strings.Join(status.Errors, ", "))
}

// inUsePercent renders the in-use fraction of the DB. A freshly initialized
// member reports DbSize 0; guard it so the output is "0.00%" rather than the
// "NaN%" a 0/0 float division would produce.
func inUsePercent(dbSize, dbSizeInUse int64) string {
	if dbSize == 0 {
		return "0.00%"
	}
	return fmt.Sprintf("%.2f%%", float64(dbSizeInUse)/float64(dbSize)*100)
}

func createDefragCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "defrag",
		Short: "Defragment etcd database on the node",
		Long: `Defragmentation is a maintenance operation that compacts the historical
records and optimizes the database storage.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			if _, err = etcdClient.Defragment(ctx, etcdClient.Endpoints()[0]); err != nil {
				return fmt.Errorf("failed to defragment etcd database: %w", err)
			}
			return nil
		},
	}
}

func createCompactCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "compact",
		Short: "Compact the etcd database",
		Long: `Compacts the etcd database up to the latest revision to free up space.
This removes old versions of keys and their associated data.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Fetch the latest revision
			statusResp, err := etcdClient.Status(ctx, etcdClient.Endpoints()[0])
			if err != nil {
				return fmt.Errorf("failed to get etcd status: %w", err)
			}

			// Compact the etcd database up to the latest revision
			if _, err = etcdClient.Compact(ctx, statusResp.Header.Revision); err != nil {
				return fmt.Errorf("failed to compact etcd database: %w", err)
			}
			return nil
		},
	}
}

func createAlarmCmd(config *Config) *cobra.Command {
	alarmCmd := &cobra.Command{
		Use:   "alarm",
		Short: "Manage etcd alarms",
		Long:  `Manage the alarms of an etcd cluster.`,
	}

	alarmCmd.AddCommand(
		createAlarmsListCmd(config),
		createAlarmsDisarmCmd(config),
	)

	return alarmCmd
}

func createAlarmsListCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List the etcd alarms for the node",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Call to etcd client to list alarms
			resp, err := etcdClient.AlarmList(ctx)
			if err != nil {
				return fmt.Errorf("failed to list etcd alarms: %w", err)
			}

			for _, alarm := range resp.Alarms {
				fmt.Printf("Alarm: %v, MemberID: %x\n", alarm.Alarm, alarm.MemberID)
			}
			return nil
		},
	}
}

func createAlarmsDisarmCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "disarm",
		Short: "Disarm the etcd alarms for the node",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Call to etcd client to disarm alarms
			if _, err = etcdClient.AlarmDisarm(ctx, &clientv3.AlarmMember{}); err != nil {
				return fmt.Errorf("failed to disarm etcd alarms: %w", err)
			}
			return nil
		},
	}
}

// setupEtcdClient sets up the port forwarding and creates an etcd client.
func setupEtcdClient(config *Config) (*clientv3.Client, error) {
	if config.PodName == "" {
		return nil, fmt.Errorf("you must specify the pod name")
	}

	clientConfig, err := clientcmd.BuildConfigFromFlags("", config.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("error building kubeconfig: %s", err)
	}

	// Resolve the namespace lazily (here, not at flag-binding time) so that
	// --help and other no-cluster invocations don't touch the kubeconfig.
	if config.Namespace == "" {
		config.Namespace = resolveNamespace(config.Kubeconfig)
	}

	clientset, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		return nil, fmt.Errorf("error creating Kubernetes client: %s", err)
	}

	tlsConfig, localPort, err := setupPortForwarding(config, clientset)
	if err != nil {
		return nil, fmt.Errorf("failed to setup port forwarding: %s", err)
	}

	etcdConfig := clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("localhost:%d", localPort)},
		DialTimeout: 5 * time.Second,
	}
	if tlsConfig != nil {
		etcdConfig.TLS = tlsConfig
	}

	// Single-user (root) auth: when --credentials-secret is given, read the
	// username/password from a kubernetes.io/basic-auth Secret (the same shape
	// the operator consumes via spec.auth.rootCredentialsSecretRef) and dial
	// authenticated. etcd refuses password auth over a plaintext wire, so this
	// is only meaningful alongside a TLS-enabled cluster.
	if config.CredentialsSecret != "" {
		user, pass, err := loadCredentials(clientset, config.Namespace, config.CredentialsSecret)
		if err != nil {
			return nil, fmt.Errorf("failed to load credentials: %s", err)
		}
		etcdConfig.Username = user
		etcdConfig.Password = pass
	}

	etcdClient, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd server: %s", err)
	}

	return etcdClient, nil
}

// loadCredentials reads etcd auth credentials from a kubernetes.io/basic-auth
// Secret. secretRef is either "name" (resolved in defaultNamespace) or
// "namespace/name". The username defaults to "root" when the Secret omits it,
// since etcd requires a user named root to enable auth.
func loadCredentials(clientset kubernetes.Interface, defaultNamespace, secretRef string) (string, string, error) {
	namespace, name := defaultNamespace, secretRef
	if parts := strings.SplitN(secretRef, "/", 2); len(parts) == 2 {
		namespace, name = parts[0], parts[1]
	}

	secret, err := clientset.CoreV1().Secrets(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return "", "", err
	}

	password, ok := secret.Data[corev1.BasicAuthPasswordKey]
	if !ok || len(password) == 0 {
		return "", "", fmt.Errorf("secret %s/%s is missing a non-empty %q key", namespace, name, corev1.BasicAuthPasswordKey)
	}

	username := string(secret.Data[corev1.BasicAuthUsernameKey])
	if username == "" {
		username = "root"
	}

	return username, string(password), nil
}

func createForfeitLeadershipCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "forfeit-leadership",
		Short: "Tell node to forfeit etcd cluster leadership",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Retrieve the current status to find the leader
			status, err := etcdClient.Status(ctx, etcdClient.Endpoints()[0])
			if err != nil {
				return fmt.Errorf("failed to get current etcd status: %w", err)
			}

			// Retrieve member list to find a member to transfer leadership to
			members, err := etcdClient.MemberList(ctx)
			if err != nil {
				return fmt.Errorf("failed to get etcd member list: %w", err)
			}

			for _, member := range members.Members {
				if member.ID != status.Leader {
					if _, err = etcdClient.MoveLeader(ctx, member.ID); err != nil {
						return fmt.Errorf("failed to forfeit leadership: %w", err)
					}
					return nil
				}
			}
			fmt.Println("No eligible member found to transfer leadership to or already not the leader.")
			return nil
		},
	}
}

func createLeaveCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "leave",
		Short: "Tell node to leave etcd cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// This operation might require administrative privileges on the etcd cluster.
			memberListResp, err := etcdClient.MemberList(ctx)
			if err != nil {
				return fmt.Errorf("failed to retrieve member list: %w", err)
			}

			for _, member := range memberListResp.Members {
				if member.Name == config.PodName { // Assuming PodName is set as the member name
					if _, err = etcdClient.MemberRemove(ctx, member.ID); err != nil {
						return fmt.Errorf("failed to remove member from cluster: %w", err)
					}
					return nil
				}
			}

			fmt.Println("Specified pod is not a member of the etcd cluster.")
			return nil
		},
	}
}

func createMembersCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "members",
		Short: "Get the list of etcd cluster members",
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			membersResp, err := etcdClient.MemberList(ctx)
			if err != nil {
				return fmt.Errorf("failed to list etcd members: %w", err)
			}

			// Header for the table
			fmt.Printf("%-19s %-10s %-30s %-30s %-7s\n", "ID", "HOSTNAME", "PEER URLS", "CLIENT URLS", "LEARNER")
			for _, member := range membersResp.Members {
				fmt.Printf("%-19x %-10s %-30s %-30s %-7v\n",
					member.ID, member.Name, strings.Join(member.PeerURLs, ","), strings.Join(member.ClientURLs, ","), member.IsLearner)
			}
			return nil
		},
	}
}

func createRemoveMemberCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "remove-member <member ID>",
		Short: "Remove a node from the etcd cluster",
		Long:  `Remove a member from the etcd cluster using its member ID.`,
		Args:  cobra.ExactArgs(1), // Ensures exactly one argument is passed
		RunE: func(cmd *cobra.Command, args []string) error {
			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Parse the member ID from the command line argument
			memberID, err := strconv.ParseUint(args[0], 16, 64)
			if err != nil {
				return fmt.Errorf("invalid member ID format: %w", err)
			}

			// Remove the member using the provided member ID
			if _, err = etcdClient.MemberRemove(ctx, memberID); err != nil {
				return fmt.Errorf("failed to remove member: %w", err)
			}
			return nil
		},
	}
}

func createAddMemberCmd(config *Config) *cobra.Command {
	return &cobra.Command{
		Use:   "add-member [urls]",
		Short: "Add a new member to the etcd cluster",
		Long:  `Add a new member to the etcd cluster using specified peer URLs.`,
		Args:  cobra.ExactArgs(1), // Requires exactly one argument: the new member URL
		RunE: func(cmd *cobra.Command, args []string) error {
			return addMember(config, args[0])
		},
	}
}

func addMember(config *Config, memberURL string) error {
	etcdClient, err := setupEtcdClient(config)
	if err != nil {
		return fmt.Errorf("failed to set up etcd client: %w", err)
	}
	//nolint:errcheck
	defer etcdClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	urls := []string{memberURL}
	if _, err = etcdClient.MemberAdd(ctx, urls); err != nil {
		return fmt.Errorf("failed to add member: %w", err)
	}

	fmt.Println("Member successfully added")
	return nil
}

func createSnapshotCmd(config *Config) *cobra.Command {
	var snapshotCmd = &cobra.Command{
		Use:   "snapshot <path>",
		Short: "Stream snapshot of the etcd node to the path.",
		Long: `Take a snapshot of the etcd database and save it to a specified file path.
This operation is typically used for backup purposes.`,
		Args: cobra.ExactArgs(1), // This command requires exactly one argument for the file path
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0] // The file path where the snapshot will be saved

			etcdClient, err := setupEtcdClient(config)
			if err != nil {
				return err
			}
			//nolint:errcheck
			defer etcdClient.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute) // Snapshot can take time
			defer cancel()

			// Requesting a snapshot from the etcd server
			r, err := etcdClient.Snapshot(ctx)
			if err != nil {
				return fmt.Errorf("failed to create snapshot: %w", err)
			}
			//nolint:errcheck
			defer r.Close() // Make sure to close the snapshot reader

			// Open the file for writing the snapshot
			f, err := os.Create(path)
			if err != nil {
				return fmt.Errorf("failed to open file %s for writing: %w", path, err)
			}
			//nolint:errcheck
			defer f.Close() // Ensure file is closed after writing

			// Copy the snapshot stream to the file
			if _, err = io.Copy(f, r); err != nil {
				return fmt.Errorf("failed to write snapshot to file: %w", err)
			}
			return nil
		},
	}

	// Optional flags can be added here

	return snapshotCmd
}

// forwardReadyTimeout bounds how long setupPortForwarding waits for the
// port-forward to signal ready before giving up.
const forwardReadyTimeout = 10 * time.Second

// awaitForward blocks until the port-forward signals ready, fails, or times
// out — whichever comes first. It exists so a forward that dies before
// becoming ready (which leaves readyChan unclosed) surfaces as an error
// instead of hanging the CLI. On timeout it closes stopChan to tear the
// forwarder down.
func awaitForward(readyChan <-chan struct{}, forwardErr <-chan error, stopChan chan struct{}, timeout time.Duration) error {
	select {
	case <-readyChan:
		return nil
	case err := <-forwardErr:
		if err == nil {
			err = fmt.Errorf("exited before becoming ready")
		}
		return fmt.Errorf("port forwarding failed: %w", err)
	case <-time.After(timeout):
		close(stopChan)
		return fmt.Errorf("timed out after %s waiting for port forwarding to become ready", timeout)
	}
}

func setupPortForwarding(config *Config, clientset *kubernetes.Clientset) (*tls.Config, uint16, error) {
	pod, err := clientset.CoreV1().Pods(config.Namespace).Get(context.Background(), config.PodName, metav1.GetOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get pod: %w", err)
	}

	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/portforward", config.Namespace, config.PodName)
	clientConfig, err := clientcmd.BuildConfigFromFlags("", config.Kubeconfig)
	if err != nil {
		return nil, 0, fmt.Errorf("error building kubeconfig: %w", err)
	}

	transport, upgrader, err := spdy.RoundTripperFor(clientConfig)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create round tripper: %w", err)
	}

	hostURL, err := url.Parse(clientConfig.Host)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to parse host URL: %w", err)
	}

	hostURL.Path = path

	stopChan, readyChan := make(chan struct{}, 1), make(chan struct{}, 1)
	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", hostURL)

	silentOut := &silentWriter{}
	portForwarder, err := portforward.New(dialer, []string{"0:2379"}, stopChan, readyChan, silentOut, os.Stderr)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create port forwarder: %w", err)
	}

	// ForwardPorts blocks until the forward is torn down; run it in the
	// background and surface a startup failure via forwardErr. On a dial
	// failure (RBAC on pods/portforward, API-server connectivity, protocol
	// negotiation) ForwardPorts returns WITHOUT ever closing readyChan, so
	// blocking on readyChan alone would hang the CLI forever — awaitForward
	// selects on the error and a timeout too.
	forwardErr := make(chan error, 1)
	go func() {
		forwardErr <- portForwarder.ForwardPorts()
	}()

	if err := awaitForward(readyChan, forwardErr, stopChan, forwardReadyTimeout); err != nil {
		return nil, 0, err
	}

	// Obtaining the local port used for forwarding
	forwardedPorts, err := portForwarder.GetPorts()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get forwarded ports: %w", err)
	}

	localPort := forwardedPorts[0].Local

	tlsConfig, err := getTLSConfig(clientset, pod, config.Namespace)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get TLS config: %w", err)
	}

	return tlsConfig, localPort, nil
}

// initializeConfig binds the persistent flags onto a Config. Flag values are
// populated by cobra during rootCmd.Execute(); the namespace default is
// resolved lazily in setupEtcdClient so --help works without a kubeconfig.
func initializeConfig(cmd *cobra.Command) *Config {
	config := &Config{}

	// Default kubeconfig: $KUBECONFIG, else ~/.kube/config.
	defaultKubeconfig := os.Getenv("KUBECONFIG")
	if defaultKubeconfig == "" {
		defaultKubeconfig = filepath.Join(homedir.HomeDir(), ".kube", "config")
	}

	cmd.PersistentFlags().StringVarP(&config.Kubeconfig, "kubeconfig", "k", defaultKubeconfig, "Path to the kubeconfig file")
	cmd.PersistentFlags().StringVarP(&config.Namespace, "namespace", "n", "",
		"Namespace of the etcd pod (default is the current namespace from kubeconfig)")
	cmd.PersistentFlags().StringVarP(&config.PodName, "pod", "p", "", "Name of the etcd pod")
	cmd.PersistentFlags().StringVarP(&config.CredentialsSecret, "credentials-secret", "s", "",
		"Name (or namespace/name) of a kubernetes.io/basic-auth Secret holding the etcd "+
			"username/password. Set this for clusters with spec.auth.enabled.")

	return config
}

// resolveNamespace returns the current-context namespace from the kubeconfig,
// honoring an explicit --kubeconfig path, and falls back to "default".
func resolveNamespace(kubeconfig string) string {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{})
	ns, _, err := loader.Namespace()
	if err != nil || ns == "" {
		return "default"
	}
	return ns
}

// Config struct to hold configuration
type Config struct {
	Kubeconfig        string
	Namespace         string
	PodName           string
	CredentialsSecret string
}

// errNoTrustedCAFile signals that the etcd container has no --trusted-ca-file
// argument, i.e. the cluster is plaintext and no TLS config is needed. A
// sentinel (matched via errors.Is) avoids string-comparing the error message.
var errNoTrustedCAFile = errors.New("trusted CA file path not specified in container args")

func getTLSConfig(clientset kubernetes.Interface, pod *corev1.Pod, namespace string) (*tls.Config, error) {
	for _, container := range pod.Spec.Containers {
		if container.Name == "etcd" {
			secretName, err := findSecretNameForTLS(pod, container)
			if err != nil {
				if errors.Is(err, errNoTrustedCAFile) {
					return nil, nil // plaintext cluster — dial without TLS
				}
				return nil, err
			}

			caCertPool, clientCert, err := extractTLSFiles(clientset, namespace, secretName)
			if err != nil {
				return nil, err
			}

			return &tls.Config{
				Certificates: []tls.Certificate{*clientCert},
				RootCAs:      caCertPool,
				MinVersion:   tls.VersionTLS12,
			}, nil
		}
	}
	return nil, fmt.Errorf("etcd container not found")
}

func findSecretNameForTLS(pod *corev1.Pod, container corev1.Container) (string, error) {
	caFilePath := ""
	for _, arg := range append(container.Command, container.Args...) {
		if strings.HasPrefix(arg, "--trusted-ca-file=") {
			caFilePath = strings.TrimPrefix(arg, "--trusted-ca-file=")
			break
		}
	}

	if caFilePath == "" {
		return "", errNoTrustedCAFile
	}

	for _, vm := range container.VolumeMounts {
		if strings.HasPrefix(caFilePath, vm.MountPath) {
			// We found the mount path, now find the volume
			for _, vol := range pod.Spec.Volumes {
				if vol.Name == vm.Name && vol.Secret != nil {
					return vol.Secret.SecretName, nil
				}
			}
		}
	}

	return "", fmt.Errorf("secret for the trusted CA file not found")
}

func extractTLSFiles(clientset kubernetes.Interface, namespace, secretName string) (
	*x509.CertPool, *tls.Certificate, error) {
	secret, err := clientset.CoreV1().Secrets(namespace).Get(context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}

	caPem, ok := secret.Data["ca.crt"]
	if !ok {
		return nil, nil, fmt.Errorf("CA certificate not found in secret")
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caPem) {
		return nil, nil, fmt.Errorf("failed to parse CA certificate")
	}

	certPem, ok := secret.Data["tls.crt"]
	if !ok {
		return nil, nil, fmt.Errorf("TLS certificate not found in secret")
	}
	keyPem, ok := secret.Data["tls.key"]
	if !ok {
		return nil, nil, fmt.Errorf("TLS key not found in secret")
	}

	clientCert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create X509 key pair: %s", err)
	}

	return caCertPool, &clientCert, nil
}

type silentWriter struct{}

func (sw *silentWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
