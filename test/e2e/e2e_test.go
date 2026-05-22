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

package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	clientv3 "go.etcd.io/etcd/client/v3"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aenix-io/etcd-operator/test/utils"
)

var _ = Describe("etcd-operator", Ordered, func() {

	BeforeAll(func() {
		var err error
		By("prepare kind environment", func() {
			cmd := exec.Command("make", "kind-prepare")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
		})

		By("upload latest etcd-operator docker image to kind cluster", func() {
			cmd := exec.Command("make", "kind-load")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
		})

		By("deploy etcd-operator", func() {
			cmd := exec.Command("make", "deploy")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
		})

		By("wait while etcd-operator is ready", func() {
			cmd := exec.Command("kubectl", "wait", "--namespace",
				"etcd-operator-system", "deployment/etcd-operator-controller-manager",
				"--for", "jsonpath={.status.availableReplicas}=1", "--timeout=5m")
			_, err = utils.Run(cmd)
			ExpectWithOffset(1, err).NotTo(HaveOccurred())
		})
	})

	if os.Getenv("DO_CLEANUP_AFTER_E2E") == "true" {
		AfterAll(func() {
			By("Delete kind environment", func() {
				cmd := exec.Command("make", "kind-delete")
				_, _ = utils.Run(cmd)
			})
		})
	}

	Context("With PVC and resize", func() {
		const namespace = "test-pvc-and-resize-etcd-cluster"
		const storageClass = `
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: standard-with-expansion
provisioner: rancher.io/local-path
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
`

		It("should resize PVCs of the etcd cluster", func() {
			var err error
			By("create namespace", func() {
				cmd := exec.Command("kubectl", "create", "namespace", namespace)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			By("create StorageClass", func() {
				cmd := exec.Command("kubectl", "apply", "-f", "-")
				cmd.Stdin = strings.NewReader(storageClass)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			By("deploying etcd cluster with initial PVC size", func() {
				dir, _ := utils.GetProjectDir()
				cmd := exec.Command("kubectl", "apply",
					"--filename", dir+"/examples/manifests/etcdcluster-persistent.yaml",
					"--namespace", namespace,
				)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			By("waiting for statefulset to be ready", func() {
				Eventually(func() error {
					cmd := exec.Command("kubectl", "wait",
						"statefulset/test",
						"--for", "jsonpath={.status.readyReplicas}=3",
						"--namespace", namespace,
						"--timeout", "5m",
					)
					_, err = utils.Run(cmd)
					return err
				}, 5*time.Minute, 10*time.Second).Should(Succeed())
			})

			By("updating the storage request", func() {
				// Patch the EtcdCluster to increase storage size
				patch := `{"spec": {"storage": {"volumeClaimTemplate": {"spec": {"resources": {"requests": {"storage": "8Gi"}}}}}}}`
				cmd := exec.Command("kubectl", "patch", "etcdcluster", "test", "--namespace", namespace, "--type", "merge", "--patch", patch) //nolint:lll
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			By("checking that PVC sizes have been updated", func() {
				Eventually(func() bool {
					cmd := exec.Command("kubectl", "get", "pvc", "-n", namespace, "-o", "jsonpath={.items[*].spec.resources.requests.storage}") //nolint:lll
					output, err := utils.Run(cmd)
					if err != nil {
						return false
					}
					// Split the output into individual sizes and check each one
					sizes := strings.Fields(string(output))
					for _, size := range sizes {
						if size != "8Gi" {
							return false
						}
					}
					return true
				}, 5*time.Minute, 10*time.Second).Should(BeTrue(), "PVCs should be resized to 8Gi")
			})
		})
	})

	Context("With emptyDir", func() {
		It("should deploy etcd cluster", func() {
			var err error
			const namespace = "test-emptydir-etcd-cluster"
			var wg sync.WaitGroup
			wg.Add(1)

			By("create namespace", func() {
				cmd := exec.Command("sh", "-c",
					fmt.Sprintf("kubectl create namespace %s --dry-run=client -o yaml | kubectl apply -f -", namespace)) // nolint:lll
				_, err = utils.Run(cmd)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			})

			By("apply etcd cluster with emptydir manifest", func() {
				dir, _ := utils.GetProjectDir()
				cmd := exec.Command("kubectl", "apply",
					"--filename", dir+"/examples/manifests/etcdcluster-emptydir.yaml",
					"--namespace", namespace,
				)
				_, err = utils.Run(cmd)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			})

			Eventually(func() error {
				cmd := exec.Command("kubectl", "wait",
					"statefulset/test",
					"--for", "jsonpath={.status.readyReplicas}=3",
					"--namespace", namespace,
					"--timeout", "5m",
				)
				_, err = utils.Run(cmd)
				return err
			}, time.Second*20, time.Second*2).Should(Succeed(), "wait for statefulset is ready")

			client, err := utils.GetEtcdClient(ctx, client.ObjectKey{Namespace: namespace, Name: "test"})
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				err := client.Close()
				Expect(err).NotTo(HaveOccurred())
			}()

			// utils.GetEtcdClient starts `kubectl port-forward` in a
			// goroutine and returns the client immediately, so the
			// local listener may not be up yet when the first gRPC
			// call fires — on slow CI runners the call lands while
			// the kubectl child process is still binding the local
			// port, and we get `connection refused` once before the
			// 2s context inside IsEtcdClusterHealthy times out.
			// Eventually retries past the port-forward warmup; the
			// underlying contract (healthy=true) is unchanged.
			By("check etcd cluster is healthy", func() {
				Eventually(func() (bool, error) {
					return utils.IsEtcdClusterHealthy(ctx, client)
				}, 60*time.Second, 2*time.Second).Should(BeTrue(),
					"etcd cluster never reported healthy via port-forwarded client")
			})
		})
	})

	// Pins the agent → controller wire contract end-to-end. Unit
	// tests stub LogStreamer, so the production code that opens
	// PodLogOptions{Container: "backup-agent"} and Streams via
	// Clientset is otherwise uncovered — if the container name ever
	// drifts between factory/backup_job.go and the controller, all
	// unit tests still pass while every real backup silently leaves
	// status.snapshot empty. This e2e test exercises that path
	// against a running apiserver + kubelet + agent and asserts the
	// resulting status fields.
	Context("With PVC backup", func() {
		const namespace = "test-pvc-backup-etcd-cluster"
		// AfterEach dumps diagnostics on per-spec failure but does NOT
		// delete the namespace: the restore spec deliberately reuses
		// the backup spec's namespace + etcd-backup-pvc + EtcdBackup CR
		// (the restore reads the snapshot from that PVC). Tearing the
		// namespace down between Its sends it into Terminating, and
		// the next spec's `kubectl apply EtcdCluster` is then rejected
		// with `Forbidden: namespace is being terminated`. Namespace
		// teardown is deferred to AfterAll below — re-runs still see a
		// clean slate because AfterAll fires after the last spec in
		// this ordered Context.
		AfterEach(func() {
			// On failure, dump everything we'd otherwise need to
			// kubectl-ssh into the runner to see: the EtcdBackup
			// object itself (status/conditions explain the
			// reconcile decision), the spawned backup Job + pod
			// (phase/events explain whether the agent ran at all),
			// the agent container's pod log (explains whether the
			// terminal marker was emitted), and the operator's
			// own log (explains why an apparently-complete backup
			// finalized with empty status.snapshot — extraction
			// race vs torn log vs genuine no-marker). All output
			// goes through GinkgoWriter so it shows up under the
			// failing spec in CI logs.
			if CurrentSpecReport().Failed() {
				dumpCmds := [][]string{
					{"kubectl", "-n", namespace, "get", "etcdbackup", "-o", "yaml"},
					{"kubectl", "-n", namespace, "get", "job,pod", "-o", "wide"},
					{"kubectl", "-n", namespace, "describe", "job"},
					{"kubectl", "-n", namespace, "describe", "pod"},
					{"kubectl", "-n", namespace, "logs", "-l",
						"etcd.aenix.io/etcdbackup-name=e2e-pvc-backup",
						"--tail=-1", "--all-containers"},
					{"kubectl", "-n", "etcd-operator-system", "logs",
						"deployment/etcd-operator-controller-manager",
						"-c", "manager", "--tail=200"},
				}
				for _, c := range dumpCmds {
					_, _ = fmt.Fprintf(GinkgoWriter, "\n=== %s ===\n", strings.Join(c, " "))
					out, _ := exec.Command(c[0], c[1:]...).CombinedOutput()
					_, _ = GinkgoWriter.Write(out)
				}
			}
		})
		// Final cleanup after the last spec in this Context. `--wait=false`
		// keeps the suite end fast; subsequent runs re-create the namespace
		// idempotently via `kubectl create ... --dry-run | kubectl apply`.
		AfterAll(func() {
			cmd := exec.Command("kubectl", "delete", "namespace", namespace,
				"--ignore-not-found", "--wait=false")
			_, _ = utils.Run(cmd)
		})
		It("should populate status.snapshot.uri/checksum after a successful backup", func() {
			var err error

			By("create namespace", func() {
				cmd := exec.Command("sh", "-c",
					fmt.Sprintf("kubectl create namespace %s --dry-run=client -o yaml | kubectl apply -f -", namespace)) //nolint:lll
				_, err = utils.Run(cmd)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			})

			By("deploy emptyDir etcd cluster", func() {
				dir, _ := utils.GetProjectDir()
				cmd := exec.Command("kubectl", "apply",
					"--filename", dir+"/examples/manifests/etcdcluster-emptydir.yaml",
					"--namespace", namespace,
				)
				_, err = utils.Run(cmd)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			})

			By("wait for etcd statefulset ready", func() {
				Eventually(func() error {
					cmd := exec.Command("kubectl", "wait",
						"statefulset/test",
						"--for", "jsonpath={.status.readyReplicas}=3",
						"--namespace", namespace,
						"--timeout", "5m",
					)
					_, err = utils.Run(cmd)
					return err
				}, 5*time.Minute, 10*time.Second).Should(Succeed())
			})

			// Backup PVC. kind's bundled standard StorageClass
			// (rancher.io/local-path) provisions on first attach.
			const backupPVC = `
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: etcd-backup-pvc
spec:
  accessModes: ["ReadWriteOnce"]
  resources:
    requests:
      storage: 1Gi
  storageClassName: standard
`
			By("create backup PVC", func() {
				cmd := exec.Command("kubectl", "apply", "--namespace", namespace, "-f", "-")
				cmd.Stdin = strings.NewReader(backupPVC)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			const backupCR = `
apiVersion: etcd.aenix.io/v1alpha1
kind: EtcdBackup
metadata:
  name: e2e-pvc-backup
spec:
  clusterRef:
    name: test
  destination:
    pvc:
      claimName: etcd-backup-pvc
`
			By("create EtcdBackup", func() {
				cmd := exec.Command("kubectl", "apply", "--namespace", namespace, "-f", "-")
				cmd.Stdin = strings.NewReader(backupCR)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			By("wait for status.phase=Complete", func() {
				Eventually(func() string {
					cmd := exec.Command("kubectl", "get", "etcdbackup", "e2e-pvc-backup",
						"--namespace", namespace,
						"-o", "jsonpath={.status.phase}",
					)
					out, runErr := utils.Run(cmd)
					if runErr != nil {
						return ""
					}
					return strings.TrimSpace(string(out))
				}, 5*time.Minute, 10*time.Second).Should(Equal("Complete"),
					"EtcdBackup did not reach Complete in time")
			})

			By("status.snapshot.uri must be a file:// URI populated by the agent", func() {
				cmd := exec.Command("kubectl", "get", "etcdbackup", "e2e-pvc-backup",
					"--namespace", namespace,
					"-o", "jsonpath={.status.snapshot.uri}",
				)
				out, runErr := utils.Run(cmd)
				Expect(runErr).NotTo(HaveOccurred())
				uri := strings.TrimSpace(string(out))
				Expect(uri).To(HavePrefix("file:///"),
					"status.snapshot.uri must start with file:/// (got %q)", uri)
			})

			By("status.snapshot.checksum must be a sha256:<hex> populated by the agent", func() {
				cmd := exec.Command("kubectl", "get", "etcdbackup", "e2e-pvc-backup",
					"--namespace", namespace,
					"-o", "jsonpath={.status.snapshot.checksum}",
				)
				out, runErr := utils.Run(cmd)
				Expect(runErr).NotTo(HaveOccurred())
				checksum := strings.TrimSpace(string(out))
				Expect(checksum).To(HavePrefix("sha256:"),
					"status.snapshot.checksum must start with sha256: (got %q)", checksum)
				Expect(len(checksum)).To(BeNumerically(">", len("sha256:")),
					"status.snapshot.checksum must have hex content after the prefix (got %q)", checksum)
			})

			By("status.snapshot.sizeBytes must be non-zero", func() {
				cmd := exec.Command("kubectl", "get", "etcdbackup", "e2e-pvc-backup",
					"--namespace", namespace,
					"-o", "jsonpath={.status.snapshot.sizeBytes}",
				)
				out, runErr := utils.Run(cmd)
				Expect(runErr).NotTo(HaveOccurred())
				Expect(strings.TrimSpace(string(out))).NotTo(Or(BeEmpty(), Equal("0")),
					"status.snapshot.sizeBytes must be set")
			})
		})

		// Continuation of the backup spec above: the EtcdCluster `test`,
		// the backup PVC `etcd-backup-pvc`, and the EtcdBackup CR are
		// still in the namespace. Exercises the full restore path so a
		// regression in the initContainers (image distroless without
		// /bin/sh, missing data volume, mis-named env vars from
		// cluster-state CM) fails here instead of in production.
		It("should restore an etcd cluster from the PVC snapshot", func() {
			var err error

			By("delete the source EtcdCluster", func() {
				cmd := exec.Command("kubectl", "-n", namespace,
					"delete", "etcdcluster", "test",
					"--wait=true", "--timeout=3m")
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			// Gate on source etcd pods only: the source cluster runs on
			// storage.emptyDir, so CreateOrUpdateStatefulSet skips the
			// volumeClaimTemplate and no PVC ever carries the instance
			// label — the pod label is set unconditionally by PodLabels(),
			// so a pod-count gate is meaningful for both emptyDir and
			// PVC-backed source clusters.
			//
			// The `!batch.kubernetes.io/job-name` half is critical: the
			// EtcdBackup controller propagates the same
			// app.kubernetes.io/{instance,name,managed-by}=... labels onto
			// the backup Job's pod template, so the completed backup pod
			// from the previous spec also matches `instance=test`. It
			// stays in the namespace for ttlSecondsAfterFinished (600s),
			// well past this wait's 3-minute budget, and without the
			// negative match the gate never reaches 0. Filtering on the
			// presence of `job-name` excludes any Job-managed pod and
			// keeps the gate scoped to StatefulSet-managed etcd pods.
			//
			// `-o name` is deliberate: utils.Run captures CombinedOutput,
			// and kubectl writes "No resources found in <ns> namespace."
			// to stderr when its human-readable printer renders an empty
			// list (`--no-headers` included). With the warning merged
			// into stdout, a trimmed-length gate never sees zero and
			// the wait times out. `-o name` produces stable empty
			// output for an empty list and writes nothing to stderr,
			// so the trimmed gate works for both 0-pod and N-pod states.
			By("wait for source pods to drain", func() {
				Eventually(func() string {
					cmd := exec.Command("kubectl", "-n", namespace, "get", "pod",
						"-l", "app.kubernetes.io/instance=test,!batch.kubernetes.io/job-name",
						"-o", "name")
					out, _ := utils.Run(cmd)
					return strings.TrimSpace(string(out))
				}, 3*time.Minute, 5*time.Second).Should(BeEmpty())
			})

			// Resolve the actual on-PVC filename from the backup's
			// status.snapshot.uri rather than guessing. backup_job.go
			// always sets BACKUP_INCLUDE_REVISION=true, so the
			// backup-agent rewrites PVC_BACKUP_PATH from
			// "<backupName>.db" to "<backupName>-rev<N>.db" via
			// injectRevision(). The restore-agent renderer in
			// statefulset.go defaults PVC_BACKUP_PATH to
			// "/backup/data/snapshot.db" when subPath is unset, which
			// does not exist on the PVC and crash-loops the init
			// container. Driving subPath from the recorded URI keeps
			// the test independent of how the backup-agent picks names.
			var restoreFilename string
			By("read backup file path from EtcdBackup.status.snapshot.uri", func() {
				cmd := exec.Command("kubectl", "get", "etcdbackup", "e2e-pvc-backup",
					"--namespace", namespace,
					"-o", "jsonpath={.status.snapshot.uri}")
				out, runErr := utils.Run(cmd)
				Expect(runErr).NotTo(HaveOccurred())
				uri := strings.TrimSpace(string(out))
				Expect(uri).To(HavePrefix("file:///backup/data/"),
					"unexpected URI %q (expected file:///backup/data/<name>)", uri)
				restoreFilename = strings.TrimPrefix(uri, "file:///backup/data/")
				Expect(restoreFilename).NotTo(BeEmpty())
				Expect(restoreFilename).NotTo(ContainSubstring("/"),
					"filename must be a single PVC entry, not a path: %q", restoreFilename)
			})

			By("re-create EtcdCluster with bootstrap.restore", func() {
				restoreCR := fmt.Sprintf(`
apiVersion: etcd.aenix.io/v1alpha1
kind: EtcdCluster
metadata:
  name: test
spec:
  replicas: 3
  storage:
    emptyDir: {}
  bootstrap:
    restore:
      source:
        pvc:
          claimName: etcd-backup-pvc
          subPath: %s
`, restoreFilename)
				cmd := exec.Command("kubectl", "apply",
					"--namespace", namespace, "-f", "-")
				cmd.Stdin = strings.NewReader(restoreCR)
				_, err = utils.Run(cmd)
				Expect(err).NotTo(HaveOccurred())
			})

			// Fast-fail gate: if the restore-agent initContainer crashes
			// (e.g. missing /bin/sh on distroless image, missing
			// volume mount, missing env var) we get CrashLoopBackOff
			// on test-0 within seconds. Surface that as an immediate
			// test failure instead of waiting 5 min for StatefulSet
			// readiness to time out.
			By("test-0 initContainer must not crash-loop", func() {
				// Wait until the pod has been scheduled and the
				// kubelet has populated initContainerStatuses;
				// before that the jsonpath returns empty and any
				// "not crash-looped" assertion is vacuous.
				Eventually(func() string {
					cmd := exec.Command("kubectl", "-n", namespace,
						"get", "pod", "test-0",
						"-o", `jsonpath={.status.initContainerStatuses[0].name}`)
					out, _ := utils.Run(cmd)
					return strings.TrimSpace(string(out))
				}, 60*time.Second, 2*time.Second).ShouldNot(BeEmpty(),
					"test-0 initContainerStatuses never populated")

				// Now hold the assertion for 90s. Consistently fails
				// on the FIRST poll that matches, so a crash-loop
				// appearing 30s in still fails the spec — which is
				// what we want; Eventually would have silently passed
				// at t=0 before the pod even started.
				Consistently(func() string {
					cmd := exec.Command("kubectl", "-n", namespace,
						"get", "pod", "test-0",
						"-o", `jsonpath={range .status.initContainerStatuses[*]}{.state.waiting.reason}{","}{end}`)
					out, _ := utils.Run(cmd)
					return string(out)
				}, 90*time.Second, 5*time.Second).ShouldNot(
					Or(ContainSubstring("CrashLoopBackOff"),
						ContainSubstring("RunContainerError")),
					"test-0 init container crash-looped; the restore-agent likely failed")
			})

			By("wait for restored statefulset Ready", func() {
				Eventually(func() error {
					cmd := exec.Command("kubectl", "wait",
						"statefulset/test",
						"--for", "jsonpath={.status.readyReplicas}=3",
						"--namespace", namespace,
						"--timeout", "5m",
					)
					_, err = utils.Run(cmd)
					return err
				}, 5*time.Minute, 10*time.Second).Should(Succeed())
			})
		})
	})

	Context("TLS and enabled auth", func() {
		It("should deploy etcd cluster with auth", func() {
			var err error
			const namespace = "test-tls-auth-etcd-cluster"
			var wg sync.WaitGroup
			wg.Add(1)

			By("create namespace", func() {
				cmd := exec.Command("sh", "-c", fmt.Sprintf("kubectl create namespace %s --dry-run=client -o yaml | kubectl apply -f -", namespace)) //nolint:lll
				_, err = utils.Run(cmd)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			})

			By("apply tls with enabled auth etcd cluster manifest", func() {
				dir, _ := utils.GetProjectDir()
				cmd := exec.Command("kubectl", "apply",
					"--filename", dir+"/examples/manifests/etcdcluster-with-external-certificates.yaml",
					"--namespace", namespace,
				)
				_, err = utils.Run(cmd)
				ExpectWithOffset(1, err).NotTo(HaveOccurred())
			})

			Eventually(func() error {
				cmd := exec.Command("kubectl", "wait",
					"certificate/client-certificate",
					"--for", "condition=Ready",
					"--namespace", namespace,
					"--timeout", "5m",
				)
				_, err = utils.Run(cmd)
				return err
			}, time.Second*20, time.Second*2).Should(Succeed(), "wait for client cert ready")

			Eventually(func() error {
				cmd := exec.Command("kubectl", "wait",
					"statefulset/test",
					"--for", "jsonpath={.status.availableReplicas}=3",
					"--namespace", namespace,
					"--timeout", "5m",
				)
				_, err = utils.Run(cmd)
				return err
			}, time.Second*20, time.Second*2).Should(Succeed(), "wait for statefulset is ready")

			client, err := utils.GetEtcdClient(ctx, client.ObjectKey{Namespace: namespace, Name: "test"})
			Expect(err).NotTo(HaveOccurred())
			defer func() {
				err := client.Close()
				Expect(err).NotTo(HaveOccurred())
			}()

			// Same port-forward warmup race as in the emptyDir spec
			// above — Eventually past the dial gap.
			By("check etcd cluster is healthy", func() {
				Eventually(func() (bool, error) {
					return utils.IsEtcdClusterHealthy(ctx, client)
				}, 60*time.Second, 2*time.Second).Should(BeTrue(),
					"etcd cluster never reported healthy via port-forwarded client")
			})

			auth := clientv3.NewAuth(client)

			By("check root role is created", func() {
				Eventually(func() error {
					_, err = auth.RoleGet(ctx, "root")
					return err
				}, time.Second*20, time.Second*2).Should(Succeed())
			})

			By("check root user is created and has root role", func() {
				userResponce, err := auth.UserGet(ctx, "root")
				Expect(err).NotTo(HaveOccurred())
				Expect(userResponce.Roles).To(ContainElement("root"))
			})

			By("check auth is enabled", func() {
				authStatus, err := auth.AuthStatus(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(authStatus.Enabled).To(BeTrue())
			})
		})
	})
})
