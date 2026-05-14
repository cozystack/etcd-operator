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

			By("check etcd cluster is healthy", func() {
				Expect(utils.IsEtcdClusterHealthy(ctx, client)).To(BeTrue())
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
		// Tear down everything this spec creates regardless of
		// pass/fail so a re-run does not see stale EtcdBackup /
		// EtcdCluster / PVC objects from a previous failed iteration.
		// kubectl delete namespace cascades to all child objects.
		// `--ignore-not-found` keeps the cleanup idempotent if the
		// spec aborts before resources land. `--wait=false` decouples
		// the next spec's start from this one's GC.
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

			By("check etcd cluster is healthy", func() {
				Expect(utils.IsEtcdClusterHealthy(ctx, client)).To(BeTrue())
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
