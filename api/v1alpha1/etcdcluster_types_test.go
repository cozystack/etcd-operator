package v1alpha1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("CalculateQuorumSize", func() {
	It("should return correct result for odd number of replicas", func() {
		etcdCluster := EtcdCluster{
			Spec: EtcdClusterSpec{Replicas: new(int32(3))},
		}
		Expect(etcdCluster.CalculateQuorumSize()).To(Equal(2))
	})
	It("should return correct result for even number of replicas", func() {
		etcdCluster := EtcdCluster{
			Spec: EtcdClusterSpec{Replicas: new(int32(4))},
		}
		Expect(etcdCluster.CalculateQuorumSize()).To(Equal(3))
	})
})

var _ = Describe("Aux Functions", func() {

	Context("When running IsClientSecurityEnabled function", func() {
		It("should return true if ClientSecret is set", func() {
			cluster := EtcdCluster{
				Spec: EtcdClusterSpec{
					Security: &SecuritySpec{
						TLS: TLSSpec{
							ClientSecret: "some-client-secret",
						},
					},
				},
			}
			Expect(cluster.IsClientSecurityEnabled()).To(BeTrue())
		})

		It("should return false if ClientSecret is not set", func() {
			cluster := EtcdCluster{
				Spec: EtcdClusterSpec{
					Security: &SecuritySpec{
						TLS: TLSSpec{},
					},
				},
			}
			Expect(cluster.IsClientSecurityEnabled()).To(BeFalse())
		})

		It("should return false if Security is nil", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{},
			}
			Expect(cluster.IsClientSecurityEnabled()).To(BeFalse())
		})
	})

	Context("When running IsServerSecurityEnabled function", func() {
		It("should return true if ServerSecret is set", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Security: &SecuritySpec{
						TLS: TLSSpec{
							ServerSecret: "some-server-secret",
						},
					},
				},
			}
			Expect(cluster.IsServerSecurityEnabled()).To(BeTrue())
		})

		It("should return false if ServerSecret is not set", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Security: &SecuritySpec{
						TLS: TLSSpec{},
					},
				},
			}
			Expect(cluster.IsServerSecurityEnabled()).To(BeFalse())
		})

		It("should return false if Security is nil", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{},
			}
			Expect(cluster.IsServerSecurityEnabled()).To(BeFalse())
		})
	})

	Context("When running IsServerTrustedCADefined function", func() {
		It("should return true if ServerTrustedCASecret is set", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Security: &SecuritySpec{
						TLS: TLSSpec{
							ServerTrustedCASecret: "some-ca-secret",
						},
					},
				},
			}
			Expect(cluster.IsServerTrustedCADefined()).To(BeTrue())
		})

		It("should return false if ServerTrustedCASecret is not set", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{
					Security: &SecuritySpec{
						TLS: TLSSpec{},
					},
				},
			}
			Expect(cluster.IsServerTrustedCADefined()).To(BeFalse())
		})

		It("should return false if Security is nil", func() {
			cluster := &EtcdCluster{
				Spec: EtcdClusterSpec{},
			}
			Expect(cluster.IsServerTrustedCADefined()).To(BeFalse())
		})
	})
})
