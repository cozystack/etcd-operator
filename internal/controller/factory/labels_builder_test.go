package factory

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Labels builder", func() {
	Context("When ensuring a labels builder", func() {
		It("constructor returns empty initialized map", func() {
			builder := NewLabelsBuilder()
			Expect(builder).To(Equal(make(LabelsBuilder)))
		})
		It("WithName sets correct key and value", func() {
			builder := NewLabelsBuilder()
			builder.WithName()
			Expect(builder["app.kubernetes.io/name"]).To(Equal("etcd"))
		})
		It("WithManagedBy sets correct key and value", func() {
			builder := NewLabelsBuilder()
			builder.WithManagedBy()
			Expect(builder["app.kubernetes.io/managed-by"]).To(Equal(etcdOperatorName))
		})
		It("WithInstance sets correct key and value", func() {
			builder := NewLabelsBuilder()
			builder.WithInstance("local")
			Expect(builder["app.kubernetes.io/instance"]).To(Equal("local"))
		})
		It("Chaining methods builds correct map", func() {
			builder := NewLabelsBuilder()
			builder.WithName().WithManagedBy().WithInstance("local")
			expected := map[string]string{
				"app.kubernetes.io/name":       "etcd",
				"app.kubernetes.io/instance":   "local",
				"app.kubernetes.io/managed-by": etcdOperatorName,
			}
			Expect(builder).To(Equal(LabelsBuilder(expected)))
		})
	})
})
