package factory

const etcdOperatorName = "etcd-operator"

type LabelsBuilder map[string]string

func NewLabelsBuilder() LabelsBuilder {
	return make(map[string]string)
}

func (b LabelsBuilder) WithName() LabelsBuilder {
	b["app.kubernetes.io/name"] = "etcd"
	return b
}

func (b LabelsBuilder) WithInstance(name string) LabelsBuilder {
	b["app.kubernetes.io/instance"] = name
	return b
}

func (b LabelsBuilder) WithManagedBy() LabelsBuilder {
	b["app.kubernetes.io/managed-by"] = etcdOperatorName
	return b
}
