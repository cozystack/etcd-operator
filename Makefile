
# Image URL to use all building/pushing image targets
IMG ?= ghcr.io/aenix-io/etcd-operator:latest
# K8S_VERSION refers to the version of kubebuilder assets to be downloaded by envtest binary.
# renovate: datasource=github-tags depName=kubernetes/kubernetes
K8S_VERSION ?= v1.35.0
K8S_VERSION_TRIMMED_V = $(subst v,,$(K8S_VERSION))

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen yq ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases
	$(YQ) -i '.spec.versions[0].schema.openAPIV3Schema.properties.spec.properties.podTemplate.properties.spec.properties |= {}' config/crd/bases/etcd.aenix.io_etcdclusters.yaml

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: generate-docs
generate-docs: yq crd-ref-docs ## Generate CRD reference documentation.
	@$(eval VERSION := $(shell $(YQ) '.params.version' site/hugo.yaml))
	$(CRD_REF_DOCS) --config=.crd-docs.yaml --renderer=markdown --templates-dir="site/reference-templates" --output-path="site/content/en/docs/$(VERSION)/reference/api.md"

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: mod-tidy
mod-tidy: ## Run go mod tidy against code.
	go mod tidy

.PHONY: test
test: manifests generate fmt vet envtest ## Run tests.
	@echo "Check for kubernetes version $(K8S_VERSION_TRIMMED_V) in $(ENVTEST)"
	@$(ENVTEST) list | grep -q $(K8S_VERSION_TRIMMED_V)
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(K8S_VERSION_TRIMMED_V) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# Utilize Kind or modify the e2e tests to load the image locally, enabling compatibility with other vendors.
.PHONY: test-e2e  # Run the e2e tests against a Kind k8s instance that is spun up.
test-e2e:
	go test ./test/e2e/ -v -ginkgo.v

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter & yamllint
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: nilaway-lint
nilaway-lint: nilaway
	$(NILAWAY_LINT) -include-pkgs=github.com/aenix-io/etcd-operator/api,github.com/aenix-io/etcd-operator/internal,github.com/aenix-io/etcd-operator/test ./...

.PHONY: helm-lint
helm-lint: helm ## Run helm lint over chart
	$(HELM) lint charts/etcd-operator

.PHONY: helm-schema-run
helm-schema-run: helm-schema ## Run helm schema over chart
	$(HELM) schema -input charts/etcd-operator/values.yaml -output charts/etcd-operator/values.schema.json

.PHONY: helm-docs-run
helm-docs-run: helm-docs ## Run helm schema over chart
	$(HELM_DOCS)

.PHONY: helm-crd-copy
helm-crd-copy: yq kustomize ## Copy CRDs from kustomize to helm-chart
	@$(eval TMP := $(shell mktemp -d))
	@$(KUSTOMIZE) build config/default > $(TMP)/manifest.yaml && cd $(TMP) && $(YQ) -s '.kind + "-" + .metadata.name' --no-doc manifest.yaml && cd $(OLDPWD)
	@mv $(TMP)/CustomResourceDefinition-etcdclusters.etcd.aenix.io charts/etcd-operator/crds/etcd-cluster.yaml
	@mv $(TMP)/CustomResourceDefinition-etcdbackups.etcd.aenix.io charts/etcd-operator/crds/etcd-backup.yaml
	@mv $(TMP)/CustomResourceDefinition-etcdbackupschedules.etcd.aenix.io charts/etcd-operator/crds/etcd-backup-schedule.yaml
	@rm -rf $(TMP)

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager cmd/manager/main.go
	go build -o bin/backup-agent cmd/backup-agent/main.go
	go build -o bin/restore-agent cmd/restore-agent/main.go

.PHONY: build-backup-agent
build-backup-agent: ## Build backup-agent binary.
	go build -o bin/backup-agent cmd/backup-agent/main.go

.PHONY: build-restore-agent
build-restore-agent: ## Build restore-agent binary.
	go build -o bin/restore-agent cmd/restore-agent/main.go

build-plugin:
	go build -o bin/kubectl-etcd cmd/kubectl-etcd/main.go

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: ## Build docker image with the manager.
	$(CONTAINER_TOOL) build -t ${IMG} .

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name project-v3-builder
	$(CONTAINER_TOOL) buildx use project-v3-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm project-v3-builder
	rm Dockerfile.cross

.PHONY: build-dist-manifests
build-dist-manifests: manifests generate kustomize yq ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image ghcr.io/aenix-io/etcd-operator=${IMG}
	$(KUSTOMIZE) build config/default > dist/etcd-operator.yaml
	$(KUSTOMIZE) build config/default | $(YQ) eval 'select(.kind != "CustomResourceDefinition")' -  > dist/etcd-operator.non-crds.yaml
	$(KUSTOMIZE) build config/default | $(YQ) eval 'select(.kind == "CustomResourceDefinition")' -  > dist/etcd-operator.crds.yaml

##@ Deployment

KIND_CLUSTER_NAME ?= etcd-operator-kind
NAMESPACE ?= etcd-operator-system

# renovate: datasource=github-tags depName=prometheus-operator/prometheus-operator
PROMETHEUS_OPERATOR_VERSION ?= v0.74.0
# renovate: datasource=github-tags depName=jetstack/cert-manager
CERT_MANAGER_VERSION ?= v1.15.0

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete -n $(NAMESPACE) --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image ghcr.io/aenix-io/etcd-operator=${IMG}
	$(KUSTOMIZE) build config/default | $(KUBECTL) -n $(NAMESPACE) apply -f -
	$(KUBECTL) wait deployment.apps/etcd-operator-controller-manager --for condition=Available --namespace $(NAMESPACE) --timeout 5m

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/default | $(KUBECTL) delete -n $(NAMESPACE) --ignore-not-found=$(ignore-not-found) -f -

.PHONY: redeploy
redeploy: deploy ## Redeploy controller with new docker image.
	# force recreate pods
	$(KUBECTL) rollout restart -n $(NAMESPACE) deploy/etcd-operator-controller-manager

.PHONY: kind-load
kind-load: docker-build kind ## Build and upload docker image to the local Kind cluster.
	$(KIND) load docker-image ${IMG} --name $(KIND_CLUSTER_NAME)

.PHONY: kind-create
kind-create: kind yq ## Create kubernetes cluster using Kind.
	@if ! $(KIND) get clusters | grep -q $(KIND_CLUSTER_NAME); then \
		$(KIND) create cluster --name $(KIND_CLUSTER_NAME) --image kindest/node:$(K8S_VERSION) --config test/e2e/config.yaml; \
	elif ! $(CONTAINER_TOOL) container inspect $$($(KIND) get nodes --name $(KIND_CLUSTER_NAME)) | $(YQ) e '.[0].Config.Image' | grep -q $(K8S_VERSION); then \
  		$(KIND) delete cluster --name $(KIND_CLUSTER_NAME); \
		$(KIND) create cluster --name $(KIND_CLUSTER_NAME) --image kindest/node:$(K8S_VERSION) --config test/e2e/config.yaml; \
	fi

.PHONY: kind-delete
kind-delete: kind ## Create kubernetes cluster using Kind.
	@if $(KIND) get clusters | grep -q $(KIND_CLUSTER_NAME); then \
		$(KIND) delete cluster --name $(KIND_CLUSTER_NAME); \
	fi

.PHONY: kind-prepare
kind-prepare: kind-create
	# Install prometheus operator
	$(KUBECTL) apply --server-side -f "https://github.com/prometheus-operator/prometheus-operator/releases/download/$(PROMETHEUS_OPERATOR_VERSION)/bundle.yaml"
	$(KUBECTL) wait deployment.apps/prometheus-operator --for condition=Available --namespace default --timeout 5m
	# Install cert-manager operator
	$(KUBECTL) apply --server-side -f "https://github.com/jetstack/cert-manager/releases/download/$(CERT_MANAGER_VERSION)/cert-manager.yaml"
	$(KUBECTL) wait deployment.apps/cert-manager-webhook --for condition=Available --namespace cert-manager --timeout 5m

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

HELM_PLUGINS ?= $(LOCALBIN)/helm-plugins
export HELM_PLUGINS
$(HELM_PLUGINS):
	mkdir -p $(HELM_PLUGINS)

## Tool Binaries
KUBECTL ?= kubectl
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT ?= $(LOCALBIN)/golangci-lint
NILAWAY_LINT ?= $(LOCALBIN)/nilaway
KIND ?= $(LOCALBIN)/kind
HELM ?= $(LOCALBIN)/helm
HELM_DOCS ?= $(LOCALBIN)/helm-docs
YQ = $(LOCALBIN)/yq
CRD_REF_DOCS ?= $(LOCALBIN)/crd-ref-docs

## Tool Versions
# renovate: datasource=github-tags depName=kubernetes-sigs/kustomize
KUSTOMIZE_VERSION ?= v5.3.0
# renovate: datasource=github-tags depName=kubernetes-sigs/controller-tools
CONTROLLER_TOOLS_VERSION ?= v0.20.1
ENVTEST_VERSION ?= latest
# renovate: datasource=github-tags depName=golangci/golangci-lint
GOLANGCI_LINT_VERSION ?= v1.64.8
# renovate: datasource=github-tags depName=kubernetes-sigs/kind
KIND_VERSION ?= v0.31.0
# renovate: datasource=github-tags depName=helm/helm
HELM_VERSION ?= v3.15.2
# renovate: datasource=github-tags depName=losisin/helm-values-schema-json
HELM_SCHEMA_VERSION ?= v1.4.1
# renovate: datasource=github-tags depName=norwoodj/helm-docs
HELM_DOCS_VERSION ?= v1.13.1
# renovate: datasource=github-tags depName=mikefarah/yq
YQ_VERSION ?= v4.44.1

## Tool install scripts
KUSTOMIZE_INSTALL_SCRIPT ?= "https://raw.githubusercontent.com/kubernetes-sigs/kustomize/master/hack/install_kustomize.sh"
HELM_INSTALL_SCRIPT ?= "https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3"

.PHONY: kustomize
kustomize: $(LOCALBIN)
	@if test -x $(KUSTOMIZE) && ! $(KUSTOMIZE) version | grep -q $(KUSTOMIZE_VERSION); then \
		rm -f $(KUSTOMIZE); \
	fi
	@test -x $(KUSTOMIZE) || { curl -Ss $(KUSTOMIZE_INSTALL_SCRIPT) | bash -s -- $(subst v,,$(KUSTOMIZE_VERSION)) $(LOCALBIN); }

.PHONY: controller-gen
controller-gen: $(LOCALBIN)
	@test -x $(CONTROLLER_GEN) && $(CONTROLLER_GEN) --version | grep -q $(CONTROLLER_TOOLS_VERSION) || \
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_TOOLS_VERSION)

.PHONY: envtest
envtest: $(LOCALBIN)
	@test -x $(ENVTEST) || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)

.PHONY: crd-ref-docs
crd-ref-docs: $(LOCALBIN)
	@test -x $(CRD_REF_DOCS) || GOBIN=$(LOCALBIN) go install github.com/elastic/crd-ref-docs@latest

.PHONY: golangci-lint
golangci-lint: $(LOCALBIN)
	@test -x $(GOLANGCI_LINT) && $(GOLANGCI_LINT) version | grep -q $(GOLANGCI_LINT_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

.PHONY: nilaway
nilaway: $(LOCALBIN)
	@test -x $(NILAWAY_LINT) || GOBIN=$(LOCALBIN) go install go.uber.org/nilaway/cmd/nilaway@latest

kind: $(LOCALBIN)
	@test -x $(KIND) && $(KIND) version | grep -q $(KIND_VERSION) || \
	GOBIN=$(LOCALBIN) go install sigs.k8s.io/kind@$(KIND_VERSION)

.PHONY: helm
helm: $(LOCALBIN)
	@if test -x $(HELM) && ! $(HELM) version | grep -q $(HELM_VERSION); then \
		rm -f $(HELM); \
	fi
	@test -x $(HELM) || { curl -Ss $(HELM_INSTALL_SCRIPT) | sed "s|/usr/local/bin|$(LOCALBIN)|" | PATH="$(LOCALBIN):$(PATH)" bash -s -- --no-sudo --version $(HELM_VERSION); }

.PHONY: helm-schema
helm-schema: helm $(HELM_PLUGINS)
	@if ! $(HELM) plugin list | grep schema | grep -q $(subst v,,$(HELM_SCHEMA_VERSION)); then \
		if $(HELM) plugin list | grep -q schema ; then \
			$(HELM) plugin uninstall schema; \
		fi; \
		$(HELM) plugin install https://github.com/losisin/helm-values-schema-json --version=$(HELM_SCHEMA_VERSION); \
	fi

.PHONY: helm-docs
helm-docs: $(LOCALBIN)
	@test -x $(HELM_DOCS) && $(HELM_DOCS) version | grep -q $(HELM_DOCS_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/norwoodj/helm-docs/cmd/helm-docs@$(HELM_DOCS_VERSION)

.PHONY: yq
yq: $(LOCALBIN)
	@test -x $(YQ) && $(YQ) version | grep -q $(YQ_VERSION) || \
	GOBIN=$(LOCALBIN) go install github.com/mikefarah/yq/v4@$(YQ_VERSION)
