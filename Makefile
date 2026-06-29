
# Image URL to use all building/pushing image targets
IMG ?= controller:latest

# Version stamped into the standalone CLIs (etcd-migrate, kubectl-etcd) via
# -ldflags. Defaults to `git describe`; the release workflow passes the tag.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
CLI_LDFLAGS ?= -X main.version=$(VERSION)
# ENVTEST_K8S_VERSION is derived from the k8s.io/api version in go.mod so a
# dependency bump automatically pulls the matching envtest assets — no need
# to remember to update this in two places. (Pattern stolen from
# edp-keycloak-operator.)
ENVTEST_K8S_VERSION := $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
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
manifests: controller-gen yq ## Generate CRDs and the manager RBAC rules straight into the Helm chart.
	# CRDs land in charts/etcd-operator/crd-bases/ (templates/crds.yaml renders
	# them, with the helm.sh/resource-policy:keep annotation); the manager
	# ClusterRole rules land in charts/etcd-operator/files/ for templates/rbac.yaml
	# to pull in via .Files.Get. ci.yml's codegen-drift gate (make manifests +
	# git diff) then guards BOTH against drift from the API types and the
	# +kubebuilder:rbac markers — no second source of truth, no grep guard.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd paths="./..." \
		output:crd:artifacts:config=charts/etcd-operator/crd-bases \
		output:rbac:artifacts:config=charts/etcd-operator/files
	# controller-gen emits a whole ClusterRole; the chart only needs its rules
	# (it wraps them in a release-named, labelled ClusterRole of its own).
	$(YQ) eval '.rules' charts/etcd-operator/files/role.yaml > charts/etcd-operator/files/manager-role-rules.yaml
	rm -f charts/etcd-operator/files/role.yaml

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet envtest ## Run tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test ./... -coverprofile cover.out

.PHONY: test-e2e
test-e2e: ## Run the e2e suite against the current kubeconfig context (expects cert-manager, Kamaji and the operator installed; see hack/e2e.sh).
	go test -tags e2e -count=1 ./test/e2e/ -v -timeout 45m

.PHONY: e2e
e2e: ## Provision a kind cluster with cert-manager and Kamaji, deploy the operator, run the e2e suite. KEEP_CLUSTER=1 keeps the cluster for debugging.
	hack/e2e.sh

.PHONY: release-smoke
release-smoke: ## Smoke-test the tag-release manifest install path on kind: build image -> render dist manifests -> apply -> assert operator Available and a 1-node cluster READY. KEEP_CLUSTER=1 keeps the cluster.
	hack/release-smoke.sh

.PHONY: helm-smoke
helm-smoke: ## Smoke-test the Helm chart install path on kind: build image -> helm install chart -> assert operator Available and a 1-node cluster READY. KEEP_CLUSTER=1 keeps the cluster.
	INSTALL_MODE=helm hack/release-smoke.sh

##@ Build

.PHONY: build
build: manifests generate fmt vet ## Build manager binary.
	go build -o bin/manager main.go

.PHONY: kubectl-etcd
kubectl-etcd: fmt vet ## Build the kubectl-etcd plugin binary.
	go build -ldflags "$(CLI_LDFLAGS)" -o bin/kubectl-etcd ./cmd/kubectl-etcd

.PHONY: etcd-migrate
etcd-migrate: fmt vet ## Build the etcd-migrate (legacy v1alpha1 -> v1alpha2) CLI binary.
	go build -ldflags "$(CLI_LDFLAGS)" -o bin/etcd-migrate ./cmd/etcd-migrate

.PHONY: dist-cli
dist-cli: ## Cross-compile etcd-migrate and kubectl-etcd into dist/ for release (linux/darwin x amd64/arm64). VERSION stamps the binary version.
	# Produces the standalone CLIs the release-assets workflow attaches to a
	# release, named <cmd>-<os>-<arch>, plus a SHA256 checksum file. These are
	# client-side tools (kubectl-etcd is a kubectl plugin, etcd-migrate is an
	# admin-run migration CLI), so they ship as binaries, not in the operator image.
	mkdir -p dist
	for os in linux darwin; do for arch in amd64 arm64; do \
	  for cmd in etcd-migrate kubectl-etcd; do \
	    echo "building dist/$$cmd-$$os-$$arch"; \
	    CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch \
	      go build -ldflags "$(CLI_LDFLAGS)" -o dist/$$cmd-$$os-$$arch ./cmd/$$cmd; \
	  done; \
	done; done
	cd dist && { command -v sha256sum >/dev/null 2>&1 && sha256sum etcd-migrate-* kubectl-etcd-* || shasum -a 256 etcd-migrate-* kubectl-etcd-*; } > cli-SHA256SUMS.txt

.PHONY: run
run: manifests generate fmt vet ## Run a controller from your host.
	go run ./main.go

# If you wish built the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64 ). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
.PHONY: docker-build
docker-build: test ## Build docker image with the manager.
	docker build -t ${IMG} .

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	docker push ${IMG}

# PLATFORMS defines the target platforms for  the manager image be build to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - able to use docker buildx . More info: https://docs.docker.com/build/buildx/
# - have enable BuildKit, More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image for your registry (i.e. if you do not inform a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To properly provided solutions that supports more than one platform you should use this option.
PLATFORMS ?= linux/arm64,linux/amd64,linux/s390x,linux/ppc64le
.PHONY: docker-buildx
docker-buildx: test ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- docker buildx create --name project-v3-builder
	docker buildx use project-v3-builder
	- docker buildx build --push --platform=$(PLATFORMS) --tag ${IMG} -f Dockerfile.cross .
	- docker buildx rm project-v3-builder
	rm Dockerfile.cross

.PHONY: build-dist-manifests
build-dist-manifests: manifests generate require-helm yq ## Render the release install manifests into dist/ for IMG.
	# Produces the YAMLs the release-assets workflow attaches to a tag, for users
	# who kubectl-apply instead of helm-install:
	#   dist/etcd-operator.yaml          – everything (Namespace + CRDs + operator)
	#   dist/etcd-operator.crds.yaml     – CRDs only
	#   dist/etcd-operator.non-crds.yaml – everything except CRDs
	# This is just `helm template` of the chart, so the rendered manifest IS the
	# chart: the image == OPERATOR_IMAGE wiring and the RBAC come from one source.
	# namespace.create emits the Namespace so a bare `kubectl apply -f
	# etcd-operator.yaml` is self-contained. Rendering is pure — no tracked file
	# is mutated. Pass IMG=<registry>/etcd-operator:<tag>.
	mkdir -p dist
	img='$(IMG)'; $(HELM) template etcd-operator charts/etcd-operator \
		--namespace etcd-operator-system \
		--set image.repository="$${img%:*}" --set image.tag="$${img##*:}" \
		--set namespace.create=true \
		> dist/etcd-operator.yaml
	$(YQ) eval 'select(.kind != "CustomResourceDefinition")' dist/etcd-operator.yaml > dist/etcd-operator.non-crds.yaml
	$(YQ) eval 'select(.kind == "CustomResourceDefinition")' dist/etcd-operator.yaml > dist/etcd-operator.crds.yaml

##@ Deployment

# The Helm-driven install targets below. The chart is the single source of
# truth for CRDs, RBAC, and the manager Deployment.
HELM_RELEASE ?= etcd-operator
NAMESPACE ?= etcd-operator-system

.PHONY: deploy
deploy: manifests require-helm ## Install/upgrade the operator (CRDs + RBAC + manager) via Helm. Pass IMG=<registry>/etcd-operator:<tag>.
	# The chart renders image == OPERATOR_IMAGE, so there is no separate image-
	# replacement step; CRDs are templated into the release so `helm upgrade`
	# keeps them current. The IMG split into repository:tag handles registry ports.
	img='$(IMG)'; $(HELM) upgrade --install $(HELM_RELEASE) charts/etcd-operator \
		--namespace $(NAMESPACE) --create-namespace \
		--set image.repository="$${img%:*}" --set image.tag="$${img##*:}" \
		$(HELM_EXTRA_ARGS) \
		--wait --timeout 5m

.PHONY: undeploy
undeploy: require-helm ## Uninstall the operator release. CRDs carry resource-policy:keep, so EtcdClusters survive — delete them (and the CRDs) by hand to wipe data.
	$(HELM) uninstall $(HELM_RELEASE) --namespace $(NAMESPACE)

##@ Build Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Versions
CONTROLLER_TOOLS_VERSION ?= v0.18.0
YQ_VERSION ?= v4.44.1

## Tool Binaries (version-suffixed so a version bump auto-triggers reinstall
## and stale builds of an old version stay on disk alongside the new one).
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen-$(CONTROLLER_TOOLS_VERSION)
ENVTEST ?= $(LOCALBIN)/setup-envtest
YQ ?= $(LOCALBIN)/yq-$(YQ_VERSION)
# Helm is the one tool we don't vendor (no clean `go install`); it must be on
# PATH. release-smoke/e2e and the publish workflows install it via setup-helm.
HELM ?= helm

# go-install-tool installs $2@$3 under $1. `go install` drops the binary at
# $LOCALBIN/<basename>, so we rename it after install to the version-suffixed
# target path.
define go-install-tool
@[ -f $(1) ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv "$$(echo "$(1)" | sed "s/-$(3)$$//")" $(1) ;\
}
endef

.PHONY: require-helm
require-helm: ## Assert Helm is on PATH (used by deploy/undeploy/build-dist-manifests).
	@command -v $(HELM) >/dev/null 2>&1 || { \
		echo "ERROR: helm not found on PATH. Install Helm v3.16+ (https://helm.sh/docs/intro/install/)."; \
		exit 1; \
	}

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: envtest
envtest: $(ENVTEST) ## Download envtest-setup locally if necessary.
$(ENVTEST): $(LOCALBIN)
	test -s $(LOCALBIN)/setup-envtest || GOBIN=$(LOCALBIN) go install sigs.k8s.io/controller-runtime/tools/setup-envtest@latest

.PHONY: yq
yq: $(YQ) ## Download yq locally if necessary.
$(YQ): $(LOCALBIN)
	$(call go-install-tool,$(YQ),github.com/mikefarah/yq/v4,$(YQ_VERSION))
