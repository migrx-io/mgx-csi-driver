# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# output dir
OUT_DIR := ./bin
# dir for tools: e.g., golangci-lint
TOOL_DIR := $(OUT_DIR)/tool
# use golangci-lint for static code check
GOLANGCI_VERSION := v2.6.0
GOLANGCI_BIN := $(TOOL_DIR)/golangci-lint
KIND_VERSION ?= v0.26.0
KIND_CLUSTER_NAME ?= mgx-cluster

# go source, scripts
SOURCE_DIRS := cmd pkg

# goarch for cross building
ifeq ($(origin GOARCH), undefined)
  GOARCH := $(shell go env GOARCH)
endif

ifeq ($(origin CSI_IMAGE_REGISTRY), undefined)
  CSI_IMAGE_REGISTRY := migrx
endif
ifeq ($(origin CSI_IMAGE_TAG), undefined)
  CSI_IMAGE_TAG := latest
endif

CSI_IMAGE := $(CSI_IMAGE_REGISTRY)/mgx-csi-driver:$(CSI_IMAGE_TAG)

# default target
all: build lint test

# build binary
.PHONY: build
build:
	@echo === building binary
	@CGO_ENABLED=0 GOARCH=$(GOARCH) GOOS=$(GOOS) go build -buildvcs=false -o $(OUT_DIR)/mgxcsi ./cmd/

.PHONY: docker-build
docker-build:
	@docker build -t $(CSI_IMAGE) .

.PHONY: docker-buildx
docker-buildx:
	@docker buildx build -t $(CSI_IMAGE) --platform linux/amd64,linux/arm64 . --push

# static code check, text lint
# lint: golangci yamllint shellcheck mdl codespell
lint: golangci

.PHONY: golangci
golangci: $(GOLANGCI_BIN)
	@echo === running golangci-lint
	@$(TOOL_DIR)/golangci-lint --config=.golangci.yml run ./...

$(GOLANGCI_BIN):
	@echo === installing golangci-lint
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | bash -s -- -b $(TOOL_DIR) $(GOLANGCI_VERSION)


.PHONY: kind-create-cluster
kind-create-cluster: install-kind
	@echo "Creating Kind cluster '$(KIND_CLUSTER_NAME)' with 3 worker nodes..."
	@$(TOOL_DIR)/kind create cluster --name $(KIND_CLUSTER_NAME) --config ./e2e/kind-config.yaml
	@echo "Kind cluster '$(KIND_CLUSTER_NAME)' created successfully"


.PHONY: install-kind
install-kind:
	@echo "Installing Kind version $(KIND_VERSION)..."
	@curl -Lo $(TOOL_DIR)/kind https://kind.sigs.k8s.io/dl/$(KIND_VERSION)/kind-$(shell uname)-$(shell uname -m)
	@chmod +x $(TOOL_DIR)/kind

# tests
test: mod-check unit-test

.PHONY: mod-check
mod-check:
	@echo === running go mod verify
	@go mod verify

.PHONY: unit-test
unit-test:
	@echo === running unit test
	@go test -v -race -cover $(foreach d,$(SOURCE_DIRS),./$(d)/...)

# e2e test
.PHONY: e2e-test
# Pass extra arguments to e2e tests. Could be used
# to pass -xpu argument and running only fouced tests
# for quick testing.
# The below example tests:
#   make e2e-test E2E_TEST_ARGS='--ginkgo.focus=\"TEST\"'
E2E_TEST_ARGS=
e2e-test:
	@echo === running e2e test
	go test -v -race -timeout 30m ./e2e $(E2E_TEST_ARGS)

.PHONY: clean
clean:
	rm -rf $(OUT_DIR)
	go clean -testcache
