
REGISTRY ?= datalake
TAG ?= latest

SERVICES := api-gateway \
            identity-service \
            metadata-service \
            query-service \
            ingestion-consumer \
            streaming-ingestion \
            data-catalog \
            compactor \
            dfs-master \
            dfs-worker

.PHONY: help scan scan-all scan-k8s

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%%-20s\033[0m %%s\n", $$1, $$2}'

scan-k8s:
	@echo "☸️  Scanning Kubernetes Manifests..."
	@(eval $$(minikube docker-env -u); \
	docker run --rm \
		-v $(shell cd .. && pwd)/k8s:/k8s \
		aquasec/trivy config /k8s > reports/k8s_audit.txt)
	@echo "✅ K8s audit finished. Check reports/k8s_audit.txt"

scan:
	@if [ -z "$(SERVICE)" ]; then echo "Error: SERVICE not set."; exit 1; fi
	@mkdir -p reports
	@echo "🔍 Scanning $(REGISTRY)/$(SERVICE):$(TAG)..."

	@docker run --rm \
		-v /var/run/docker.sock:/var/run/docker.sock \
		aquasec/trivy image \
		--severity HIGH,CRITICAL \
		--scanners vuln \
		--format json \
		$(REGISTRY)/$(SERVICE):$(TAG) > reports/$(SERVICE).json

	@echo "✅ Scan finished. Open 'reports/$(SERVICE).html' in your browser to view it."

scan-all:
	@for service in $(SERVICES); do \
		$(MAKE) scan SERVICE=$$service; \
	done

PROTO_ROOT := ../protobuf

proto: proto-clean proto-lint
	@echo "🛠️  Generating Protobuf code..."
	cd $(PROTO_ROOT) && buf generate
	@echo "✅ Protobuf generation complete! Files updated in $(PROTO_ROOT)/gen/go/"

proto-lint:
	@echo "🔍 Linting Protobuf files..."
	cd $(PROTO_ROOT) && buf lint
	@echo "✅ Linting passed!"

proto-clean:
	@echo "🧹 Cleaning generated Protobuf files..."
	rm -rf $(PROTO_ROOT)/gen/go/*
