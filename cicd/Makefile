
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

.PHONY: test test-coverage test-service

GO_SERVICES := dfs identity metadata-service query-service api-gateway ingestion-consumer streaming-ingestion compactor

test: ## Run all tests across all Go services
	@echo "🧪 Running all tests..."
	@for service in $(GO_SERVICES); do \
		echo "\n=== Testing $$service ==="; \
		cd ../services/$$service && go test ./... -v || exit 1; \
	done
	@echo "✅ All tests passed!"

test-coverage: ## Run all tests with coverage reports
	@echo "📊 Running tests with coverage..."
	@mkdir -p reports
	@for service in $(GO_SERVICES); do \
		echo "\n=== Coverage for $$service ==="; \
		(cd ../services/$$service && \
		go test ./... -coverprofile=coverage.out && \
		go tool cover -func=coverage.out | tail -1 && \
		mv coverage.out ../../cicd/reports/$$service-coverage.out) || true; \
	done
	@echo "✅ Coverage reports generated in cicd/reports/"

test-service: ## Run tests for a specific service (usage: make test-service SERVICE=dfs)
	@if [ -z "$(SERVICE)" ]; then echo "Error: SERVICE not set. Usage: make test-service SERVICE=dfs"; exit 1; fi
	@echo "🧪 Testing $(SERVICE)..."
	@cd ../services/$(SERVICE) && go test ./... -v -coverprofile=coverage.out
	@cd ../services/$(SERVICE) && go tool cover -func=coverage.out | tail -1
