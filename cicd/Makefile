NAMESPACE = datalake
REGISTRY = docker.io/razvan1y
K8S_DIR = ../k8s/plain-yaml

IMAGES := $(shell docker images --format '{{.Repository}}' | grep '^datalake/')

# Service mappings: source_path:deployment_file:deployment_name
SERVICE_MAPPINGS = \
	services/ingestion-consumer:consumer-deployment.yml:ingestion-consumer \
	services/api-gateway:api_gateway.yaml:api-gateway \
	services/data-catalog:data-catalog-deployment.yaml:data-catalog \
	services/identity:identity-service.yaml:identity-service \
	services/metadata-service:metadata-deployment.yml:metadata-service \
	services/streaming-ingestion:stream_ingest.yml:streaming-ingestion \
	services/query-service:query-service.yml:query-service

export DOCKER_BUILDKIT=1

.PHONY: all apply-rbac docker-env docker-login deploy deploy-all port-forward rollout rollout-all list-services help status clean-ports

all: docker-env
# DOCKER

docker-env:
	@echo "🟢 Using Minikube Docker daemon"
	@eval $$(minikube docker-env) && echo "✅ Docker pointed to Minikube"

docker-login:
	@echo "🔒 Logging into Docker Hub..."
	docker login

deploy-master:
	docker build -t datalake/dfs-master:latest -f ../services/dfs/Dockerfile.master ../services/dfs
	docker tag datalake/dfs-master:latest $(REGISTRY)/dfs-master:latest
	docker push $(REGISTRY)/dfs-master:latest
	@echo "☸️  Deploying to Kubernetes..."
	kubectl apply -f $(K8S_DIR)/master-deployment.yaml -n $(NAMESPACE)
	kubectl set image deployment/master master=$(REGISTRY)/dfs-master:latest -n $(NAMESPACE)
	kubectl rollout restart deployment/master -n $(NAMESPACE)
	@echo "✅ dfs-master deployed successfully!"

deploy:
	@if [ -z "$(SERVICE)" ]; then \
		echo "❌ Please specify SERVICE=<name>"; exit 1; \
	fi
	@echo "🚀 Deploying $(SERVICE)..."
	$(eval MAPPING := $(shell echo "$(SERVICE_MAPPINGS)" | tr ' ' '\n' | grep -E ":.*$(SERVICE)$$" | head -1))
	@if [ -z "$(MAPPING)" ]; then \
		echo "❌ Service '$(SERVICE)' not found in mappings"; \
		echo "Available services:"; \
		echo "$(SERVICE_MAPPINGS)" | tr ' ' '\n' | cut -d':' -f3 | sed 's/^/  - /'; \
		exit 1; \
	fi
	$(eval SERVICE_PATH := $(shell echo "$(MAPPING)" | cut -d':' -f1))
	$(eval DEPLOY_FILE := $(shell echo "$(MAPPING)" | cut -d':' -f2))
	$(eval DEPLOY_NAME := $(shell echo "$(MAPPING)" | cut -d':' -f3))
	@echo "📁 Source: $(SERVICE_PATH)"
	@echo "📄 Deploy file: $(DEPLOY_FILE)"
	@echo "🏷️  Deploy name: $(DEPLOY_NAME)"
	@if [ ! -d "../$(SERVICE_PATH)" ]; then \
		echo "❌ Source directory ../$(SERVICE_PATH) not found"; exit 1; \
	fi
	@if [ ! -f "$(K8S_DIR)/$(DEPLOY_FILE)" ]; then \
		echo "❌ Deployment file $(K8S_DIR)/$(DEPLOY_FILE) not found"; exit 1; \
	fi
	@echo "🔨 Building Docker image..."
	docker build -t datalake/$(SERVICE):latest ../$(SERVICE_PATH)
	docker tag datalake/$(SERVICE):latest $(REGISTRY)/$(SERVICE):latest
	docker push $(REGISTRY)/$(SERVICE):latest
	@echo "☸️  Deploying to Kubernetes..."
	kubectl apply -f $(K8S_DIR)/$(DEPLOY_FILE) -n $(NAMESPACE)
	kubectl set image deployment/$(DEPLOY_NAME) $(DEPLOY_NAME)=$(REGISTRY)/$(SERVICE):latest -n $(NAMESPACE)
	kubectl rollout restart deployment/$(DEPLOY_NAME) -n $(NAMESPACE)
	@echo "✅ $(SERVICE) deployed successfully!"


apply-rbac:
	kubectl apply -f ../k8s/plain-yaml/rbac/rbac.yaml \

deploy-all:
	make apply-rbac
	@echo "Deploying all services..."
	@for mapping in $(SERVICE_MAPPINGS); do \
		service_path=$$(echo "$$mapping" | cut -d':' -f1); \
		deploy_file=$$(echo "$$mapping" | cut -d':' -f2); \
		deploy_name=$$(echo "$$mapping" | cut -d':' -f3); \
		service_name=$$(basename "$$service_path"); \
		echo ""; \
		echo "Deploying $$deploy_name ($$service_name)..."; \
		echo "Source: $$service_path"; \
		echo "Deploy file: $$deploy_file"; \
		if [ ! -d "../$$service_path" ]; then \
			echo "Skipping $$deploy_name - source directory ../$$service_path not found"; \
			continue; \
		fi; \
		if [ ! -f "$(K8S_DIR)/$$deploy_file" ]; then \
			echo "Skipping $$deploy_name - deployment file $(K8S_DIR)/$$deploy_file not found"; \
			continue; \
		fi; \
		echo "Building Docker image for $$service_name..."; \
		docker build -t datalake/$$service_name:latest ../$$service_path || { echo "Build failed for $$service_name"; continue; }; \
		docker tag datalake/$$service_name:latest $(REGISTRY)/$$service_name:latest; \
		docker push $(REGISTRY)/$$service_name:latest || { echo "Push failed for $$service_name"; continue; }; \
		echo "Deploying $$deploy_name to Kubernetes..."; \
		kubectl apply -f $(K8S_DIR)/$$deploy_file -n $(NAMESPACE) || { echo "Apply failed for $$deploy_name"; continue; }; \
		kubectl set image deployment/$$deploy_name $$deploy_name=$(REGISTRY)/$$service_name:latest -n $(NAMESPACE) || true; \
		kubectl rollout restart deployment/$$deploy_name -n $(NAMESPACE) || true; \
		echo "$$deploy_name deployed successfully"; \
	done
	@echo ""
	@echo "All services deployment completed"

port-forward:
	@echo "🌐 Setting up port forwards..."
	kubectl port-forward -n datalake svc/api-gateway 8083:80 > /dev/null 2>&1 &
	kubectl port-forward -n observability svc/jaeger-query 16686:16686 > /dev/null 2>&1 &
	kubectl port-forward -n datalake svc/identity-service 8082:8082 > /dev/null 2>&1 &
	kubectl port-forward -n datalake svc/metadata-service 8081:8081 > /dev/null 2>&1 &
	kubectl port-forward svc/prometheus 9090:9090 -n observability > /dev/null 2>&1 &
	kubectl port-forward svc/grafana 3000:3000 -n observability > /dev/null 2>&1 &
	kubectl port-forward svc/alertmanager 9093:9093 -n observability > /dev/null 2>&1 &
	kubectl port-forward svc/loki 3100:3100 -n observability > /dev/null 2>&1 &
	kubectl port-forward svc/query-service 8086:80 -n datalake > /dev/null 2>&1 &
	@echo "✅ Port forwards started:"
	@echo "  - API Gateway:     http://localhost:8083"
	@echo "  - Identity Service: http://localhost:8082"
	@echo "  - Metadata Service:  http://localhost:8081"
	@echo "  - Prometheus:      http://localhost:9090"
	@echo "  - Grafana:         http://localhost:3000"
	@echo "  - Jaeger:          http://localhost:16686"
	@echo "  - AlertManager:    http://localhost:9093"
	@echo "  - Loki:            http://localhost:3100"
	@echo "  - Query Service:   http://localhost:8087"
rollout:
	@if [ -z "$(SERVICE)" ]; then \
		echo "❌ Please specify SERVICE=<name>"; exit 1; \
	fi
	$(eval MAPPING := $(shell echo "$(SERVICE_MAPPINGS)" | tr ' ' '\n' | grep -E ":.*$(SERVICE)$$" | head -1))
	@if [ -z "$(MAPPING)" ]; then \
		echo "❌ Service '$(SERVICE)' not found in mappings"; exit 1; \
	fi
	$(eval DEPLOY_NAME := $(shell echo "$(MAPPING)" | cut -d':' -f3))
	@echo "🔄 Rolling out $(SERVICE) ($(DEPLOY_NAME))..."
	kubectl rollout restart deployment/$(DEPLOY_NAME) -n $(NAMESPACE)

rollout-all:
	@echo "🔄 Rolling out all services..."
	@for mapping in $(SERVICE_MAPPINGS); do \
		deploy_name=$$(echo "$$mapping" | cut -d':' -f3); \
		echo "🔄 Rolling out $$deploy_name..."; \
		kubectl rollout restart deployment/"$$deploy_name" -n $(NAMESPACE) || echo "⚠️  Failed to rollout $$deploy_name"; \
	done
	@echo "✅ All rollouts completed!"

# Helper targets
list-services:
	@echo "$(SERVICE_MAPPINGS)" | tr ' ' '\n' | while IFS=':' read -r path file name; do \
		echo "  - $$name ($$path)"; \
	done

help:
	@echo "🚀 Datalake Deployment Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make deploy SERVICE=<name>     Deploy a specific service"
	@echo "  make deploy-all                Deploy all services"
	@echo "  make rollout SERVICE=<name>    Rollout restart a service"
	@echo "  make rollout-all               Rollout restart all services"
	@echo "  make port-forward              Setup port forwards for all services"
	@echo "  make list-services             List all available services"
	@echo "  make docker-env                Configure Docker for Minikube"
	@echo ""
	@echo "Examples:"
	@echo "  make deploy SERVICE=identity-service"
	@echo "  make deploy SERVICE=api-gateway"
	@echo "  make deploy-all"
	@echo ""
	@echo "📋 Available services:"
	@make list-services

# Check deployment status
status:
	@echo "📊 Deployment Status:"
	@kubectl get deployments -n $(NAMESPACE)
	@echo ""
	@echo "🎯 Pod Status:"
	@kubectl get pods -n $(NAMESPACE)

# Clean up port forwards
clean-ports:
	@echo "🧹 Cleaning up port forwards..."
	@pkill -f "kubectl port-forward" || true
	@echo "✅ Port forwards cleaned up"
