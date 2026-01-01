NAMESPACE = datalake
REGISTRY = docker.io/razvan1y
K8S_DIR = ../k8s/plain-yaml

# Service mappings: source_path:deployment_file:deployment_name
# NOTE: Ensure the source path (consumers/ingestion) matches where your Go code is located.
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

# --- DOCKER SETUP ---
docker-env:
	@echo "🟢 Using Minikube Docker daemon"
	@eval $$(minikube docker-env) && echo "✅ Docker pointed to Minikube"

docker-login:
	@echo "🔒 Logging into Docker Hub..."
	docker login

# --- DEPLOYMENT TARGETS ---

# Usage: make deploy SERVICE=ingestion-consumer
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
	# Builds image: datalake/ingestion-consumer:latest
	docker build -t datalake/$(SERVICE):latest ../$(SERVICE_PATH)
	# Tags image: docker.io/razvan1y/ingestion-consumer:latest
	docker tag datalake/$(SERVICE):latest $(REGISTRY)/$(SERVICE):latest
	# Pushes to Docker Hub (or loads into Minikube if using local registry)
	docker push $(REGISTRY)/$(SERVICE):latest
	@echo "☸️  Deploying to Kubernetes..."
	# Applies the YAML
	kubectl apply -f $(K8S_DIR)/$(DEPLOY_FILE) -n $(NAMESPACE)
	# Force updates the image to ensure the new hash is pulled
	kubectl set image deployment/$(DEPLOY_NAME) $(DEPLOY_NAME)=$(REGISTRY)/$(SERVICE):latest -n $(NAMESPACE)
	# Restarts the pods to pick up the new image
	kubectl rollout restart deployment/$(DEPLOY_NAME) -n $(NAMESPACE)
	@echo "✅ $(SERVICE) deployed successfully!"

deploy-all:
	@echo "Deploying all services..."
	@for mapping in $(SERVICE_MAPPINGS); do \
		service_path=$$(echo "$$mapping" | cut -d':' -f1); \
		deploy_file=$$(echo "$$mapping" | cut -d':' -f2); \
		deploy_name=$$(echo "$$mapping" | cut -d':' -f3); \
		# Use deploy_name for image tagging to match individual deploy target
		image_name=$$deploy_name; \
		echo ""; \
		echo "Deploying $$deploy_name..."; \
		if [ ! -d "../$$service_path" ]; then echo "Skipping - src not found"; continue; fi; \
		if [ ! -f "$(K8S_DIR)/$$deploy_file" ]; then echo "Skipping - yaml not found"; continue; fi; \
		docker build -t datalake/$$image_name:latest ../$$service_path; \
		docker tag datalake/$$image_name:latest $(REGISTRY)/$$image_name:latest; \
		docker push $(REGISTRY)/$$image_name:latest; \
		kubectl apply -f $(K8S_DIR)/$$deploy_file -n $(NAMESPACE); \
		kubectl set image deployment/$$deploy_name $$deploy_name=$(REGISTRY)/$$image_name:latest -n $(NAMESPACE); \
		kubectl rollout restart deployment/$$deploy_name -n $(NAMESPACE); \
	done

# --- UTILS ---
status:
	@echo "📊 Deployment Status:"
	@kubectl get deployments -n $(NAMESPACE)
	@echo ""
	@echo "🎯 Pod Status:"
	@kubectl get pods -n $(NAMESPACE) -l app=ingestion-consumer

clean-ports:
	@echo "🧹 Cleaning up port forwards..."
	@pkill -f "kubectl port-forward" || true