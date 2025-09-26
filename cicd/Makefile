NAMESPACE = datalake
REGISTRY = docker.io/razvan1y

IMAGES := $(shell docker images --format '{{.Repository}}' | grep '^datalake/')

# This list should contain the paths to the service directories, not the deployment names.
# The Kubernetes deployment names will be derived from these paths.
SERVICES = \
	consumers/ingestion \
	services/api-gateway \
	services/data_catalog \
	services/identity-service \
	services/schema-registry \
	services/streaming-ingestion

export DOCKER_BUILDKIT=1

.PHONY: all docker-env docker-login deploy deploy-all port-forward rollout rollout-all

all: docker-env
# DOCKER

docker-env:
	@echo "🟢 Using Minikube Docker daemon"
	@eval $$(minikube docker-env) && echo "✅ Docker pointed to Minikube"

docker-login:
	@echo "🔒 Logging into Docker Hub..."
	docker login

deploy:
	@if [ -z "$(SERVICE)" ]; then \
		echo "❌ Please specify SERVICE=<name>"; exit 1; \
	fi
	@echo "🚀 Deploying $(SERVICE)..."
	# Determine the correct service path from the SERVICES list
	SERVICE_PATH := $(shell echo $(SERVICES) | grep -oE "([^[:space:]]+/$(SERVICE)|$(SERVICE))")
	# If a matching path is not found, try the default services directory
	ifeq ($(SERVICE_PATH),)
		SERVICE_PATH := services/$(SERVICE)
	endif
	
	@echo "Building Docker image from source path: ../$(SERVICE_PATH)"
	docker build -t datalake/$(SERVICE):latest ../$(SERVICE_PATH)
	docker tag datalake/$(SERVICE):latest $(REGISTRY)/$(SERVICE):latest
	docker push $(REGISTRY)/$(SERVICE):latest
	# Apply YAML changes before restarting rollout
	kubectl apply -f ../k8s/$(SERVICE)-deployment.yml -n $(NAMESPACE)
	kubectl set image deployment/$(SERVICE) $(SERVICE)=$(REGISTRY)/$(SERVICE):latest -n $(NAMESPACE)
	kubectl rollout restart deployment/$(SERVICE) -n $(NAMESPACE)


deploy-all:
	@for svc in $(SERVICES); do \
		name=$$(basename "$$svc"); \
		echo "🚀 Deploying $$name..."; \
		docker build -t datalake/"$$name":latest ../"$$svc"; \
		docker tag datalake/"$$name":latest $(REGISTRY)/"$$name":latest; \
		docker push $(REGISTRY)/"$$name":latest; \
		kubectl apply -f ../k8s/"$$name"-deployment.yml -n $(NAMESPACE); \
		kubectl set image deployment/"$$name" "$$name"=$(REGISTRY)/"$$name":latest -n $(NAMESPACE); \
		kubectl rollout restart deployment/"$$name" -n $(NAMESPACE); \
	done

port-forward:
	kubectl port-forward -n datalake svc/api-gateway 8083:80&
	kubectl port-forward -n observability svc/jaeger-query 16686:16686&
	kubectl port-forward -n datalake svc/identity-service 8082:8082&
	kubectl port-forward -n datalake svc/schema-registry 8081:8081&
	kubectl port-forward svc/prometheus 9090:9090 -n observability&
	kubectl port-forward svc/grafana 3000:3000 -n observability&
	kubectl port-forward svc/alertmanager 9093:9093 -n observability&
rollout:
	@if [ -z "$(SERVICE)" ]; then \
		echo "❌ Please specify SERVICE=<name>"; exit 1; \
	fi
	@echo "🔄 Rolling out $(SERVICE)..."
	kubectl rollout restart deployment/$(SERVICE) -n $(NAMESPACE)

rollout-all:
	@for svc in $(SERVICES); do \
		name=$$(basename "$$svc"); \
		echo "🔄 Rolling out $$name..."; \
		kubectl rollout restart deployment/"$$name" -n $(NAMESPACE); \
	done
