# Root Makefile

# List of service paths relative to root
SERVICES = \
    consumers/ingestion \
    services/api_gateway \
    services/data_catalog \
    services/identity \
    services/jwt \
    services/rcss \
    services/schema_registry \
    services/streaming_ingestion

# Dockerfiles for dfs are special-cased
DFS_DOCKERFILES = \
    dfs/Dockerfile.master \
    dfs/Dockerfile.worker

# Minikube Docker environment
export DOCKER_BUILDKIT=1

.PHONY: all docker-env build-docker build-push clean

# Build all images
all: docker-env build-dfs build-services

# Point Docker CLI to Minikube daemon
docker-env:
	@echo "🟢 Using Minikube Docker daemon"
	@eval $(minikube docker-env) && echo "✅ Docker pointed to Minikube"

# Build DFS (master + worker) with custom Dockerfile names
build-dfs:
	@echo "🔧 Building dfs/master..."
	docker build -f dfs/Dockerfile.master -t datalake/master:latest dfs
	@echo "🔧 Building dfs/worker..."
	docker build -f dfs/Dockerfile.worker -t datalake/worker:latest dfs

# Build each service using its default Dockerfile
build-services:
	@for svc in $(SERVICES); do \
		name=$$(basename $$svc); \
		echo "🔧 Building $$name..."; \
		docker build -t datalake/$$name:latest $$svc; \
	done

# Optionally push to a registry (not used in Minikube by default)
build-push: all
	@for svc in $(SERVICES); do \
		name=$$(basename $$svc); \
		docker push datalake/$$name:latest; \
	done
	docker push datalake/master:latest
	docker push datalake/worker:latest

# Remove local images
clean:
	@for svc in $(SERVICES); do \
		name=$$(basename $$svc); \
		docker rmi -f datalake/$$name:latest || true; \
	done
	docker rmi -f datalake/master:latest datalake/worker:latest || true
