SERVICES = \
    consumers/ingestion \
    services/api_gateway \
    services/data_catalog \
    services/identity \
    services/jwt \
    services/rcss \
    services/schema-registry \
    services/streaming_ingestion


export DOCKER_BUILDKIT=1

.PHONY: all docker-env build-docker build-push clean

all: docker-env build-dfs build-services

docker-env:
	@echo "🟢 Using Minikube Docker daemon"
	@eval $(minikube docker-env) && echo "✅ Docker pointed to Minikube"

build-dfs:
	@echo "🔧 Building dfs/master..."
	docker build -f services/dfs/Dockerfile.master -t datalake/master:latest services/dfs
	@echo "🔧 Building dfs/worker..."
	docker build -f services/dfs/Dockerfile.worker -t datalake/worker:latest services/dfs

build-services:
	@for svc in $(SERVICES); do \
		name=$$(basename $$svc); \
		echo "🔧 Building $$name..."; \
		docker build -t datalake/$$name:latest $$svc; \
	done

build-push: all
	@for svc in $(SERVICES); do \
		name=$$(basename $$svc); \
		docker push datalake/$$name:latest; \
	done
	docker push datalake/master:latest
	docker push datalake/worker:latest

clean:
	@for svc in $(SERVICES); do \
		name=$$(basename $$svc); \
		docker rmi -f datalake/$$name:latest || true; \
	done
	docker rmi -f datalake/master:latest datalake/worker:latest || true


pfa:
	kubectl port-forward -n argocd svc/argocd-server 8080:443&

port-forward:
	kubectl port-forward -n datalake svc/api-gateway 8083:80&
	kubectl port-forward -n observability svc/jaeger-query 16686:16686&
	kubectl port-forward -n datalake svc/identity-service 8082:8082&
