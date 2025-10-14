#!/bin/bash

# Deploy Datalake Monitoring Stack
# This script deploys Prometheus, Grafana, and AlertManager for monitoring the datalake platform

set -e

echo "🚀 Deploying Datalake Monitoring Stack..."

K8S_OBS_DIR="../k8s/plain-yaml/obs"

# Deploy Prometheus
echo "📊 Deploying Prometheus..."
kubectl apply -f "${K8S_OBS_DIR}/prometheus-config.yaml"
kubectl apply -f "${K8S_OBS_DIR}/prometheus-deployment.yaml"

# Deploy Grafana
echo "📈 Deploying Grafana..."
kubectl apply -f "${K8S_OBS_DIR}/grafana-config.yaml"
kubectl apply -f "${K8S_OBS_DIR}/grafana-deployment.yaml"

# Deploy AlertManager
echo "🚨 Deploying AlertManager..."
kubectl apply -f "${K8S_OBS_DIR}/alertmanager-config.yaml"

# Deploy Loki
echo "📚 Deploying Loki..."
kubectl apply -f "${K8S_OBS_DIR}/loki-deployment.yaml"
kubectl apply -f "${K8S_OBS_DIR}/loki-rbac.yaml"
kubectl apply -f "${K8S_OBS_DIR}/grafana-datasource-loki.yaml"
kubectl apply -f "${K8S_OBS_DIR}/promtail-deployment.yaml"



# Wait for deployments to be ready
echo "⏳ Waiting for deployments to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/prometheus -n observability || echo "⚠️  Prometheus deployment timeout"
kubectl wait --for=condition=available --timeout=300s deployment/grafana -n observability || echo "⚠️  Grafana deployment timeout"
kubectl wait --for=condition=available --timeout=300s deployment/alertmanager -n observability || echo "⚠️  AlertManager deployment timeout"
kubectl wait --for=condition=available --timeout=300s deployment/loki -n observability || echo "⚠️  Loki deployment timeout"
kubectl wait --for=condition=available --timeout=300s deployment/promtail -n



# Display access information
echo ""
echo "✅ Monitoring stack deployed successfully!"
echo ""
echo "🔗 Access Information:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 Prometheus:"
echo "   kubectl port-forward svc/prometheus 9090:9090 -n observability"
echo "   Then visit: http://localhost:9090"
echo ""
echo "📈 Grafana:"
echo "   kubectl port-forward svc/grafana 3000:3000 -n observability"
echo "   Then visit: http://localhost:3000"
echo "   Username: admin"
echo "   Password: datalake123"
echo ""
echo "🚨 AlertManager:"
echo "   kubectl port-forward svc/alertmanager 9093:9093 -n observability"
echo "   Then visit: http://localhost:9093"
echo ""
echo "🔍 To check the status of all monitoring components:"
echo "   kubectl get all -n observability"
echo ""
echo "📋 Service endpoints for scraping:"
echo "   API Gateway: http://api-gateway.default.svc.cluster.local:8080/metrics"
echo "   Streaming Ingestion: http://streaming-ingestion.default.svc.cluster.local:8080/metrics"
echo "   Schema Registry: http://schema-registry.default.svc.cluster.local:8080/metrics"
echo "   Identity Service: http://identity-service.default.svc.cluster.local:8082/metrics"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"