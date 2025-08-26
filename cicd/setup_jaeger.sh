kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.2/cert-manager.yaml


sleep 40

kubectl apply -f https://github.com/jaegertracing/jaeger-operator/releases/latest/download/jaeger-operator.yaml -n observability

echo "Waiting for Jaeger Operator to be ready..."
kubectl rollout status deployment/jaeger-operator -n observability --timeout=180s

kubectl wait --for=condition=available --timeout=300s deployment/jaeger-operator -n observability