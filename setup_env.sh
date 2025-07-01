#!/bin/bash


minikube delete
minikube start
eval $(minikube docker-env)
make

sleep 10

kubectl create ns observability
kubectl create ns kafka
kubectl create ns argocd
kubectl create ns datalake

kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.5.8/manifests/install.yaml

kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.12.2/cert-manager.yaml


sleep 40


kubectl apply -f https://github.com/jaegertracing/jaeger-operator/releases/latest/download/jaeger-operator.yaml -n observability



echo "Waiting for ArgoCD to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/argocd-server -n argocd

pw=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)
echo "ArgoCD admin password: $pw"

echo "Setting up port forwarding to ArgoCD server..."
kubectl port-forward svc/argocd-server -n argocd 8080:443 &
PORT_FORWARD_PID=$!


sleep 5

echo "Logging in to ArgoCD..."
argocd login localhost:8080 --username admin --password "$pw" --insecure


echo "Creating datalake application in ArgoCD..."
argocd app create datalake \
  --repo https://github.com/razvanmarinn/datalake \
  --path k8s/plain-yaml \
  --dest-server https://kubernetes.default.svc \
  --dest-namespace datalake \
  --sync-policy automated \
  --auto-prune \
  --self-heal

echo "Syncing datalake application..."
argocd app sync datalake

echo "Application status:"
argocd app get datalake


echo "Setup complete!"
echo "ArgoCD UI available at: https://localhost:8080"
echo "Username: admin"
echo "Password: $pw"
echo ""
echo "To stop port forwarding later, run: kill $PORT_FORWARD_PID"

make port-forward
