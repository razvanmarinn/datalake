#!/bin/bash

echo "--- 🧹 Resetting Environment ---"
rm -rf /Users/marinrazvan/Developer/datalake/datalake_data
mkdir -p /Users/marinrazvan/Developer/datalake/datalake_data
minikube delete
minikube start --mount --mount-string="/Users/marinrazvan/Developer/datalake/datalake_data:/datalake/data" --memory 8192 --cpus 4

eval $(minikube docker-env)

echo "--- 📦 Creating Namespaces ---"
kubectl create ns observability
kubectl create ns kafka
kubectl create ns argocd
kubectl create ns datalake
kubectl config set-context --current --namespace=datalake

echo "--- 🐘 Installing Kafka (Strimzi) ---"
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka


echo "--- 🔐 Generating Secrets ---"
./generate_secrets.sh
kubectl apply -f k8s/infra/rbac.yaml
kubectl apply -f k8s/infra/kafka-deployment.yml -n kafka
if [ "$1" == "argo=true" ]; then
    echo "--- 🐙 Initiating ArgoCD deployment workflow ---"

    echo "Applying ArgoCD manifests..."
    kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.5.8/manifests/install.yaml

    echo "Waiting for ArgoCD to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/argocd-server -n argocd

    pw=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)

    kubectl port-forward svc/argocd-server -n argocd 8080:443 > /dev/null 2>&1 &
    PORT_FORWARD_PID=$!


    ./setup_jaeger.sh
    ./setup_monitoring.sh

    echo "Logging in to ArgoCD..."
    argocd login localhost:8080 --username admin --password "$pw" --insecure

    echo "Creating datalake application in ArgoCD..."
    argocd app create datalake \
      --repo https://github.com/razvanmarinn/datalake \
      --path k8s/plain-yaml \
      --dest-server https://kubernetes.default.svc \
      --dest-namespace datalake \
      --sync-policy none

    echo "--- ArgoCD setup complete! ---"
    echo "ArgoCD UI: https://localhost:8080 (admin / $pw)"
    echo "⚠️  Auto-Sync is DISABLED so you can use Skaffold."
else
    ./setup_monitoring.sh
fi

echo ""
echo "✅ Environment Ready!"
echo "🚀 NEXT STEP: Run 'skaffold dev' to build and deploy your app."
