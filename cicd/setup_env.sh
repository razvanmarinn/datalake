#!/bin/bash
rm -rf /Users/marinrazvan/Developer/datalake/datalake_data
mkdir /Users/marinrazvan/Developer/datalake/datalake_data
minikube delete
minikube start --mount --mount-string="/Users/marinrazvan/Developer/datalake/datalake_data:/datalake/data" --memory 8192

  eval $(minikube docker-env)


  kubectl create ns observability
  kubectl create ns kafka
  kubectl create ns argocd
  kubectl create ns datalake
  kubectl config set-context --current --namespace=datalake
  kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
  make apply-rbac
  if [ "$1" == "rebuild-images=true" ]; then
    make
    sleep 10

  else
    echo "Skipping image build. To rebuild images, run with 'rebuild-images=true'"
  fi

if [ "$2" == "argo=true" ]; then
    echo "--- Initiating ArgoCD deployment workflow ---"
    echo "Creating Kubernetes namespaces..."
    kubectl create ns argocd



    echo "Applying ArgoCD manifests..."
    kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.5.8/manifests/install.yaml


    echo "Waiting for ArgoCD to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/argocd-server -n argocd


    pw=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)
    echo "ArgoCD admin password: $pw"

    echo "Setting up port forwarding to ArgoCD server..."
    kubectl port-forward svc/argocd-server -n argocd 8080:443 &
    PORT_FORWARD_PID=$!

    sleep 5



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
      --sync-policy automated \
      --auto-prune \
      --self-heal


    echo "--- ArgoCD setup complete! ---"
    echo "ArgoCD UI available at: https://localhost:8080"
    echo "Username: admin"
    echo "Password: $pw"
    echo ""
    echo "To stop port forwarding later, run: kill $PORT_FORWARD_PID"
else
    echo "--- Initiating direct kubectl apply workflow ---"
    echo "Applying all YAML files in k8s/plain-yaml directly..."
    kubectl apply -f ../k8s/plain-yaml/
    ./setup_monitoring.sh

    echo "--- Direct kubectl apply setup complete! ---"
fi

alias log-qs='kubectl logs -l app.kubernetes.io/name=query-service -n datalake -f'
alias log-ag='kubectl logs -l app.kubernetes.io/name=api-gateway -n datalake -f'
alias log-id='kubectl logs -l app=identity-service -n datalake -f'
alias log-meta='kubectl logs -l app=metadata-service -n datalake -f'
alias log-master='kubectl logs -l app=master -n datalake -f'
alias log-worker='kubectl logs -l app=worker -n datalake -f'
alias log-cons='kubectl logs -l app=ingestion-consumer -n datalake -f'
alias log-si='kubectl logs -l app=streaming-ingestion -n datalake -f'
alias log-comp='kubectl logs -l app=compactor -n datalake -f'
alias log-kafka='kubectl logs -l strimzi.io/cluster=my-kafka-cluster -n kafka -f'
source setup_env.sh
