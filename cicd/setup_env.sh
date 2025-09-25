#!/bin/bash

  minikube delete
  minikube start --mount --mount-string="/Users/marinrazvan/Developer/datalake/worker_data:/datalake/worker_data"

  eval $(minikube docker-env)
    
    
  kubectl create ns observability
  kubectl create ns kafka
  kubectl create ns argocd
  kubectl create ns datalake
  if [ "$1" == "rebuild-images=true" ]; then
    make
    sleep 10

  else
    echo "Skipping image build. To rebuild images, run with 'rebuild-images=true'"
  fi

# Check if the first argument is "argo=true" to determine the workflow
if [ "$1" == "argo=true" ]; then
    echo "--- Initiating ArgoCD deployment workflow ---"
    echo "Creating Kubernetes namespaces..."
    kubectl create ns argocd


    # Apply the ArgoCD installation manifests
    echo "Applying ArgoCD manifests..."
    kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/v2.5.8/manifests/install.yaml

    # Wait for the ArgoCD server deployment to be ready
    echo "Waiting for ArgoCD to be ready..."
    kubectl wait --for=condition=available --timeout=300s deployment/argocd-server -n argocd

    # Get the initial admin password for ArgoCD
    pw=$(kubectl -n argocd get secret argocd-initial-admin-secret -o jsonpath="{.data.password}" | base64 -d)
    echo "ArgoCD admin password: $pw"

    # Set up port forwarding to access the ArgoCD UI
    echo "Setting up port forwarding to ArgoCD server..."
    kubectl port-forward svc/argocd-server -n argocd 8080:443 &
    PORT_FORWARD_PID=$!

    sleep 5



    ./setup_jaeger.sh
    # Log in to ArgoCD using the initial credentials
    echo "Logging in to ArgoCD..."
    argocd login localhost:8080 --username admin --password "$pw" --insecure

    # Create the 'datalake' application in ArgoCD
    echo "Creating datalake application in ArgoCD..."
    argocd app create datalake \
      --repo https://github.com/razvanmarinn/datalake \
      --path k8s/plain-yaml \
      --dest-server https://kubernetes.default.svc \
      --dest-namespace datalake \
      --sync-policy automated \
      --auto-prune \
      --self-heal

    # Force a sync of the application to deploy the resources
    # echo "Syncing datalake application..."
    # argocd app sync datalake

    # # Display the status of the application
    # echo "Application status:"
    # argocd app get datalake

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
    
    echo "--- Direct kubectl apply setup complete! ---"
fi

