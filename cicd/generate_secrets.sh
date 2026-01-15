#!/bin/bash
set -e

# Define paths
JWT_CERTS_DIR="../pkg/jwt/manager/certs"
PRIVATE_KEY="$JWT_CERTS_DIR/private.pem"
PUBLIC_KEY="$JWT_CERTS_DIR/public.pem"

echo "Generating/Updating Kubernetes Secrets..."

# 1. JWT Secrets
if [[ -f "$PRIVATE_KEY" && -f "$PUBLIC_KEY" ]]; then
    echo "Found JWT keys in $JWT_CERTS_DIR. Creating 'jwt-secrets'..."
    kubectl create secret generic jwt-secrets \
        --from-file=private.pem="$PRIVATE_KEY" \
        --from-file=public.pem="$PUBLIC_KEY" \
        --dry-run=client -o yaml | kubectl apply -f -
else
    echo "ERROR: JWT keys not found in $JWT_CERTS_DIR. Please generate them first."
    exit 1
fi

# 2. Identity Service DB Secrets
echo "Creating 'identity-postgres-secret'..."
#IDENTITY_DB_PASS=$(openssl rand -base64 12)
IDENTITY_DB_PASS="identitypassword"
IDENTITY_DB_USER="identityuser"

kubectl create secret generic identity-postgres-secret \
    --from-literal=postgres-password="$IDENTITY_DB_PASS" \
    --from-literal=postgres-user="$IDENTITY_DB_USER" \
    --dry-run=client -o yaml | kubectl apply -f -

# 3. Metadata Service DB Secrets
echo "Creating 'metadata-postgres-secret'..."
METADATA_DB_PASS="metadatapassword"
METADATA_DB_USER="metadatauser"

kubectl create secret generic metadata-postgres-secret \
    --from-literal=postgres-password="$METADATA_DB_PASS" \
    --from-literal=postgres-user="$METADATA_DB_USER" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "Secrets generated successfully."
