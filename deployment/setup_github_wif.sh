#!/bin/bash

# Setup Workload Identity Federation for GitHub Actions
# This enables OIDC authentication without service account keys

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="cobalt-door-450002-c0"
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
POOL_NAME="github-actions-pool"
PROVIDER_NAME="github-provider"
SERVICE_ACCOUNT_NAME="ig-metrics-sa"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# GitHub repository details - UPDATE THESE
GITHUB_ORG="your-github-org"
GITHUB_REPO="ig-stories-fetcher"

echo -e "${BLUE}Setting up Workload Identity Federation for GitHub Actions${NC}"
echo "Project: $PROJECT_ID"
echo "Project Number: $PROJECT_NUMBER"

# Enable required APIs
echo -e "\n${YELLOW}Enabling required APIs...${NC}"
gcloud services enable iamcredentials.googleapis.com \
    iam.googleapis.com \
    cloudresourcemanager.googleapis.com \
    sts.googleapis.com \
    --project=$PROJECT_ID

# Check if workload identity pool exists
echo -e "\n${YELLOW}Checking Workload Identity Pool...${NC}"
if gcloud iam workload-identity-pools describe $POOL_NAME \
    --location=global \
    --project=$PROJECT_ID &> /dev/null; then
    echo -e "${GREEN}✓ Workload Identity Pool already exists${NC}"
else
    echo "Creating Workload Identity Pool..."
    gcloud iam workload-identity-pools create $POOL_NAME \
        --location=global \
        --display-name="GitHub Actions Pool" \
        --description="Pool for GitHub Actions OIDC authentication" \
        --project=$PROJECT_ID
    echo -e "${GREEN}✓ Workload Identity Pool created${NC}"
fi

# Check if provider exists
echo -e "\n${YELLOW}Checking Workload Identity Provider...${NC}"
if gcloud iam workload-identity-pools providers describe $PROVIDER_NAME \
    --workload-identity-pool=$POOL_NAME \
    --location=global \
    --project=$PROJECT_ID &> /dev/null; then
    echo -e "${GREEN}✓ Provider already exists${NC}"
else
    echo "Creating Workload Identity Provider..."
    gcloud iam workload-identity-pools providers create-oidc $PROVIDER_NAME \
        --workload-identity-pool=$POOL_NAME \
        --location=global \
        --issuer-uri="https://token.actions.githubusercontent.com" \
        --attribute-mapping="google.subject=assertion.sub,attribute.actor=assertion.actor,attribute.repository=assertion.repository,attribute.repository_owner=assertion.repository_owner" \
        --attribute-condition="assertion.repository_owner == '${GITHUB_ORG}'" \
        --project=$PROJECT_ID
    echo -e "${GREEN}✓ Provider created${NC}"
fi

# Check if service account exists
echo -e "\n${YELLOW}Checking service account...${NC}"
if gcloud iam service-accounts describe $SERVICE_ACCOUNT_EMAIL \
    --project=$PROJECT_ID &> /dev/null; then
    echo -e "${GREEN}✓ Service account already exists${NC}"
else
    echo "Creating service account..."
    gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME \
        --display-name="Instagram Metrics Service Account" \
        --description="Service account for Instagram story metrics fetcher" \
        --project=$PROJECT_ID
    echo -e "${GREEN}✓ Service account created${NC}"
fi

# Grant necessary permissions to service account
echo -e "\n${YELLOW}Granting permissions to service account...${NC}"

# Cloud Functions permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/cloudfunctions.developer" \
    --condition=None

# Cloud Scheduler permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/cloudscheduler.admin" \
    --condition=None

# Storage permissions for GCS
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/storage.objectAdmin" \
    --condition=None

# Secret Manager permissions
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/secretmanager.secretAccessor" \
    --condition=None

# Service Account User permission (needed for deployment)
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SERVICE_ACCOUNT_EMAIL}" \
    --role="roles/iam.serviceAccountUser" \
    --condition=None

echo -e "${GREEN}✓ Permissions granted${NC}"

# Bind service account to workload identity pool
echo -e "\n${YELLOW}Binding service account to Workload Identity Pool...${NC}"

WORKLOAD_IDENTITY_POOL_ID="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_NAME}/providers/${PROVIDER_NAME}"

# Grant the service account impersonation permission
gcloud iam service-accounts add-iam-policy-binding $SERVICE_ACCOUNT_EMAIL \
    --role="roles/iam.workloadIdentityUser" \
    --member="principalSet://iam.googleapis.com/${WORKLOAD_IDENTITY_POOL_ID}/attribute.repository/${GITHUB_ORG}/${GITHUB_REPO}" \
    --project=$PROJECT_ID

echo -e "${GREEN}✓ Service account bound to Workload Identity Pool${NC}"

# Generate the provider resource name
WIF_PROVIDER="projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL_NAME}/providers/${PROVIDER_NAME}"

# Summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Setup Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}Add these secrets to your GitHub repository:${NC}"
echo ""
echo "WIF_PROVIDER:"
echo "$WIF_PROVIDER"
echo ""
echo "WIF_SERVICE_ACCOUNT:"
echo "$SERVICE_ACCOUNT_EMAIL"
echo ""
echo -e "${BLUE}To add these secrets to GitHub, run:${NC}"
echo "gh secret set WIF_PROVIDER --body=\"$WIF_PROVIDER\""
echo "gh secret set WIF_SERVICE_ACCOUNT --body=\"$SERVICE_ACCOUNT_EMAIL\""
echo ""
echo -e "${YELLOW}Important: Update the GITHUB_ORG and GITHUB_REPO variables in this script${NC}"
echo "Current values:"
echo "  GITHUB_ORG: $GITHUB_ORG"
echo "  GITHUB_REPO: $GITHUB_REPO"