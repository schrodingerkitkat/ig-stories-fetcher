#!/bin/bash

# Deployment script for Instagram Story Metrics Fetcher
# Deploys Cloud Function with hourly schedule for all accounts

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
PROJECT_ID="cobalt-door-450002-c0"
REGION="us-central1"
FUNCTION_NAME="ig-story-metrics-fetcher"
SERVICE_ACCOUNT="ig-metrics-sa@${PROJECT_ID}.iam.gserviceaccount.com"

# Parse arguments
ENVIRONMENT=${1:-staging}

if [[ "$ENVIRONMENT" != "staging" && "$ENVIRONMENT" != "production" ]]; then
    echo -e "${RED}Invalid environment: $ENVIRONMENT${NC}"
    echo "Usage: $0 [staging|production]"
    exit 1
fi

echo -e "${BLUE}Deploying Instagram Story Metrics Fetcher to $ENVIRONMENT${NC}"

# Set environment-specific values
if [[ "$ENVIRONMENT" == "production" ]]; then
    FUNCTION_NAME="${FUNCTION_NAME}-prod"
    SCHEDULER_NAME="ig-story-metrics-scheduler-prod"
    SCHEDULE="0 * * * *"  # Every hour
    MEMORY="512MB"
    TIMEOUT="540s"  # 9 minutes
else
    FUNCTION_NAME="${FUNCTION_NAME}-staging"
    SCHEDULER_NAME="ig-story-metrics-scheduler-staging"
    SCHEDULE="0 */2 * * *"  # Every 2 hours for staging
    MEMORY="512MB"
    TIMEOUT="540s"
fi

# Check if gcloud is configured
echo -e "\n${YELLOW}Checking gcloud configuration...${NC}"
gcloud config set project $PROJECT_ID

# Check if service account exists
echo -e "\n${YELLOW}Checking service account...${NC}"
if ! gcloud iam service-accounts describe $SERVICE_ACCOUNT --project=$PROJECT_ID &> /dev/null; then
    echo -e "${RED}Service account $SERVICE_ACCOUNT does not exist${NC}"
    echo "Please run setup_secrets.sh first"
    exit 1
fi

# Deploy Cloud Function
echo -e "\n${BLUE}Deploying Cloud Function: $FUNCTION_NAME${NC}"

gcloud functions deploy $FUNCTION_NAME \
    --gen2 \
    --runtime=python311 \
    --region=$REGION \
    --source=. \
    --entry-point=fetch_instagram_story_metrics_batch \
    --memory=$MEMORY \
    --timeout=$TIMEOUT \
    --service-account=$SERVICE_ACCOUNT \
    --set-env-vars="GOOGLE_CLOUD_PROJECT=$PROJECT_ID,GCS_BUCKET=chapala-bronze-bucket,LOG_LEVEL=INFO,DATA_WINDOW_DAYS=1" \
    --trigger-http \
    --allow-unauthenticated \
    --max-instances=10 \
    --min-instances=0

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Cloud Function deployed successfully${NC}"
else
    echo -e "${RED}✗ Cloud Function deployment failed${NC}"
    exit 1
fi

# Get the function URL
FUNCTION_URL=$(gcloud functions describe $FUNCTION_NAME --region=$REGION --format="value(url)")
echo -e "${GREEN}Function URL: $FUNCTION_URL${NC}"

# Create or update Cloud Scheduler job
echo -e "\n${BLUE}Setting up Cloud Scheduler job: $SCHEDULER_NAME${NC}"

# Delete existing job if it exists
gcloud scheduler jobs delete $SCHEDULER_NAME \
    --location=$REGION \
    --quiet &> /dev/null || true

# Create new scheduler job
gcloud scheduler jobs create http $SCHEDULER_NAME \
    --location=$REGION \
    --schedule="$SCHEDULE" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --headers="Content-Type=application/json" \
    --message-body='{"accounts":["NPI","LT","MD","RE","SML"]}' \
    --oidc-service-account-email=$SERVICE_ACCOUNT \
    --time-zone="America/Los_Angeles" \
    --attempt-deadline="540s" \
    --max-retry-attempts=3 \
    --min-backoff="30s"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Cloud Scheduler job created${NC}"
else
    echo -e "${RED}✗ Cloud Scheduler job creation failed${NC}"
    exit 1
fi

# Test the function
echo -e "\n${BLUE}Testing the deployed function...${NC}"
echo "Sending test request for NPI account..."

TEST_RESPONSE=$(curl -s -X POST $FUNCTION_URL \
    -H "Content-Type: application/json" \
    -d '{"accounts":["NPI"]}' \
    --max-time 30)

if echo "$TEST_RESPONSE" | grep -q "status"; then
    echo -e "${GREEN}✓ Function test successful${NC}"
    echo "Response: $TEST_RESPONSE" | head -n 5
else
    echo -e "${YELLOW}⚠ Function test returned unexpected response${NC}"
    echo "Response: $TEST_RESPONSE"
fi

# Summary
echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "Environment: $ENVIRONMENT"
echo "Function: $FUNCTION_NAME"
echo "Region: $REGION"
echo "Schedule: $SCHEDULE"
echo "Memory: $MEMORY"
echo "Timeout: $TIMEOUT"
echo "URL: $FUNCTION_URL"
echo ""
echo "The story metrics fetcher will run automatically on schedule."
echo "Stories are only available for 24 hours, so hourly runs are important."
echo ""
echo "To trigger manually:"
echo "  gcloud scheduler jobs run $SCHEDULER_NAME --location=$REGION"
echo ""
echo "To view logs:"
echo "  gcloud functions logs read $FUNCTION_NAME --region=$REGION"
echo ""
echo "To check scheduler status:"
echo "  gcloud scheduler jobs describe $SCHEDULER_NAME --location=$REGION"