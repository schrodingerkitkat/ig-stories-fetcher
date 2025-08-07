# Instagram Story Metrics Fetcher

A cost-optimized, serverless pipeline for fetching Instagram story metrics and storing them in Google Cloud Storage. Stories have metrics available for only 24 hours after posting, making hourly collection critical.

## Overview

This Cloud Function fetches story metrics for 5 Instagram business accounts (NPI, LT, MD, RE, SML) and stores them in GCS as Parquet files. It's designed to run hourly to capture metrics before they expire.

## Key Features

- **Hourly Execution**: Captures story metrics within 24-hour availability window
- **Multi-Account Support**: Processes all 5 accounts in parallel
- **Cost Optimized**: 
  - Serverless (Cloud Functions) - no idle costs
  - 512MB memory allocation
  - Efficient Polars data processing
  - Automatic scaling (0-10 instances)
- **OIDC Authentication**: Secure GitHub Actions deployment without service account keys
- **Navigation Breakdown**: Captures detailed story navigation metrics (taps, swipes, exits)

## Architecture

```
Cloud Scheduler (hourly)
    ↓
Cloud Function (ig-story-metrics-fetcher)
    ↓
Instagram Graph API
    ↓
Process with Polars
    ↓
Store in GCS (Parquet format)
```

## Metrics Collected

- **Views**: Total story views
- **Reach**: Unique users reached
- **Navigation**: Total navigation actions with breakdown:
  - Taps Forward
  - Taps Back
  - Taps Exit
  - Swipe Forward
- **Engagement**:
  - Replies
  - Shares
  - Total Interactions
  - Profile Visits
  - Follows

## Setup

### Prerequisites

1. Google Cloud Project with billing enabled
2. Facebook App with Instagram Business accounts connected
3. Long-lived Facebook access token (60+ days)

### Local Development

```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export GOOGLE_CLOUD_PROJECT="cobalt-door-450002-c0"
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"

# Test single account
python run_local.py NPI

# Test all accounts
python run_local.py --all
```

### Deployment

#### 1. Set up OIDC for GitHub Actions

```bash
chmod +x deployment/setup_github_wif.sh
./deployment/setup_github_wif.sh
```

Add the generated secrets to your GitHub repository:
- `WIF_PROVIDER`
- `WIF_SERVICE_ACCOUNT`

#### 2. Deploy to Staging

```bash
chmod +x deployment/deploy.sh
./deployment/deploy.sh staging
```

- Runs every 2 hours
- Processes all 5 accounts

#### 3. Deploy to Production

```bash
./deployment/deploy.sh production
```

- Runs every hour (critical for 24-hour window)
- Processes all 5 accounts

## GCS Output Structure

```
gs://chapala-bronze-bucket/
└── Instagram/
    └── {ACCOUNT}/
        ├── insights/
        │   └── stories/
        │       └── {date}/
        │           └── instagram_story_metrics_{date}.parquet
        └── schemas/
            └── stories/
                └── {date}/
                    └── instagram_story_metrics_{date}_schema.json
```

## Monitoring

### View Logs

```bash
# Staging logs
gcloud functions logs read ig-story-metrics-fetcher-staging \
  --region=us-central1 --limit=50

# Production logs
gcloud functions logs read ig-story-metrics-fetcher-prod \
  --region=us-central1 --limit=50
```

### Check Scheduler Status

```bash
# View scheduler job
gcloud scheduler jobs describe ig-story-metrics-scheduler-prod \
  --location=us-central1

# View recent runs
gcloud scheduler jobs list --location=us-central1
```

### Manual Trigger

```bash
# Trigger scheduler manually
gcloud scheduler jobs run ig-story-metrics-scheduler-prod \
  --location=us-central1

# Or call function directly
curl -X POST https://[FUNCTION_URL] \
  -H "Content-Type: application/json" \
  -d '{"accounts":["NPI"]}'
```

## Cost Optimization

- **Serverless**: No idle compute costs
- **Hourly Schedule**: Minimum runs needed for 24-hour window
- **512MB Memory**: Optimized for story data volume
- **Parallel Processing**: All accounts in one execution
- **Efficient Storage**: Parquet format with compression

Estimated monthly cost: < $5 for hourly execution

## API Limitations

- Story metrics expire after 24 hours
- Navigation breakdown may require additional permissions
- Replies from EU/Japan users are not counted (privacy regulations)
- Stories with < 5 viewers may not return metrics

## Troubleshooting

### No Stories Found
- Normal if no stories posted in last 24 hours
- Check if accounts are actively posting stories

### Missing Navigation Breakdown
- API may return total navigation without breakdown
- Consider webhook setup for real-time metrics

### Rate Limiting
- Function includes retry logic with exponential backoff
- Processes accounts sequentially to avoid rate limits

## Development

### Running Tests

```bash
pytest tests/ -v --cov=src
```

### Code Quality

```bash
ruff check src/
mypy src/ --ignore-missing-imports
black src/
```

## License

Proprietary - Internal use only