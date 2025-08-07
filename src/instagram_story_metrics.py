#!/usr/bin/env python3
"""
Instagram Story Metrics Fetcher - Modernized Cloud-Native Implementation
Fetches Instagram story metrics from Facebook Graph API and stores in GCS
Note: Story metrics are only available for 24 hours after posting, then expire
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple, Optional
from functools import lru_cache

import polars as pl
import pytz
import requests
from google.cloud import storage, secretmanager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configure structured logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# Constants
FB_API_VERSION = "v21.0"
FB_API_BASE_URL = f"https://graph.facebook.com/{FB_API_VERSION}"
PST_TIMEZONE = pytz.timezone("US/Pacific")

# Story metrics are only available for 24 hours after posting
STORY_METRICS_WINDOW_HOURS = 24
DATA_WINDOW_DAYS = int(os.getenv("DATA_WINDOW_DAYS", "1"))  # Stories expire after 24h
BATCH_SIZE = 100  # Facebook API limit


class InstagramAPIError(Exception):
    """Custom exception for Instagram API errors"""
    pass


class SecretsManager:
    """Manages secrets from Google Secret Manager"""

    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = secretmanager.SecretManagerServiceClient()
        self._cache = {}

    @lru_cache(maxsize=10)
    def get_secret(self, secret_id: str, version: str = "latest") -> str:
        """Fetch secret from Google Secret Manager with caching"""
        cache_key = f"{secret_id}:{version}"

        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            name = f"projects/{self.project_id}/secrets/{secret_id}/versions/{version}"
            response = self.client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode("UTF-8").strip()
            self._cache[cache_key] = secret_value
            logger.info(f"Successfully retrieved secret: {secret_id}")
            return secret_value
        except Exception as e:
            logger.error(f"Failed to retrieve secret {secret_id}: {str(e)}")
            raise


class InstagramStoryMetricsFetcher:
    """Main class for fetching Instagram story metrics"""

    def __init__(self, account_name: str, project_id: str):
        self.account_name = account_name.upper()
        self.project_id = project_id
        self.secrets_manager = SecretsManager(project_id)

        # Set up HTTP session with retry logic
        self.session = requests.Session()
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

        # Initialize credentials
        self._init_credentials()

        # Initialize GCS client
        self.gcs_client = storage.Client(project=project_id)
        self.bucket_name = os.getenv("GCS_BUCKET", "chapala-bronze-bucket")

    def _init_credentials(self):
        """Initialize credentials from Secret Manager"""
        # Fetch access token (shared across all IG accounts)
        self.access_token = self.secrets_manager.get_secret("fb_access_token")

        # Fetch business ID for specific account
        business_id_secret = f"ig_business_id_{self.account_name.lower()}"
        self.business_id = self.secrets_manager.get_secret(business_id_secret)

        logger.info(f"Initialized credentials for account: {self.account_name}")

    def verify_token_scopes(self) -> bool:
        """Verify that the access token has required permissions"""
        required_scopes = [
            "instagram_basic",
            "instagram_manage_insights",
            "pages_read_engagement",
        ]

        try:
            endpoint = f"{FB_API_BASE_URL}/debug_token"
            params = {
                "input_token": self.access_token,
                "access_token": self.access_token,
            }

            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()

            data = response.json().get("data", {})
            token_scopes = set(data.get("scopes", []))
            missing_scopes = set(required_scopes) - token_scopes

            if missing_scopes:
                logger.error(f"Missing required scopes: {missing_scopes}")
                return False

            logger.info("Token scopes verified successfully")
            return True

        except Exception as e:
            logger.error(f"Failed to verify token scopes: {str(e)}")
            return False

    def get_eligible_story_date_range(self) -> Tuple[datetime.date, datetime.date]:
        """
        Calculate date range for stories with available metrics.
        Story metrics are only available for 24 hours after posting.
        """
        now = datetime.now(PST_TIMEZONE)
        
        # Stories from the last 24 hours have metrics available
        end_datetime = now
        end_date = end_datetime.date()
        
        # Start date is 24 hours ago (metrics expire after 24 hours)
        start_datetime = now - timedelta(hours=STORY_METRICS_WINDOW_HOURS)
        start_date = start_datetime.date()
        
        logger.info(f"Fetching stories with available metrics from {start_date} to {end_date}")
        logger.info(f"(Stories posted in the last {STORY_METRICS_WINDOW_HOURS} hours)")
        return start_date, end_date

    def fetch_stories(
        self, since_date: datetime.date, until_date: datetime.date
    ) -> List[Dict[str, Any]]:
        """Fetch Instagram stories within date range"""
        stories = []
        # First try the stories endpoint, then fall back to media endpoint
        endpoint = f"{FB_API_BASE_URL}/{self.business_id}/stories"

        params = {
            "access_token": self.access_token,
            "fields": "id,timestamp,media_type,permalink,media_url,media_product_type",
            "limit": BATCH_SIZE,
        }

        page_count = 0
        max_pages = 10  # Stories are limited, fewer pages needed

        while endpoint and page_count < max_pages:
            try:
                response = self.session.get(endpoint, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()

                for story in data.get("data", []):
                    # Parse timestamp to date
                    story_timestamp = datetime.strptime(
                        story["timestamp"], "%Y-%m-%dT%H:%M:%S%z"
                    )
                    story_date = story_timestamp.date()

                    # Filter by date range and ensure story is old enough
                    if since_date <= story_date <= until_date:
                        stories.append(story)
                        logger.debug(f"Found eligible story from {story_date}: {story['id']}")
                    elif story_date > until_date:
                        logger.debug(f"Skipping story from {story_date} - too recent for metrics")
                    elif story_date < since_date:
                        # Stories are returned in reverse chronological order
                        logger.info(
                            "Reached stories before start date. Stopping pagination."
                        )
                        return stories

                # Get next page URL
                endpoint = data.get("paging", {}).get("next")
                params = {}  # Next URL includes all params
                page_count += 1

            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching stories: {str(e)}")
                raise InstagramAPIError(f"Failed to fetch stories: {str(e)}")

        logger.info(
            f"Fetched {len(stories)} stories for date range {since_date} to {until_date}"
        )
        return stories

    def fetch_story_metrics(self, story_id: str) -> Dict[str, Any]:
        """Fetch metrics for a single story"""
        metrics = {}
        
        # First fetch navigation with breakdown
        try:
            endpoint = f"{FB_API_BASE_URL}/{story_id}/insights"
            params = {
                "access_token": self.access_token,
                "metric": "navigation",
                "breakdown": "story_navigation_action_type"
            }
            
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Parse navigation breakdown
            navigation_total = 0
            taps_forward = 0
            taps_back = 0
            taps_exit = 0
            swipe_forward = 0
            
            for metric in data.get("data", []):
                if metric.get("name") == "navigation":
                    for value in metric.get("values", []):
                        total_value = value.get("value", {})
                        if isinstance(total_value, dict):
                            # Parse breakdown values
                            for action_type, count in total_value.items():
                                if action_type == "TAP_FORWARD":
                                    taps_forward = count
                                elif action_type == "TAP_BACK":
                                    taps_back = count
                                elif action_type == "TAP_EXIT":
                                    taps_exit = count
                                elif action_type == "SWIPE_FORWARD":
                                    swipe_forward = count
                                navigation_total += count
                        else:
                            navigation_total = total_value
            
            metrics["navigation_total"] = navigation_total
            metrics["taps_forward"] = taps_forward
            metrics["taps_back"] = taps_back
            metrics["taps_exit"] = taps_exit
            metrics["swipe_forward"] = swipe_forward
            
        except Exception as e:
            logger.warning(f"Error fetching navigation metrics: {str(e)}")
            metrics.update({
                "navigation_total": 0,
                "taps_forward": 0,
                "taps_back": 0,
                "taps_exit": 0,
                "swipe_forward": 0
            })
        
        # Then fetch other story metrics
        try:
            params = {
                "access_token": self.access_token,
                "metric": "reach,replies,shares,total_interactions,views,profile_visits,follows"
            }
            
            response = self.session.get(endpoint, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Parse other metrics
            for metric in data.get("data", []):
                metric_name = metric.get("name")
                metric_value = metric.get("values", [{}])[0].get("value", 0)
                if metric_name:
                    metrics[metric_name] = metric_value
                    
        except Exception as e:
            logger.warning(f"Error fetching other story metrics: {str(e)}")
            # Add defaults for missing metrics
            for key in ["reach", "replies", "shares", "total_interactions", "views", "profile_visits", "follows"]:
                if key not in metrics:
                    metrics[key] = 0

        return metrics

    def process_stories(self, stories: List[Dict[str, Any]]) -> pl.DataFrame:
        """Process stories and metrics into Polars DataFrame"""
        processed_data = []

        for story in stories:
            # Fetch metrics for story
            metrics = self.fetch_story_metrics(story["id"])

            # Parse story data
            story_timestamp = datetime.strptime(story["timestamp"], "%Y-%m-%dT%H:%M:%S%z")
            pst_timestamp = story_timestamp.astimezone(PST_TIMEZONE)

            # Build row data
            row_data = {
                "story_id": story["id"],
                "Story Date": pst_timestamp.replace(
                    hour=0, minute=0, second=0, microsecond=0
                ),
                "timestamp": pst_timestamp,
                "Media Type": "Story",
                "permalink": story.get("permalink", ""),
                "media_url": story.get("media_url", ""),
                # Core metrics
                "Views": metrics.get("views", 0),
                "Reach": metrics.get("reach", 0),
                "Replies": metrics.get("replies", 0),
                "Shares": metrics.get("shares", 0),
                "Total Interactions": metrics.get("total_interactions", 0),
                "Profile Visits": metrics.get("profile_visits", 0),
                "Follows": metrics.get("follows", 0),
                # Navigation breakdown
                "Navigation Total": metrics.get("navigation_total", 0),
                "Taps Forward": metrics.get("taps_forward", 0),
                "Taps Back": metrics.get("taps_back", 0),
                "Taps Exit": metrics.get("taps_exit", 0),
                "Swipe Forward": metrics.get("swipe_forward", 0),
            }

            # Calculate engagement rates based on views
            if row_data["Views"] > 0:
                row_data["Exit Rate"] = round(
                    (row_data["Taps Exit"] / row_data["Views"]) * 100, 2
                )
                row_data["Reply Rate"] = round(
                    (row_data["Replies"] / row_data["Views"]) * 100, 2
                )
                row_data["Forward Rate"] = round(
                    ((row_data["Taps Forward"] + row_data["Swipe Forward"]) / row_data["Views"]) * 100, 2
                )
                row_data["Back Rate"] = round(
                    (row_data["Taps Back"] / row_data["Views"]) * 100, 2
                )
            else:
                row_data["Exit Rate"] = None
                row_data["Reply Rate"] = None
                row_data["Forward Rate"] = None
                row_data["Back Rate"] = None

            processed_data.append(row_data)

        # Create Polars DataFrame
        if processed_data:
            df = pl.DataFrame(processed_data)

            # Add metric_date column as datetime (midnight)
            metric_datetime = datetime.now().replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            df = df.with_columns(pl.lit(metric_datetime).alias("metric_date"))

            # Cast datetime columns to nanoseconds to match standard format
            df = df.with_columns(
                [
                    pl.col("Story Date")
                    .dt.cast_time_unit("ns")
                    .dt.replace_time_zone(None),
                    pl.col("timestamp")
                    .dt.cast_time_unit("ns")
                    .dt.replace_time_zone(None),
                    pl.col("metric_date").dt.cast_time_unit("ns"),
                ]
            )

            logger.info(f"Processed {len(df)} stories into DataFrame")
            return df
        else:
            # Return empty DataFrame with correct schema
            return self._create_empty_dataframe()

    def _create_empty_dataframe(self) -> pl.DataFrame:
        """Create empty DataFrame with correct schema"""
        schema = {
            "story_id": pl.Utf8,
            "Story Date": pl.Datetime("ns"),
            "timestamp": pl.Datetime("ns"),
            "Media Type": pl.Utf8,
            "permalink": pl.Utf8,
            "media_url": pl.Utf8,
            "Views": pl.Int64,
            "Reach": pl.Int64,
            "Replies": pl.Int64,
            "Shares": pl.Int64,
            "Total Interactions": pl.Int64,
            "Profile Visits": pl.Int64,
            "Follows": pl.Int64,
            "Navigation Total": pl.Int64,
            "Taps Forward": pl.Int64,
            "Taps Back": pl.Int64,
            "Taps Exit": pl.Int64,
            "Swipe Forward": pl.Int64,
            "Exit Rate": pl.Float64,
            "Reply Rate": pl.Float64,
            "Forward Rate": pl.Float64,
            "Back Rate": pl.Float64,
            "metric_date": pl.Datetime("ns"),
        }
        return pl.DataFrame(schema=schema)

    def upload_to_gcs(self, df: pl.DataFrame, date: datetime.date) -> None:
        """Upload DataFrame to GCS as Parquet with schema"""
        if df.is_empty():
            logger.warning("No data to upload - DataFrame is empty")
            return

        try:
            bucket = self.gcs_client.get_bucket(self.bucket_name)

            # Define GCS paths - stories go in a date-partitioned folder
            parquet_key = f"Instagram/{self.account_name}/insights/stories/{date}/instagram_story_metrics_{date}.parquet"
            schema_key = f"Instagram/{self.account_name}/schemas/stories/{date}/instagram_story_metrics_{date}_schema.json"

            # Write Parquet to bytes
            import io

            buffer = io.BytesIO()
            df.write_parquet(buffer)
            parquet_bytes = buffer.getvalue()

            # Upload Parquet file
            blob = bucket.blob(parquet_key)
            blob.upload_from_string(
                parquet_bytes, content_type="application/octet-stream"
            )
            logger.info(
                f"Uploaded Parquet file to: gs://{self.bucket_name}/{parquet_key}"
            )

            # Create and upload schema
            schema_dict = {
                "type": "struct",
                "fields": [
                    {"name": col, "type": str(df[col].dtype)} for col in df.columns
                ],
            }

            schema_blob = bucket.blob(schema_key)
            schema_blob.upload_from_string(
                json.dumps(schema_dict, indent=2), content_type="application/json"
            )
            logger.info(f"Uploaded schema to: gs://{self.bucket_name}/{schema_key}")

        except Exception as e:
            logger.error(f"Failed to upload to GCS: {str(e)}")
            raise

    def fetch_from_local_file(self, file_path: str) -> pl.DataFrame:
        """
        Fetch story data from a local JSON file (for testing/manual processing)
        """
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Process the data as if it came from the API
            if isinstance(data, list):
                stories = data
            elif isinstance(data, dict) and 'data' in data:
                stories = data['data']
            else:
                raise ValueError("Invalid file format - expected list or dict with 'data' key")
            
            logger.info(f"Loaded {len(stories)} stories from local file: {file_path}")
            
            # Process stories into DataFrame
            return self.process_stories(stories)
            
        except Exception as e:
            logger.error(f"Failed to load from local file: {str(e)}")
            raise

    def run(self, local_file: Optional[str] = None) -> Dict[str, Any]:
        """Main execution method"""
        start_time = datetime.now()

        try:
            # If local file provided, use that instead of API
            if local_file:
                logger.info(f"Processing stories from local file: {local_file}")
                df = self.fetch_from_local_file(local_file)
                
                # Upload to GCS with today's date
                today = datetime.now().date()
                self.upload_to_gcs(df, today)
                
                return {
                    "status": "success",
                    "account": self.account_name,
                    "stories_processed": len(df),
                    "source": "local_file",
                    "duration_seconds": (datetime.now() - start_time).total_seconds(),
                }
            
            # Normal API flow
            # Verify token scopes
            if not self.verify_token_scopes():
                raise InstagramAPIError("Token validation failed")

            # Define date range (24-hour delay for metrics availability)
            start_date, end_date = self.get_eligible_story_date_range()

            logger.info(
                f"Fetching story data for {self.account_name} from {start_date} to {end_date}"
            )

            # Fetch stories
            stories = self.fetch_stories(start_date, end_date)

            if not stories:
                logger.warning("No stories found in date range")
                return {
                    "status": "success",
                    "account": self.account_name,
                    "stories_processed": 0,
                    "duration_seconds": (datetime.now() - start_time).total_seconds(),
                }

            # Process stories into DataFrame
            df = self.process_stories(stories)

            # Upload to GCS with today's date
            today = datetime.now().date()
            self.upload_to_gcs(df, today)

            # Calculate execution time
            duration = (datetime.now() - start_time).total_seconds()

            return {
                "status": "success",
                "account": self.account_name,
                "stories_processed": len(df),
                "date_range": f"{start_date} to {end_date}",
                "duration_seconds": duration,
            }

        except Exception as e:
            logger.error(f"Execution failed: {str(e)}")
            return {
                "status": "error",
                "account": self.account_name,
                "error": str(e),
                "duration_seconds": (datetime.now() - start_time).total_seconds(),
            }


def main(account_name: str = None, local_file: str = None) -> Dict[str, Any]:
    """Main entry point for the Instagram story metrics fetcher"""
    # Get account name from parameter or environment
    if not account_name:
        account_name = os.getenv("IG_ACCOUNT_NAME", "NPI")

    # Get project ID
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
    if not project_id:
        # For local testing, use a default or passed value
        project_id = os.getenv("GCP_PROJECT_ID", "your-project-id")
        logger.warning(f"Using project ID: {project_id}")

    logger.info(f"Starting Instagram story metrics fetch for account: {account_name}")

    # Initialize and run fetcher
    fetcher = InstagramStoryMetricsFetcher(account_name, project_id)
    return fetcher.run(local_file=local_file)


# Cloud Function entry point
def fetch_instagram_story_metrics(request) -> Tuple[Dict[str, Any], int]:
    """Cloud Function HTTP entry point"""
    try:
        # Parse request for account name
        request_json = request.get_json(silent=True) or {}
        account_name = request_json.get("account")
        local_file = request_json.get("local_file")

        # Run main process
        result = main(account_name, local_file)

        # Return appropriate status code
        status_code = 200 if result["status"] == "success" else 500
        return result, status_code

    except Exception as e:
        logger.error(f"Cloud Function error: {str(e)}")
        return {"status": "error", "error": str(e)}, 500


if __name__ == "__main__":
    # For local testing
    import argparse
    
    parser = argparse.ArgumentParser(description='Fetch Instagram story metrics')
    parser.add_argument('account', nargs='?', default='NPI', help='Instagram account name')
    parser.add_argument('--local-file', help='Path to local JSON file with story data')
    
    args = parser.parse_args()
    
    result = main(args.account, args.local_file)
    print(json.dumps(result, indent=2))