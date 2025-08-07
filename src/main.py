#!/usr/bin/env python3
"""
Cloud Function entry points for Instagram Story Metrics Fetcher
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Any, List, Tuple

from instagram_story_metrics import main as fetch_story_metrics

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Supported Instagram accounts
SUPPORTED_ACCOUNTS = ["NPI", "LT", "MD", "RE", "SML"]


def fetch_instagram_story_metrics_batch(request) -> Tuple[Dict[str, Any], int]:
    """
    Cloud Function entry point for batch processing multiple accounts
    """
    try:
        # Parse request
        request_json = request.get_json(silent=True) or {}
        accounts = request_json.get("accounts", [])
        local_file = request_json.get("local_file")
        
        # If no accounts specified, use default
        if not accounts:
            accounts = [os.getenv("IG_ACCOUNT_NAME", "NPI")]
        
        # Validate accounts
        invalid_accounts = [acc for acc in accounts if acc.upper() not in SUPPORTED_ACCOUNTS]
        if invalid_accounts:
            logger.warning(f"Invalid accounts requested: {invalid_accounts}")
        
        valid_accounts = [acc for acc in accounts if acc.upper() in SUPPORTED_ACCOUNTS]
        
        if not valid_accounts:
            return {
                "status": "error",
                "error": "No valid accounts specified",
                "supported_accounts": SUPPORTED_ACCOUNTS
            }, 400
        
        # Process accounts
        results = []
        if len(valid_accounts) == 1:
            # Single account - process directly
            result = fetch_story_metrics(valid_accounts[0], local_file)
            results.append(result)
        else:
            # Multiple accounts - process in parallel
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {
                    executor.submit(fetch_story_metrics, account, local_file): account
                    for account in valid_accounts
                }
                
                for future in as_completed(futures):
                    account = futures[future]
                    try:
                        result = future.result(timeout=300)
                        results.append(result)
                    except Exception as e:
                        logger.error(f"Failed to process account {account}: {str(e)}")
                        results.append({
                            "status": "error",
                            "account": account,
                            "error": str(e)
                        })
        
        # Aggregate results
        total_stories = sum(r.get("stories_processed", 0) for r in results)
        failed_accounts = [r["account"] for r in results if r["status"] == "error"]
        
        response = {
            "status": "success" if not failed_accounts else "partial_success",
            "accounts_processed": len(valid_accounts),
            "total_stories": total_stories,
            "results": results
        }
        
        if failed_accounts:
            response["failed_accounts"] = failed_accounts
        
        status_code = 200 if not failed_accounts else 207  # 207 = Multi-Status
        return response, status_code
        
    except Exception as e:
        logger.error(f"Batch processing error: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }, 500


def fetch_all_story_accounts(request) -> Tuple[Dict[str, Any], int]:
    """
    Cloud Function entry point to process all configured accounts
    """
    try:
        # Process all supported accounts
        request_json = request.get_json(silent=True) or {}
        request_json["accounts"] = SUPPORTED_ACCOUNTS
        
        return fetch_instagram_story_metrics_batch(request)
        
    except Exception as e:
        logger.error(f"Error processing all accounts: {str(e)}")
        return {
            "status": "error",
            "error": str(e)
        }, 500


def health_check(request) -> Tuple[Dict[str, Any], int]:
    """
    Health check endpoint for monitoring
    """
    try:
        # Basic health check
        health_status = {
            "status": "healthy",
            "service": "instagram-story-metrics-fetcher",
            "timestamp": datetime.utcnow().isoformat(),
            "supported_accounts": SUPPORTED_ACCOUNTS,
            "environment": {
                "project_id": os.getenv("GOOGLE_CLOUD_PROJECT", "not_set"),
                "bucket": os.getenv("GCS_BUCKET", "chapala-bronze-bucket"),
                "log_level": os.getenv("LOG_LEVEL", "INFO"),
            }
        }
        
        return health_status, 200
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return {
            "status": "unhealthy",
            "error": str(e)
        }, 500


# For local testing
if __name__ == "__main__":
    import sys
    
    class MockRequest:
        def __init__(self, json_data=None):
            self.json_data = json_data or {}
        
        def get_json(self, silent=False):
            return self.json_data
    
    # Test with single account
    if len(sys.argv) > 1:
        account = sys.argv[1]
        request = MockRequest({"accounts": [account]})
    else:
        # Test with all accounts
        request = MockRequest({"accounts": SUPPORTED_ACCOUNTS})
    
    result, status = fetch_instagram_story_metrics_batch(request)
    print(f"Status Code: {status}")
    print(json.dumps(result, indent=2))