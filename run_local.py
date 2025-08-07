#!/usr/bin/env python3
"""
Local runner script for Instagram Story Metrics Fetcher.
Tests real API calls and GCS uploads for all accounts.
"""
import os
import sys
import json
from pathlib import Path
from datetime import datetime

# Set local execution mode
os.environ["EXECUTION_MODE"] = "local"

# Use the standard credentials location
credentials_path = Path.home() / "Documents/Scripting/NPI/IG/gk/cobalt-door-450002-c0-f433e6e20ca9.json"
if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and credentials_path.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(credentials_path)

# Set project ID if not set
if not os.environ.get("GOOGLE_CLOUD_PROJECT"):
    os.environ["GOOGLE_CLOUD_PROJECT"] = "cobalt-door-450002-c0"

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

# Import the story fetcher
from instagram_story_metrics import InstagramStoryMetricsFetcher

# All Instagram accounts
ACCOUNTS = ["NPI", "LT", "MD", "RE", "SML"]

def test_single_account(account_name):
    """Test fetching and uploading for a single account"""
    print(f"\n{'='*60}")
    print(f"Testing account: {account_name}")
    print(f"{'='*60}")
    
    try:
        # Initialize fetcher
        project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        fetcher = InstagramStoryMetricsFetcher(account_name, project_id)
        
        # Verify token
        print("Verifying API token...")
        if not fetcher.verify_token_scopes():
            print(f"❌ Token verification failed for {account_name}")
            return False
        print("✓ Token verified")
        
        # Get eligible date range for stories
        start_date, end_date = fetcher.get_eligible_story_date_range()
        print(f"Fetching stories from {start_date} to {end_date}")
        print(f"(Metrics only available for stories posted in last 24 hours)")
        
        # Fetch stories from API
        stories = fetcher.fetch_stories(start_date, end_date)
        
        if not stories:
            print(f"⚠️  No stories found for {account_name} in date range")
            print("   This is normal if no stories were posted 24-48 hours ago")
            return True  # Not an error, just no data
        
        print(f"✓ Found {len(stories)} stories")
        
        # Process stories
        print("Processing story metrics...")
        df = fetcher.process_stories(stories)
        
        if df.is_empty():
            print(f"⚠️  No metrics available for stories")
            return True
        
        print(f"✓ Processed {len(df)} stories with metrics")
        
        # Show sample data
        print("\nSample story data:")
        print(f"  Story IDs: {df['story_id'].head(3).to_list()}")
        print(f"  Views: {df['Views'].head(3).to_list()}")
        print(f"  Reach: {df['Reach'].head(3).to_list()}")
        print(f"  Navigation Total: {df['Navigation Total'].head(3).to_list()}")
        print(f"  Total Interactions: {df['Total Interactions'].head(3).to_list()}")
        
        # Upload to GCS
        print("\nUploading to GCS...")
        today = datetime.now().date()
        fetcher.upload_to_gcs(df, today)
        
        # Verify upload
        bucket_name = fetcher.bucket_name
        parquet_path = f"Instagram/{account_name}/insights/stories/{today}/instagram_story_metrics_{today}.parquet"
        print(f"✓ Uploaded to: gs://{bucket_name}/{parquet_path}")
        
        # Try to verify the file exists
        try:
            bucket = fetcher.gcs_client.get_bucket(bucket_name)
            blob = bucket.blob(parquet_path)
            if blob.exists():
                blob.reload()
                print(f"✓ Verified file in GCS (size: {blob.size:,} bytes)")
            else:
                print("⚠️  Could not verify file in GCS")
        except Exception as e:
            print(f"⚠️  Could not verify GCS upload: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error processing {account_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main test execution"""
    print("\n" + "="*70)
    print("Instagram Story Metrics Fetcher - API & GCS Test")
    print("="*70)
    
    # Check environment
    print("\nEnvironment:")
    print(f"  Project: {os.environ.get('GOOGLE_CLOUD_PROJECT')}")
    print(f"  Credentials: {os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')}")
    
    import argparse
    parser = argparse.ArgumentParser(description='Test Instagram story metrics fetcher')
    parser.add_argument('accounts', nargs='*', help='Accounts to test (default: all)')
    parser.add_argument('--all', action='store_true', help='Test all accounts')
    
    args = parser.parse_args()
    
    # Determine which accounts to test
    if args.all or not args.accounts:
        accounts_to_test = ACCOUNTS
        print(f"\nTesting all accounts: {', '.join(accounts_to_test)}")
    else:
        accounts_to_test = [acc.upper() for acc in args.accounts]
        print(f"\nTesting accounts: {', '.join(accounts_to_test)}")
    
    # Test each account
    results = {}
    for account in accounts_to_test:
        if account not in ACCOUNTS:
            print(f"\n⚠️  Skipping invalid account: {account}")
            continue
        
        success = test_single_account(account)
        results[account] = success
    
    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    successful = [acc for acc, success in results.items() if success]
    failed = [acc for acc, success in results.items() if not success]
    
    if successful:
        print(f"✓ Successful: {', '.join(successful)}")
    if failed:
        print(f"❌ Failed: {', '.join(failed)}")
    
    # Overall status
    if not failed:
        print("\n✅ All accounts processed successfully!")
        print("\nNext steps:")
        print("1. Verify the data in GCS looks correct")
        print("2. Set up Cloud Function deployment")
        print("3. Configure Cloud Scheduler for hourly runs")
        return 0
    else:
        print(f"\n⚠️  {len(failed)} account(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())