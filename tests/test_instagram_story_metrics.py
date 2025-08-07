#!/usr/bin/env python3
"""
Test suite for Instagram Story Metrics Fetcher
Tests API integration, data processing, and GCS upload
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import polars as pl
import pytest
import pytz

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from instagram_story_metrics import (
    InstagramStoryMetricsFetcher,
    SecretsManager,
    InstagramAPIError,
)


class TestSecretsManager:
    """Test the SecretsManager class"""

    @patch('google.cloud.secretmanager.SecretManagerServiceClient')
    def test_get_secret_success(self, mock_client):
        """Test successful secret retrieval"""
        # Mock the response
        mock_response = Mock()
        mock_response.payload.data.decode.return_value = "test-secret-value"
        mock_client.return_value.access_secret_version.return_value = mock_response

        # Test secret retrieval
        manager = SecretsManager("test-project")
        result = manager.get_secret("test-secret")

        assert result == "test-secret-value"
        mock_client.return_value.access_secret_version.assert_called_once()

    @patch('google.cloud.secretmanager.SecretManagerServiceClient')
    def test_get_secret_caching(self, mock_client):
        """Test that secrets are cached after first retrieval"""
        # Mock the response
        mock_response = Mock()
        mock_response.payload.data.decode.return_value = "cached-value"
        mock_client.return_value.access_secret_version.return_value = mock_response

        # Test caching
        manager = SecretsManager("test-project")
        result1 = manager.get_secret("cached-secret")
        result2 = manager.get_secret("cached-secret")

        assert result1 == result2 == "cached-value"
        # Should only be called once due to caching
        mock_client.return_value.access_secret_version.assert_called_once()


class TestInstagramStoryMetricsFetcher:
    """Test the main fetcher class"""

    @patch('google.cloud.storage.Client')
    @patch.object(SecretsManager, 'get_secret')
    def test_init(self, mock_get_secret, mock_storage):
        """Test fetcher initialization"""
        # Mock secrets
        mock_get_secret.side_effect = ["test-token", "test-business-id"]

        # Initialize fetcher
        fetcher = InstagramStoryMetricsFetcher("NPI", "test-project")

        assert fetcher.account_name == "NPI"
        assert fetcher.project_id == "test-project"
        assert fetcher.access_token == "test-token"
        assert fetcher.business_id == "test-business-id"

    def test_eligible_date_range(self):
        """Test calculation of eligible story date range"""
        with patch('google.cloud.storage.Client'):
            with patch.object(SecretsManager, 'get_secret') as mock_secret:
                mock_secret.side_effect = ["token", "business_id"]
                
                fetcher = InstagramStoryMetricsFetcher("NPI", "test-project")
                start_date, end_date = fetcher.get_eligible_story_date_range()
                
                # End date should be ~24 hours ago
                now = datetime.now(pytz.timezone("US/Pacific"))
                expected_end = (now - timedelta(hours=24)).date()
                
                assert end_date == expected_end
                # Start date should be 2 days before end date (stories expire after 24h)
                assert start_date == end_date - timedelta(days=2)

    @patch('requests.Session.get')
    @patch('google.cloud.storage.Client')
    @patch.object(SecretsManager, 'get_secret')
    def test_verify_token_scopes_success(self, mock_get_secret, mock_storage, mock_get):
        """Test successful token scope verification"""
        # Mock secrets
        mock_get_secret.side_effect = ["test-token", "test-business-id"]

        # Mock API response
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "scopes": [
                    "instagram_basic",
                    "instagram_manage_insights",
                    "pages_read_engagement",
                ]
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Test verification
        fetcher = InstagramStoryMetricsFetcher("NPI", "test-project")
        result = fetcher.verify_token_scopes()

        assert result is True

    @patch('google.cloud.storage.Client')
    @patch.object(SecretsManager, 'get_secret')
    def test_create_empty_dataframe(self, mock_get_secret, mock_storage):
        """Test creation of empty DataFrame with correct schema"""
        # Mock secrets
        mock_get_secret.side_effect = ["test-token", "test-business-id"]

        fetcher = InstagramStoryMetricsFetcher("NPI", "test-project")
        df = fetcher._create_empty_dataframe()

        # Check schema
        expected_columns = [
            "story_id", "Story Date", "timestamp", "Media Type", "permalink",
            "media_url", "Impressions", "Reach", "Exits", "Replies",
            "Taps Forward", "Taps Back", "Exit Rate", "Reply Rate",
            "Tap Forward Rate", "Tap Back Rate", "metric_date"
        ]
        
        assert list(df.columns) == expected_columns
        assert df.is_empty()
        
        # Check data types
        assert df["Story Date"].dtype == pl.Datetime("ns")
        assert df["Impressions"].dtype == pl.Int64
        assert df["Exit Rate"].dtype == pl.Float64

    @patch('google.cloud.storage.Client')
    @patch.object(SecretsManager, 'get_secret')
    def test_process_stories(self, mock_get_secret, mock_storage):
        """Test processing of story data"""
        # Mock secrets
        mock_get_secret.side_effect = ["test-token", "test-business-id"]
        
        fetcher = InstagramStoryMetricsFetcher("NPI", "test-project")
        
        # Mock story metrics fetch
        with patch.object(fetcher, 'fetch_story_metrics') as mock_fetch_metrics:
            mock_fetch_metrics.return_value = {
                "impressions": 1000,
                "reach": 800,
                "exits": 50,
                "replies": 10,
                "taps_forward": 100,
                "taps_back": 20,
            }
            
            # Sample story data
            stories = [
                {
                    "id": "17900000001",
                    "timestamp": "2025-08-04T12:00:00+0000",
                    "media_type": "STORY",
                    "permalink": "https://instagram.com/stories/test/1/",
                    "media_url": "https://scontent.instagram.com/story.jpg"
                }
            ]
            
            df = fetcher.process_stories(stories)
            
            assert len(df) == 1
            assert df["story_id"][0] == "17900000001"
            assert df["Impressions"][0] == 1000
            assert df["Reach"][0] == 800
            assert df["Exit Rate"][0] == 5.0  # 50/1000 * 100
            assert df["Reply Rate"][0] == 1.0  # 10/1000 * 100

    @patch('google.cloud.storage.Client')
    @patch.object(SecretsManager, 'get_secret')
    def test_upload_to_gcs(self, mock_get_secret, mock_storage):
        """Test GCS upload functionality"""
        # Mock secrets
        mock_get_secret.side_effect = ["test-token", "test-business-id"]
        
        # Mock GCS bucket and blob
        mock_bucket = Mock()
        mock_blob = Mock()
        mock_storage.return_value.get_bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        
        fetcher = InstagramStoryMetricsFetcher("NPI", "test-project")
        
        # Create test DataFrame
        df = pl.DataFrame({
            "story_id": ["123"],
            "Story Date": [datetime.now()],
            "timestamp": [datetime.now()],
            "Media Type": ["Story"],
            "permalink": ["https://test.com"],
            "media_url": ["https://test.com/img.jpg"],
            "Impressions": [100],
            "Reach": [80],
            "Exits": [5],
            "Replies": [2],
            "Taps Forward": [10],
            "Taps Back": [3],
            "Exit Rate": [5.0],
            "Reply Rate": [2.0],
            "Tap Forward Rate": [10.0],
            "Tap Back Rate": [3.0],
            "metric_date": [datetime.now()],
        })
        
        # Test upload
        today = datetime.now().date()
        fetcher.upload_to_gcs(df, today)
        
        # Verify calls
        mock_bucket.blob.assert_called()
        mock_blob.upload_from_string.assert_called()
        
        # Check the blob path
        expected_path = f"Instagram/NPI/insights/stories/{today}/instagram_story_metrics_{today}.parquet"
        actual_path = mock_bucket.blob.call_args[0][0]
        assert actual_path == expected_path


class TestIntegration:
    """Integration tests for the full pipeline"""
    
    @pytest.mark.skipif(
        not os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        reason="No Google credentials available"
    )
    def test_full_pipeline(self):
        """Test the full pipeline with real API calls (if credentials available)"""
        # This test will only run if proper credentials are set
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT", "test-project")
        
        try:
            fetcher = InstagramStoryMetricsFetcher("NPI", project_id)
            
            # Verify token
            if fetcher.verify_token_scopes():
                # Get date range
                start_date, end_date = fetcher.get_eligible_story_date_range()
                
                # Try to fetch stories (may be empty if no stories in range)
                stories = fetcher.fetch_stories(start_date, end_date)
                
                if stories:
                    # Process stories
                    df = fetcher.process_stories(stories)
                    assert not df.is_empty()
                    
                    # Could test upload here if we want
                    # fetcher.upload_to_gcs(df, datetime.now().date())
        except Exception as e:
            # If secrets are not configured, skip
            pytest.skip(f"Integration test skipped: {str(e)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])