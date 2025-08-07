terraform {
  required_version = ">= 1.0"
  
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  
  backend "gcs" {
    bucket = "terraform-state-bucket"
    prefix = "ig-story-metrics-fetcher"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "cobalt-door-450002-c0"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Environment (staging or production)"
  type        = string
  default     = "production"
}

# Local variables
locals {
  function_name     = var.environment == "production" ? "ig-story-metrics-fetcher-prod" : "ig-story-metrics-fetcher-staging"
  scheduler_name    = var.environment == "production" ? "ig-story-metrics-scheduler-prod" : "ig-story-metrics-scheduler-staging"
  schedule_cron     = var.environment == "production" ? "0 * * * *" : "0 */2 * * *"  # Hourly for prod, bi-hourly for staging
  service_account   = "ig-metrics-sa@${var.project_id}.iam.gserviceaccount.com"
}

# Cloud Storage bucket for function source
resource "google_storage_bucket" "function_source" {
  name     = "${var.project_id}-ig-story-functions"
  location = var.region
  
  uniform_bucket_level_access = true
  
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }
}

# Zip the function source code
data "archive_file" "function_source" {
  type        = "zip"
  output_path = "/tmp/ig-story-function-source.zip"
  
  source_dir = "${path.module}/.."
  
  excludes = [
    "terraform",
    ".terraform",
    "*.tfstate*",
    ".git",
    ".github",
    "venv",
    "__pycache__",
    "*.pyc",
    ".pytest_cache",
    "tests",
    "deployment",
    "*.md",
    ".env"
  ]
}

# Upload function source to GCS
resource "google_storage_bucket_object" "function_source" {
  name   = "ig-story-metrics-fetcher-${data.archive_file.function_source.output_md5}.zip"
  bucket = google_storage_bucket.function_source.name
  source = data.archive_file.function_source.output_path
}

# Cloud Function
resource "google_cloudfunctions2_function" "story_metrics_fetcher" {
  name        = local.function_name
  location    = var.region
  description = "Instagram Story Metrics Fetcher - ${var.environment}"
  
  build_config {
    runtime     = "python311"
    entry_point = "fetch_instagram_story_metrics_batch"
    
    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.function_source.name
      }
    }
  }
  
  service_config {
    max_instance_count    = 10
    min_instance_count    = 0
    available_memory      = "512M"
    timeout_seconds       = 540
    service_account_email = local.service_account
    
    environment_variables = {
      GOOGLE_CLOUD_PROJECT = var.project_id
      GCS_BUCKET           = "chapala-bronze-bucket"
      LOG_LEVEL            = "INFO"
      DATA_WINDOW_DAYS     = "1"
    }
  }
}

# Make function publicly accessible
resource "google_cloud_run_service_iam_member" "public_access" {
  service  = google_cloudfunctions2_function.story_metrics_fetcher.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# Cloud Scheduler job
resource "google_cloud_scheduler_job" "story_metrics_scheduler" {
  name        = local.scheduler_name
  description = "Trigger Instagram Story Metrics Fetcher ${var.environment}"
  schedule    = local.schedule_cron
  time_zone   = "America/Los_Angeles"
  region      = var.region
  
  retry_config {
    retry_count          = 3
    min_backoff_duration = "30s"
    max_backoff_duration = "600s"
  }
  
  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.story_metrics_fetcher.url
    
    body = base64encode(jsonencode({
      accounts = ["NPI", "LT", "MD", "RE", "SML"]
    }))
    
    headers = {
      "Content-Type" = "application/json"
    }
    
    oidc_token {
      service_account_email = local.service_account
    }
  }
}

# Outputs
output "function_url" {
  value       = google_cloudfunctions2_function.story_metrics_fetcher.url
  description = "URL of the deployed Cloud Function"
}

output "scheduler_name" {
  value       = google_cloud_scheduler_job.story_metrics_scheduler.name
  description = "Name of the Cloud Scheduler job"
}

output "schedule" {
  value       = google_cloud_scheduler_job.story_metrics_scheduler.schedule
  description = "Cron schedule for the job"
}

output "next_run_time" {
  value       = google_cloud_scheduler_job.story_metrics_scheduler.state
  description = "Scheduler job state"
}