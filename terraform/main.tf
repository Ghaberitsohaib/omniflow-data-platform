terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.30"
    }
  }
}

provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# =========================================================================
# MODULE 7 & LAKE: Google Cloud Storage Data Lake
# =========================================================================
resource "google_storage_bucket" "data_lake_bucket" {
  name          = var.gcs_bucket_name
  location      = var.gcp_region
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  versioning {
    enabled = true
  }
}

# Landing zones / folders simulation
resource "google_storage_bucket_object" "bronze_folder" {
  name    = "bronze/"
  content = " "
  bucket  = google_storage_bucket.data_lake_bucket.name
}

resource "google_storage_bucket_object" "silver_folder" {
  name    = "silver/"
  content = " "
  bucket  = google_storage_bucket.data_lake_bucket.name
}

resource "google_storage_bucket_object" "gold_folder" {
  name    = "gold/"
  content = " "
  bucket  = google_storage_bucket.data_lake_bucket.name
}

# =========================================================================
# MODULE 3: Google BigQuery Data Warehouse
# =========================================================================
resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id                  = var.bq_dataset_raw
  friendly_name               = "E-Commerce Raw Ingestion"
  description                 = "Raw tables ingested from Kafka & dlt"
  location                    = var.gcp_region
  delete_contents_on_destroy  = true
}

resource "google_bigquery_dataset" "analytics_dataset" {
  dataset_id                  = var.bq_dataset_analytics
  friendly_name               = "E-Commerce Analytics Marts"
  description                 = "Modeled dimension and fact tables created by dbt & Spark"
  location                    = var.gcp_region
  delete_contents_on_destroy  = true
}
