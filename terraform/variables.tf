variable "gcp_project_id" {
  description = "The Google Cloud Project ID"
  type        = string
  default     = "omniflow-de-project"
}

variable "gcp_region" {
  description = "Google Cloud Region for resources"
  type        = string
  default     = "europe-west1"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Storage Data Lake Bucket Name"
  type        = string
  default     = "omniflow-ecommerce-lake"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  type        = string
  default     = "STANDARD"
}

variable "bq_dataset_raw" {
  description = "BigQuery raw / staging dataset ID"
  type        = string
  default     = "ecommerce_raw"
}

variable "bq_dataset_analytics" {
  description = "BigQuery gold / analytics dataset ID"
  type        = string
  default     = "ecommerce_analytics"
}
