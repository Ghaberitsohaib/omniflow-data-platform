output "data_lake_bucket_url" {
  description = "GCS Data Lake Bucket URL"
  value       = google_storage_bucket.data_lake_bucket.url
}

output "bigquery_raw_dataset_id" {
  description = "BigQuery Raw Dataset ID"
  value       = google_bigquery_dataset.raw_dataset.dataset_id
}

output "bigquery_analytics_dataset_id" {
  description = "BigQuery Analytics Dataset ID"
  value       = google_bigquery_dataset.analytics_dataset.dataset_id
}
