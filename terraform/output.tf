output "service_account_email" {
  value = google_service_account.env_sa.email
}

output "gcs_bucket_name" {
  value = google_storage_bucket.env_bucket.name
}

output "bq_dataset_id" {
  value = google_bigquery_dataset.env_dataset.dataset_id
}
