variable "env" {
  description = "The environment name (e.g., dev, staging, prod)"
  type        = string
}

variable "project" {
  description = "The GCP Project ID to deploy resources into"
  type        = string
  default     = "just-camera-454714-e8"
}

variable "region" {
  description = "The GCP region used by the provider (not all services use this)"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "The location used for resources like GCS and BigQuery (e.g., US, EU, us-central1)"
  type        = string
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "The base name of the GCS bucket (environment prefix will be added automatically)"
  type        = string
  default     = "project-bucket-pebbles"
}

variable "bq_dataset_name" {
  description = "The base name of the BigQuery dataset (environment prefix will be added automatically)"
  type        = string
  default     = "project-dataset-pebbles"
}

variable "credentials" {
  description = "Path to the service account key JSON file (used for authentication)"
  type        = string
  default     = "../de-project-key.json" # relative path to make it easier for team cooperation
}
