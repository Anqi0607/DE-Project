terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# Create Service Account for each environment
resource "google_service_account" "env_sa" {
  account_id   = "${var.env}-terraform-sa"
  display_name = "Terraform Service Account for ${var.env}"
}

# Assign IAM Roles
locals {
  sa_email = google_service_account.env_sa.email
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "storage_object_admin" {
  project = var.project
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${local.sa_email}"
}

resource "google_project_iam_member" "viewer" {
  project = var.project
  role    = "roles/viewer"
  member  = "serviceAccount:${local.sa_email}"
}

# Create GCS bucket
resource "google_storage_bucket" "env_bucket" {
  name          = "${var.env}-${var.gcs_bucket_name}"
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
}

# Create BigQuery dataset
resource "google_bigquery_dataset" "env_dataset" {
  dataset_id                  = "${var.env}_${var.bq_dataset_name}"
  location                    = var.location
  delete_contents_on_destroy = true
}
