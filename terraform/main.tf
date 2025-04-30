terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
# no need to use credentials of a service account
# as we will create service accounts for different env
#   credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# Create Service Account for each environment (staging/dev)
resource "google_service_account" "env_sa" {
  account_id   = "${terraform.workspace}-terraform-sa"  # 动态命名
  display_name = "Terraform Service Account for ${terraform.workspace}"
}

# Assign IAM Roles
locals {
  sa_email = google_service_account.env_sa.email
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${local.sa_email}"

  depends_on = [google_service_account.env_sa]  # 确保在服务账号创建后再进行 IAM 角色分配
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

# Create GCS bucket with dynamic name
resource "google_storage_bucket" "env_bucket" {
  name          = "${terraform.workspace}-${var.gcs_bucket_name}"  # 动态命名
  location      = var.location
  force_destroy = true
  uniform_bucket_level_access = true
}

# Create BigQuery dataset with dynamic name
resource "google_bigquery_dataset" "env_dataset" {
  dataset_id                  = "${terraform.workspace}_${var.bq_dataset_name}"  # 动态命名
  location                    = var.location
  delete_contents_on_destroy = true
}

# create service account for dataproc
resource "google_service_account" "dataproc_node_sa" {
  account_id   = "${terraform.workspace}-dataproc-node"
  display_name = "Dataproc Node SA for ${terraform.workspace}"
}

# assign minimum access
resource "google_project_iam_member" "dataproc_node_storage_object_admin" {
  project = var.project
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataproc_node_sa.email}"
}

resource "google_project_iam_member" "dataproc_node_log_writer" {
  project = var.project
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.dataproc_node_sa.email}"
}

resource "google_project_iam_member" "dataproc_node_metric_writer" {
  project = var.project
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.dataproc_node_sa.email}"
}
