terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

variable "project_id"  { default = "data-platform-portfolio" }
variable "region"      { default = "US" }
variable "environment" { default = "dev" }

locals {
  prefix = "data-platform-${var.environment}"
  common_labels = {
    project     = "data-platform"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_storage_bucket" "bronze" {
  name                     = "${local.prefix}-bronze"
  location                 = var.region
  storage_class            = "STANDARD"
  labels                   = merge(local.common_labels, { layer = "bronze" })
  public_access_prevention = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }
}

resource "google_storage_bucket" "silver" {
  name                     = "${local.prefix}-silver"
  location                 = var.region
  storage_class            = "STANDARD"
  labels                   = merge(local.common_labels, { layer = "silver" })
  public_access_prevention = "enforced"
}

resource "google_storage_bucket" "gold" {
  name                     = "${local.prefix}-gold"
  location                 = var.region
  storage_class            = "STANDARD"
  labels                   = merge(local.common_labels, { layer = "gold" })
  public_access_prevention = "enforced"
}

resource "google_bigquery_dataset" "staging" {
  dataset_id                 = "staging"
  description                = "dbt staging models — cleaned source data"
  location                   = var.region
  labels                     = merge(local.common_labels, { layer = "staging" })
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "core" {
  dataset_id                 = "core"
  description                = "dbt core marts — dimensions and facts"
  location                   = var.region
  labels                     = merge(local.common_labels, { layer = "core" })
  delete_contents_on_destroy = false
}

resource "google_bigquery_dataset" "finance" {
  dataset_id                 = "finance"
  description                = "dbt finance marts — revenue and LTV models"
  location                   = var.region
  labels                     = merge(local.common_labels, { layer = "finance" })
  delete_contents_on_destroy = false
}

resource "google_service_account" "pipeline_sa" {
  account_id   = "${local.prefix}-pipeline-sa"
  display_name = "Data Platform Pipeline Service Account"
  description  = "Used by Airflow and dbt to access GCS and BigQuery"
}

resource "google_storage_bucket_iam_member" "pipeline_bronze" {
  bucket = google_storage_bucket.bronze.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_silver" {
  bucket = google_storage_bucket.silver.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_storage_bucket_iam_member" "pipeline_gold" {
  bucket = google_storage_bucket.gold.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "pipeline_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_secret_manager_secret" "postgres_creds" {
  secret_id = "postgres-credentials"
  labels    = local.common_labels

  replication {
    auto {}
  }
}

resource "google_secret_manager_secret_version" "postgres_creds" {
  secret = google_secret_manager_secret.postgres_creds.name
  secret_data = jsonencode({
    host     = "your-postgres-host"
    port     = 5432
    dbname   = "ecommerce"
    username = "platform"
    password = "replace-with-strong-password"
  })
}

output "bronze_bucket"      { value = google_storage_bucket.bronze.name }
output "silver_bucket"      { value = google_storage_bucket.silver.name }
output "gold_bucket"        { value = google_storage_bucket.gold.name }
output "pipeline_sa_email"  { value = google_service_account.pipeline_sa.email }
output "bq_staging_dataset" { value = google_bigquery_dataset.staging.dataset_id }
output "bq_core_dataset"    { value = google_bigquery_dataset.core.dataset_id }
output "bq_finance_dataset" { value = google_bigquery_dataset.finance.dataset_id }