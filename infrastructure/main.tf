terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }

  backend "gcs" {
    bucket = "kyivnotkiev-terraform-state"
    prefix = "terraform/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Create the GCP project
resource "google_project" "research" {
  name            = "KyivNotKiev Research"
  project_id      = var.project_id
  billing_account = var.billing_account_id
}

# Enable required APIs
resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "dataproc.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "youtube.googleapis.com",
    "compute.googleapis.com",
  ])

  project = google_project.research.project_id
  service = each.value

  disable_dependent_services = false
  disable_on_destroy         = false
}
