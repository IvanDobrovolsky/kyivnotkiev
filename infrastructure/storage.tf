# GCS Buckets

# Main data bucket
resource "google_storage_bucket" "data" {
  name          = "${var.project_id}-data"
  location      = "US"
  project       = google_project.research.project_id
  force_destroy = false

  uniform_bucket_level_access = true

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

  labels = {
    project = "kyivnotkiev"
  }

  depends_on = [google_project_service.apis]
}

# Terraform state bucket (must be created manually before first apply)
# gcloud storage buckets create gs://kyivnotkiev-terraform-state --location=US

# Dataproc staging bucket
resource "google_storage_bucket" "dataproc_staging" {
  name          = "${var.project_id}-dataproc-staging"
  location      = "US"
  project       = google_project.research.project_id
  force_destroy = true

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# Artifact Registry for Docker images
resource "google_artifact_registry_repository" "pipeline" {
  location      = var.region
  repository_id = "kyivnotkiev-pipeline"
  description   = "Pipeline Docker images"
  format        = "DOCKER"
  project       = google_project.research.project_id

  depends_on = [google_project_service.apis]
}
