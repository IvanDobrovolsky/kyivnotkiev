output "project_id" {
  value = var.project_id
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.kyivnotkiev.dataset_id
}

output "data_bucket" {
  value = google_storage_bucket.data.name
}

output "artifact_registry" {
  value = google_artifact_registry_repository.pipeline.name
}
