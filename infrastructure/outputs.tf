output "project_id" {
  value = google_project.research.project_id
}

output "bigquery_dataset" {
  value = google_bigquery_dataset.kyivnotkiev.dataset_id
}

output "data_bucket" {
  value = google_storage_bucket.data.name
}

output "dataproc_cluster" {
  value = google_dataproc_cluster.spark.name
}

output "cloud_run_url" {
  value = google_cloud_run_v2_service.orchestrator.uri
}

output "artifact_registry" {
  value = google_artifact_registry_repository.pipeline.name
}

output "service_account" {
  value = google_service_account.pipeline.email
}
