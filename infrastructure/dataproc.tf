# Dataproc Cluster for Spark processing
# Used for Common Crawl and Reddit bulk processing

resource "google_dataproc_cluster" "spark" {
  name    = "kyivnotkiev-spark"
  region  = var.region
  project = google_project.research.project_id

  cluster_config {
    staging_bucket = google_storage_bucket.dataproc_staging.name

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 4
      machine_type  = "n1-standard-4"

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 100
      }
    }

    autoscaling_config {
      policy_uri = google_dataproc_autoscaling_policy.spark.name
    }

    software_config {
      image_version = "2.1-debian11"

      override_properties = {
        "spark:spark.executor.memory"      = "8g"
        "spark:spark.driver.memory"        = "8g"
        "spark:spark.sql.shuffle.partitions" = "200"
        "spark:spark.dynamicAllocation.enabled" = "true"
      }
    }

    initialization_action {
      script      = "gs://${google_storage_bucket.data.name}/scripts/init_cluster.sh"
      timeout_sec = 300
    }

    gce_cluster_config {
      service_account_scopes = [
        "https://www.googleapis.com/auth/cloud-platform",
      ]
    }
  }

  depends_on = [google_project_service.apis]

  # Don't keep the cluster running — create on demand
  lifecycle {
    ignore_changes = [cluster_config[0].initialization_action]
  }
}

resource "google_dataproc_autoscaling_policy" "spark" {
  policy_id = "kyivnotkiev-autoscale"
  location  = var.region
  project   = google_project.research.project_id

  basic_algorithm {
    yarn_config {
      scale_up_factor           = 1.0
      scale_down_factor         = 1.0
      scale_up_min_worker_fraction = 0.0
      graceful_decommission_timeout = "1h"
    }
    cooldown_period = "120s"
  }

  worker_config {
    min_instances = 2
    max_instances = 8
    weight        = 1
  }

  secondary_worker_config {
    min_instances = 0
    max_instances = 8
    weight        = 1
  }

  depends_on = [google_project_service.apis]
}
