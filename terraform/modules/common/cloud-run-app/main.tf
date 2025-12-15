terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0"
    }
  }
}

resource "google_cloud_run_v2_service" "default" {
  name     = var.service_name
  location = var.region
  project  = var.project_id
  ingress  = var.ingress

  deletion_protection = false

  template {
    vpc_access {
      egress = "ALL_TRAFFIC"
      network_interfaces {
        network    = var.network_id
        subnetwork = var.subnetwork_id
      }
    }
    containers {
      image = var.image

      ports {
        container_port = var.container_port
      }

      dynamic "env" {
        for_each = var.env_vars
        content {
          name  = env.key
          value = env.value
        }
      }

      resources {
        limits = var.limits
      }
    }
    service_account = var.service_account_email
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  # We cannot easily abstract depends_on for the build script here directly 
  # unless we pass a dummy variable that changes when build changes, 
  # but Cloud Run usually just needs the image to exist.
  # The parent module should handle the depends_on relationship if needed 
  # by passing the image digest or ensuring build completes first.
}

resource "google_cloud_run_v2_service_iam_binding" "default" {
  count    = length(var.invoker_iam_members) > 0 ? 1 : 0
  project  = var.project_id
  location = google_cloud_run_v2_service.default.location
  name     = google_cloud_run_v2_service.default.name
  role     = "roles/run.invoker"
  members  = var.invoker_iam_members

  depends_on = [
    google_cloud_run_v2_service.default,
  ]
}
