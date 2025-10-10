# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# a null resource that is connected to the content_hash from build.tf
resource "null_resource" "deployment_trigger" {
  triggers = {
    source_contents_hash = local.cloud_build_content_hash
  }
}

resource "google_service_account" "cloudrun_sa" {
  project      = var.project_id
  account_id   = "open-lakehouse-demo-run-sa"
  display_name = "Cloud Run Service Account for Open Lakehouse Demo"
  description  = "Service account for the Cloud Run service to run PySpark jobs and interact with BigQuery and GCS."
}

# the cloud service
resource "google_cloud_run_v2_service" "default" {
  provider = google
  name     = "open-data-lakehouse-demo"
  location = var.region
  project  = var.project_id
  ingress  = "INGRESS_TRAFFIC_ALL"

  deletion_protection = false # set to "true" in production

  template {

    vpc_access {
      egress = "ALL_TRAFFIC"
      network_interfaces {
        network    = google_compute_network.open-lakehouse-network.id
        subnetwork = google_compute_subnetwork.open-lakehouse-subnetwork.id
      }
    }
    containers {

      image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/${local.image_name}:latest"
      ports {
        container_port = 8080
      }
      env {
        name  = "BQ_DATASET"
        value = google_bigquery_dataset.ridership_lakehouse.dataset_id
      }
      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
      env {
        name  = "GCS_MAIN_BUCKET"
        value = google_storage_bucket.data_lakehouse_bucket.name
      }
      env {
        name  = "REGION"
        value = var.region
      }
      env {
        name  = "KAFKA_BOOTSTRAP"
        value = "bootstrap.${google_managed_kafka_cluster.default.cluster_id}.${google_managed_kafka_cluster.default.location}.managedkafka.${var.project_id}.cloud.goog:9092"
      }
      env {
        name  = "KAFKA_TOPIC"
        value = google_managed_kafka_topic.bus_updates.topic_id
      }
      env {
        name  = "KAFKA_ALERT_TOPIC"
        value = google_managed_kafka_topic.capacity_alerts.topic_id
      }
      env {
        name  = "SPARK_TMP_BUCKET"
        value = google_storage_bucket.spark_bucket.name
      }
      env {
        name  = "SPARK_CHECKPOINT_LOCATION"
        value = "gs://${google_storage_bucket.spark_bucket.name}/checkpoint"
      }
      env {
        name  = "BIGQUERY_TABLE"
        value = "bus_state"
      }
      env {
        name  = "SUBNET_URI"
        value = google_compute_subnetwork.open-lakehouse-subnetwork.id
      }
      env {
        name  = "SERVICE_ACCOUNT"
        value = google_service_account.cloudrun_sa.email
      }
      resources {
        limits = {
          cpu    = "1000m"
          memory = "2Gi"
        }
      }
    }
    service_account = google_service_account.cloudrun_sa.email
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_artifact_registry_repository.docker_repo,
    module.gcloud_build_webapp.wait,
    module.project_services,
    google_project_iam_member.cloud_run_user_bq_permissions,
    google_project_iam_member.cloud_run_user_dataproc_permissions
  ]

  lifecycle {
    replace_triggered_by = [null_resource.deployment_trigger]

  }
  invoker_iam_disabled = true

}

# Assigns the Storage Object Viewer role to the service account, allowing it to read objects in GCS buckets.
resource "google_project_iam_member" "cloud_run_user_bq_permissions" {
  for_each = toset(["roles/bigquery.dataEditor", "roles/bigquery.jobUser", "roles/iam.serviceAccountUser"])
  project  = var.project_id
  role     = each.value

  member = "serviceAccount:${google_service_account.cloudrun_sa.email}"
}

resource "google_project_iam_member" "cloud_run_user_dataproc_permissions" {
  for_each = toset(["roles/dataproc.editor", "roles/dataproc.worker", "roles/storage.expressModeUserAccess", "roles/managedkafka.client"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.cloudrun_sa.email}"
}

output "cloud_run_url" {
  description = "The URL of the deployed Cloud Run service"
  value       = google_cloud_run_v2_service.default.uri
}
