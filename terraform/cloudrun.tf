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

# the cloud service
resource "google_cloud_run_v2_service" "default" {
  provider = google
  name     = "open-data-lakehouse-demo"
  location = var.region
  project  = var.project_id
  ingress  = "INGRESS_TRAFFIC_ALL"

  deletion_protection = false # set to "true" in production

  template {
    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.docker_repo.repository_id}/${local.image_name}:latest"
      ports {
        container_port = 8080
      }
      resources {
        limits = {
          cpu    = "1000m"
          memory = "2Gi"
        }
      }
    }
  }

  traffic {
    type    = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
    percent = 100
  }

  depends_on = [
    google_artifact_registry_repository.docker_repo,
    module.gcloud_build_webapp.wait,
    module.project_services,
    google_project_organization_policy.allow_policy_member_domains
  ]

  lifecycle {
    replace_triggered_by = [null_resource.deployment_trigger]
  }
  invoker_iam_disabled = true

}

resource "google_cloud_run_v2_service_iam_binding" "default" {
  project  = var.project_id
  location = google_cloud_run_v2_service.default.location
  name     = google_cloud_run_v2_service.default.name
  role     = "roles/run.invoker"
  members = [
    "allUsers"
  ]

  depends_on = [
    google_cloud_run_v2_service.default,
    google_project_organization_policy.allow_policy_member_domains
  ]
}

output "cloud_run_url" {
  description = "The URL of the deployed Cloud Run service"
  value       = google_cloud_run_v2_service.default.uri
}