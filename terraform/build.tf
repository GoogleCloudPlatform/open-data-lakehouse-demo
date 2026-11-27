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


locals {
  artifact_repo            = "open-lakehouse-demo-docker-repo"
  image_name               = "open-lakehouse-demo-webapp"
  cloud_build_fileset      = fileset(path.module, "../webapp/**")
  cloud_build_content_hash = sha512(join("", [for f in local.cloud_build_fileset : filesha512("${path.module}/${f}")]))
  image_name_and_tag       = "${var.region}-docker.pkg.dev/${var.project_id}/${local.artifact_repo}/${local.image_name}:latest"
}

resource "google_artifact_registry_repository" "docker_repo" {
  project       = var.project_id
  format        = "DOCKER"
  location      = var.region
  repository_id = local.artifact_repo
  description   = "Docker containers"
}

module "cloud_build_account" {
  source     = "github.com/terraform-google-modules/terraform-google-service-accounts"
  project_id = var.project_id
  names      = ["cloud-build"]
  project_roles = [
    "${var.project_id}=>roles/logging.logWriter",
    "${var.project_id}=>roles/storage.objectAdmin",
    "${var.project_id}=>roles/artifactregistry.writer",
    "${var.project_id}=>roles/run.developer",
  ]
  display_name = "Cloud Build Service Account"
  description  = "specific custom service account for Cloud Build"
}

# Grant actAs only on the Cloud Run runtime SA, not project-wide across every SA.
resource "google_service_account_iam_member" "cloud_build_actas_cloudrun" {
  service_account_id = google_service_account.cloudrun_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${module.cloud_build_account.email}"
}

# Propagation time for change of access policy typically takes 2 minutes
# according to https://cloud.google.com/iam/docs/access-change-propagation
# this wait make sure the policy changes are propagated before proceeding
# with the build
resource "time_sleep" "wait_for_policy_propagation" {
  create_duration = "120s"
  depends_on = [
    module.cloud_build_account,
    google_service_account_iam_member.cloud_build_actas_cloudrun,
  ]
}

# Pre-create the regional Cloud Build logs bucket so the custom cloud-build SA
# stays scoped to roles/storage.objectAdmin without needing storage.buckets.create.
resource "google_storage_bucket" "cloudbuild_logs" {
  name                        = "${data.google_project.project.number}-${var.region}-cloudbuild-logs"
  location                    = var.region
  project                     = var.project_id
  uniform_bucket_level_access = true
  force_destroy               = true
}

# Cloud Build calls storage.buckets.get on the regional logs bucket; grant it
# scoped strictly to this bucket rather than project-wide roles/storage.admin.
resource "google_storage_bucket_iam_member" "cloudbuild_logs_bucket_reader" {
  bucket = google_storage_bucket.cloudbuild_logs.name
  role   = "roles/storage.legacyBucketReader"
  member = "serviceAccount:${module.cloud_build_account.email}"
}

# See github.com/terraform-google-modules/terraform-google-gcloud
module "gcloud_build_webapp" {
  source                = "github.com/terraform-google-modules/terraform-google-gcloud" # commit hash of version 3.5.0
  create_cmd_entrypoint = "gcloud"
  create_cmd_body       = <<-EOT
    builds submit ${path.module}/../webapp \
      --tag ${local.image_name_and_tag} \
      --project ${var.project_id} \
      --billing-project ${var.project_id} \
      --region ${var.region} \
      --default-buckets-behavior regional-user-owned-bucket \
      --service-account "projects/${var.project_id}/serviceAccounts/${module.cloud_build_account.email}"
  EOT
  enabled               = true

  create_cmd_triggers = {
    source_contents_hash   = local.cloud_build_content_hash
    cloudbuild_logs_bucket = google_storage_bucket_iam_member.cloudbuild_logs_bucket_reader.id
  }

  # Commenting the dependency for the wait command - the gcloud module has a bug when specifying explicit dependenceis
  # This means that the first build might fail, since the IAM permissions hasn't cascaded yet.
  # depends_on = [
  #   module.wait_for_policy_propagation
  # ]
}


