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


# the cloud service
module "cloud_run_app" {
  source = "../common/cloud-run-app"

  project_id            = var.project_id
  region                = var.region
  service_name          = "open-data-lakehouse-data-journey"
  image                 = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/${local.image_name}:latest"
  network_id            = var.network_id
  subnetwork_id         = var.subnetwork_id
  service_account_email = google_service_account.data_journey_service_account.email

  env_vars = {
    PROJECT_ID               = var.project_id
    LOCATION                 = var.region
    GENERAL_BUCKET_NAME      = var.gcs_main_bucket
    STAGING_BQ_DATASET       = var.staging_bq_dataset
    MAIN_BQ_DATASET          = var.bq_dataset_id
    BQ_CONNECTION_NAME       = var.bq_connection_name
    BQ_CATALOG_BUCKET_NAME   = var.bq_catalog_bucket_name
    REST_CATALOG_BUCKET_NAME = var.rest_catalog_bucket_name
    SUBNETWORK_ID            = var.subnetwork_id
  }

  invoker_iam_members = ["allUsers"]
}

moved {
  from = google_cloud_run_v2_service.default
  to   = module.cloud_run_app.google_cloud_run_v2_service.default
}

moved {
  from = google_cloud_run_v2_service_iam_binding.default
  to   = module.cloud_run_app.google_cloud_run_v2_service_iam_binding.default[0]
}
