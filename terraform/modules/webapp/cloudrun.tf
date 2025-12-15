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
  service_name          = "open-data-lakehouse-demo"
  image                 = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/${local.image_name}:latest"
  network_id            = var.network_id
  subnetwork_id         = var.subnetwork_id
  service_account_email = google_service_account.cloudrun_sa.email

  env_vars = {
    BQ_DATASET                = var.bq_dataset_id
    PROJECT_ID                = var.project_id
    GCS_MAIN_BUCKET           = var.gcs_main_bucket
    REGION                    = var.region
    KAFKA_BOOTSTRAP           = var.kafka_bootstrap
    KAFKA_TOPIC               = var.kafka_topic
    KAFKA_ALERT_TOPIC         = var.kafka_alert_topic
    SPARK_TMP_BUCKET          = var.spark_tmp_bucket
    SPARK_CHECKPOINT_LOCATION = "gs://${var.spark_tmp_bucket}/checkpoint"
    BIGQUERY_TABLE            = "bus_state"
    SUBNET_URI                = var.subnetwork_id
    SERVICE_ACCOUNT           = var.spark_service_account_email
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
