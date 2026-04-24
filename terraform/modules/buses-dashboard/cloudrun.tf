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


module "cloud_run_app" {
  source = "../common/cloud-run-app"

  project_id            = var.project_id
  region                = var.region
  service_name          = "buses-dashboard"
  image                 = local.buses_dashboard_image_name_and_tag
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
    SPARK_CHECKPOINT_LOCATION = "gs://${var.spark_tmp_bucket}/checkpoints"
    BIGQUERY_TABLE            = "bus_state"
    SUBNET_URI                = var.subnetwork_id
    SERVICE_ACCOUNT           = var.spark_service_account_email
    SOURCE_CONTENT_HASH       = local.buses_dashboard_content_hash
  }

  container_port = 3000

  invoker_iam_members = ["allUsers"]

  depends_on = [module.container_build]
}

moved {
  from = google_cloud_run_v2_service.buses_dashboard
  to   = module.cloud_run_app.google_cloud_run_v2_service.default
}

moved {
  from = google_cloud_run_v2_service_iam_binding.buses_dashboard
  to   = module.cloud_run_app.google_cloud_run_v2_service_iam_binding.default[0]
}
