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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or .
# See the License for the specific language governing permissions and
# limitations under the License.

provider "google" {
  project               = var.project_id
  user_project_override = true
  billing_project       = var.project_id
}

module "general_infra" {
  source     = "./modules/general-infra"
  project_id = var.project_id
  region     = var.region
  zone       = var.zone
}

module "data_infra" {
  source                  = "./modules/data-infra"
  project_id              = var.project_id
  region                  = var.region
  zone                    = var.zone
  subnetwork_id           = module.general_infra.subnetwork_id
  gcs_main_bucket         = module.general_infra.gcs_main_bucket
  gcs_rest_catalog_bucket = module.general_infra.rest_catalog_bucket_name
}

module "webapp" {
  source                      = "./modules/webapp"
  artifact_repo               = module.general_infra.artifact_repo
  project_id                  = var.project_id
  region                      = var.region
  zone                        = var.zone
  subnetwork_id               = module.general_infra.subnetwork_id
  kafka_topic                 = module.data_infra.kafka_topic
  bq_dataset_id               = module.data_infra.bq_dataset_id
  build_service_account       = module.general_infra.cloud_build_sa_email
  gcs_main_bucket             = module.general_infra.gcs_main_bucket
  kafka_alert_topic           = module.data_infra.kafka_alert_topic
  kafka_bootstrap             = module.data_infra.kafka_bootstrap
  network_id                  = module.general_infra.network_id
  spark_service_account_email = module.data_infra.spark_service_account_email
  spark_tmp_bucket            = module.general_infra.spark_bucket
}

module "data_journey" {
  source                   = "./modules/data-journey-cloud-run"
  artifact_repo            = module.general_infra.artifact_repo
  project_id               = var.project_id
  region                   = var.region
  zone                     = var.zone
  subnetwork_id            = module.general_infra.subnetwork_id
  network_id               = module.general_infra.network_id
  bq_catalog_bucket_name   = module.general_infra.bq_catalog_bucket_name
  bq_connection_name       = module.data_infra.bq_connection_name
  bq_dataset_id            = module.data_infra.bq_dataset_id
  gcs_main_bucket          = module.general_infra.gcs_main_bucket
  rest_catalog_bucket_name = module.general_infra.rest_catalog_bucket_name
  staging_bq_dataset       = module.data_infra.staging_bq_dataset
  build_service_account    = module.general_infra.cloud_build_sa_email
}

module "buses-dashboard" {
  source                      = "./modules/buses-dashboard"
  artifact_repo               = module.general_infra.artifact_repo
  project_id                  = var.project_id
  region                      = var.region
  zone                        = var.zone
  subnetwork_id               = module.general_infra.subnetwork_id
  kafka_topic                 = module.data_infra.kafka_topic
  bq_dataset_id               = module.data_infra.bq_dataset_id
  build_service_account       = module.general_infra.cloud_build_sa_email
  gcs_main_bucket             = module.general_infra.gcs_main_bucket
  kafka_alert_topic           = module.data_infra.kafka_alert_topic
  kafka_bootstrap             = module.data_infra.kafka_bootstrap
  network_id                  = module.general_infra.network_id
  spark_service_account_email = module.data_infra.spark_service_account_email
  spark_tmp_bucket            = module.general_infra.spark_bucket
}
