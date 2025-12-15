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

resource "google_service_account" "kafka_service_account" {
  account_id   = "kafka-service-account"
  display_name = "Kafka Service Account"
}

resource "google_service_account" "spark_service_account" {
  account_id   = "spark-service-account"
  display_name = "Spark Service Account"
}


# Assigns the Storage Object Viewer role to the service account, allowing it to read objects in GCS buckets.
resource "google_project_iam_member" "ridership_dataset_sa_gcs_reader" {
  for_each = toset(["roles/storage.objectUser"])
  project  = var.project_id
  role     = each.value

  member = "serviceAccount:${google_bigquery_connection.cloud_resources_connection.cloud_resource[0].service_account_id}"
}

resource "google_project_iam_member" "spark_sa_roles" {
  for_each = toset([
    "roles/dataproc.worker",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectUser",
    "roles/dataproc.editor",
    "roles/logging.logWriter"
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.spark_service_account.email}"
}
