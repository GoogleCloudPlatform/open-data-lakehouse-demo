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


# Creates a BigQuery dataset for optional Part 0 data-generation staging.
# Notebook 0 drops this dataset upon completion; Parts 1-3 do not depend on it.
resource "google_bigquery_dataset" "ridership_lakehouse_staging" {
  project                    = var.project_id
  dataset_id                 = "ridership_lakehouse_staging"
  friendly_name              = "Staging Dataset"
  description                = "Dataset for optional Part 0 staging of ridership data"
  location                   = var.region
  delete_contents_on_destroy = true
  # Ensures that the dataset and its contents are deleted when the resource is destroyed.
}

# Creates the main BigQuery dataset for the ridership data lakehouse.
# Stores curated/processed ridership data and exported Knowledge Catalog Auto Data
# Quality scan results (bus_rides_dq_results, bus_rides_features_dq_results).
resource "google_bigquery_dataset" "ridership_lakehouse" {
  project                    = var.project_id
  dataset_id                 = "ridership_lakehouse"
  friendly_name              = "Main RidershipDataset"
  description                = "Dataset for ridership data"
  location                   = var.region
  delete_contents_on_destroy = true
  # Ensures that the dataset and its contents are deleted when the resource is destroyed.
}

# Creates a BigQuery connection resource.
# This connection enables BigQuery to interact with external cloud resources
# using the permissions granted to the specified service account.
resource "google_bigquery_connection" "cloud_resources_connection" {
  connection_id = "cloud-resources-connection"
  location      = var.region
  project       = var.project_id
  friendly_name = "Cloud Resources Connection"

  cloud_resource {}
}


# Assigns Storage Object User to the BigQuery connection service account so BigQuery
# Managed Iceberg tables can read and write Parquet/metadata files in Cloud Storage.
resource "google_project_iam_member" "ridership_dataset_sa_gcs_reader" {
  for_each = toset(["roles/storage.objectUser"])
  project = var.project_id
  role    = each.value

  member = "serviceAccount:${google_bigquery_connection.cloud_resources_connection.cloud_resource[0].service_account_id}"
}

output "staging_dataset_id" {
  value = google_bigquery_dataset.ridership_lakehouse_staging.dataset_id
}

output "main_dataset_id" {
  value = google_bigquery_dataset.ridership_lakehouse.dataset_id
}

output "bq_connection_id" {
  value = google_bigquery_connection.cloud_resources_connection.connection_id
}