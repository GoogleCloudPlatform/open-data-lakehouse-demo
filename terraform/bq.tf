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
# See the License for the specific language govemrning permissions and
# limitations under the License.

# Creates a BigQuery dataset for staging ridership data.
# This dataset is used for temporary storage and preparation of data
# before it's loaded into the main data lakehouse.
resource "google_bigquery_dataset" "ridership_lakehouse_staging" {
  dataset_id                  = "ridership_lakehouse_staging"
  friendly_name               = "Staging Dataset"
  description                 = "Dataset for Staging data for preparation of ridership data"
  location                    = var.region
  delete_contents_on_destroy  = true # Ensures that the dataset and its contents are deleted when the resource is destroyed.
}

# Creates the main BigQuery dataset for the ridership data lakehouse.
# This dataset will store the curated and processed ridership data.
resource "google_bigquery_dataset" "ridership_lakehouse" {
  dataset_id                  = "ridership_lakehouse"
  friendly_name               = "Main RidershipDataset"
  description                 = "Dataset for ridership data"
  location                    = var.region
  delete_contents_on_destroy  = true # Ensures that the dataset and its contents are deleted when the resource is destroyed.
}

# Creates a Google Service Account for the ridership dataset.
# This service account is used to grant permissions to BigQuery
# to access other Google Cloud resources like GCS (for Iceberg/Parquet files)
# and Vertex AI (for calling ML models).
resource "google_service_account" "ridership_dataset_sa" {
  account_id = "ridership_dataset_sa"
}

# Creates a BigQuery connection resource.
# This connection enables BigQuery to interact with external cloud resources
# using the permissions granted to the specified service account.
resource "google_bigquery_connection" "cloud_resources_connection" {
  connection_id = "cloud-resources-connection"
  location      = var.region
  friendly_name = "Cloud Resources Connection"

  cloud_resource {
    # Associates the previously created service account with this connection.
    service_account_id = google_service_account.ridership_dataset_sa.account_id
  }
}