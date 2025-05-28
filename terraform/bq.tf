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


resource "google_bigquery_dataset" "ridership_lakehouse_staging" {
  dataset_id                  = "ridership_lakehouse_staging"
  friendly_name               = "Staging Dataset"
  description                 = "Dataset for Staging data for preparation of ridership data"
  location                    = var.region
  delete_contents_on_destroy  = true
}

resource "google_bigquery_dataset" "ridership_lakehouse" {
  dataset_id                  = "ridership_lakehouse"
  friendly_name               = "Main RidershipDataset"
  description                 = "Dataset for ridership data"
  location                    = var.region
  delete_contents_on_destroy  = true
}


resource "google_bigquery_connection" "cloud_resources_connection" {
  connection_id = "cloud-resources-connection"
  location      = var.region
  friendly_name = "Cloud Resources Connection"

  cloud_resource {
    service_account_id = google_service_account.vertex_ai_sa.account_id
  }
}

resource "google_bigquery_connection" "gcs_connection" {
  connection_id = "gcs-connection"
  location      = "US"
  friendly_name = "GCS Connection"

  cloud_resource {
    service_account_id = google_service_account.gcs_sa.account_id
  }
}
