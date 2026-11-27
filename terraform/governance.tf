# Copyright 2026 Google LLC
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
  knowledge_catalog_glossary_terms = {
    "demand-metric" = {
      display_name = "Demand Metric"
      description  = "Net passenger pressure at a bus stop computed as (remaining_at_stop - remaining_capacity). Positive values indicate overcrowding where passengers are left behind."
    }
    "bus-capacity" = {
      display_name = "Bus Capacity"
      description  = "Total passenger capacity of a bus trip computed as (seating_capacity + standing_capacity) across S (40), M (55), and L (70) bus sizes."
    }
    "left-behind-passengers" = {
      display_name = "Left Behind Passengers"
      description  = "Count of waiting passengers unable to board a bus because capacity was exhausted (remaining_at_stop). Non-zero values emit real-time capacity-alerts events."
    }
  }
}

# Lakehouse Runtime Catalog (Apache Iceberg REST Catalog) with Credential Vending.
# Note: Auto Data Quality scans (google_dataplex_datascan) are intentionally created
# in Notebooks Part 2 and Part 3 because target Iceberg tables (bus_rides,
# bus_rides_features) do not exist yet during terraform apply.
resource "google_biglake_iceberg_catalog" "ridership_rest_catalog" {
  project          = var.project_id
  name             = google_storage_bucket.iceberg_rest_catalog.name
  primary_location = var.region
  catalog_type     = "CATALOG_TYPE_GCS_BUCKET"
  credential_mode  = "CREDENTIAL_MODE_VENDED_CREDENTIALS"
  deletion_policy  = "DELETE"

  depends_on = [
    module.project_services,
    google_storage_bucket.iceberg_rest_catalog,
  ]
}

# Least-privilege delegated storage access: only the catalog's managed service account
# holds bucket-level objectUser; engines receive short-lived, table-scoped tokens.
resource "google_storage_bucket_iam_member" "iceberg_catalog_vending_sa_storage" {
  bucket = google_storage_bucket.iceberg_rest_catalog.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_biglake_iceberg_catalog.ridership_rest_catalog.biglake_service_account}"
}

# Grant the Cloud Run dashboard SA catalog-scoped read access so queries against the
# live 4-part P.C.N.T BigQuery view (ridership_lakehouse.bus_lines) succeed.
resource "google_biglake_iceberg_catalog_iam_member" "cloudrun_sa_catalog_viewer" {
  project = var.project_id
  name    = google_biglake_iceberg_catalog.ridership_rest_catalog.name
  role    = "roles/biglake.viewer"
  member  = "serviceAccount:${google_service_account.cloudrun_sa.email}"
}

# Grant the Dataplex service agent dataset-scoped Data Editor on ridership_lakehouse
# so Knowledge Catalog Auto Data Quality scans (bus-rides-dq-scan and
# bus-rides-features-dq-scan) can export result tables directly into the main dataset.
resource "google_bigquery_dataset_iam_member" "dataplex_sa_dq_export_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.ridership_lakehouse.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:service-${data.google_project.project.number}@gcp-sa-dataplex.iam.gserviceaccount.com"

  depends_on = [module.project_services]
}

# Knowledge Catalog Business Glossary providing shared transit domain definitions.
resource "google_dataplex_glossary" "ridership_glossary" {
  project      = var.project_id
  location     = var.region
  glossary_id  = "ridership-lakehouse-glossary"
  display_name = "Ridership Open Lakehouse Glossary"
  description  = "Canonical transit domain terms for the Open Data Lakehouse demo."

  depends_on = [module.project_services]
}

resource "google_dataplex_glossary_term" "ridership_terms" {
  for_each = local.knowledge_catalog_glossary_terms

  project      = var.project_id
  location     = var.region
  glossary_id  = google_dataplex_glossary.ridership_glossary.glossary_id
  term_id      = each.key
  parent       = google_dataplex_glossary.ridership_glossary.name
  display_name = each.value.display_name
  description  = each.value.description
}

output "iceberg_rest_catalog_id" {
  description = "Lakehouse Runtime Catalog (Apache Iceberg REST Catalog) identifier"
  value       = google_biglake_iceberg_catalog.ridership_rest_catalog.name
}

output "iceberg_rest_catalog_sa" {
  description = "Managed service account used by the Lakehouse Runtime Catalog for Credential Vending"
  value       = google_biglake_iceberg_catalog.ridership_rest_catalog.biglake_service_account
}

output "knowledge_catalog_glossary_id" {
  description = "Knowledge Catalog Business Glossary ID for transit domain terms"
  value       = google_dataplex_glossary.ridership_glossary.glossary_id
}
