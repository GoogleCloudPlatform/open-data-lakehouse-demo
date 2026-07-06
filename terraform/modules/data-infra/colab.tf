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

resource "google_colab_runtime_template" "runtime-template" {
  name         = "lakehouse-runtime-template"
  display_name = "Runtime for Open Data Lakehouse Demo"
  location     = var.region
  description  = "Runtime for Open Data Lakehouse Demo"
  machine_spec {
    machine_type = "e2-standard-4"
  }
  shielded_vm_config {
    enable_secure_boot = true
  }

  data_persistent_disk_spec {
    disk_type    = "pd-standard"
    disk_size_gb = 200
  }

  network_spec {
    enable_internet_access = false
    network                = var.network_id
    subnetwork             = var.subnetwork_id
  }

  idle_shutdown_config {
    idle_timeout = "3600s"
  }

  software_config {
    env {
      name  = "PROJECT_ID"
      value = var.project_id
    }
    env {
      name  = "LOCATION"
      value = var.region
    }
    env {
      name  = "GENERAL_BUCKET_NAME"
      value = var.gcs_main_bucket
    }
    env {
      name  = "STAGING_BQ_DATASET"
      value = google_bigquery_dataset.ridership_lakehouse_staging.dataset_id
    }
    env {
      name  = "MAIN_BQ_DATASET"
      value = google_bigquery_dataset.ridership_lakehouse.dataset_id
    }
    env {
      name  = "FULL_BQ_CONNECTION_NAME"
      value = google_bigquery_connection.cloud_resources_connection.id
    }
    env {
      name  = "ICEBERG_CATALOG_BUCKET_NAME"
      value = var.gcs_iceberg_catalog_bucket
    }
    env {
      name  = "SUBNETWORK_ID"
      value = var.subnetwork_id
    }
    env {
      name  = "SPARK_SERVICE_ACCOUNT"
      value = google_service_account.backend_service_account.email
    }

    # post_startup_script_config {
    #   post_startup_script          = "echo 'hello world'"
    #   post_startup_script_url      = "gs://colab-enterprise-pss-secure/secure_pss.sh"
    #   post_startup_script_behavior = "RUN_ONCE"
    # }
  }
}
