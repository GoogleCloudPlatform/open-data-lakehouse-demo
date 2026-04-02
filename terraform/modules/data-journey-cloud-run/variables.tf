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

variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  description = "GCP Region"
}

variable "zone" {
  type        = string
  description = "GCP Zone"
}

variable "network_id" {
  type        = string
  description = "GCP Network ID"
}

variable "subnetwork_id" {
  type        = string
  description = "GCP Subnetwork ID"
}

variable "bq_dataset_id" {
  type        = string
  description = "BigQuery Dataset ID"
}

variable "gcs_main_bucket" {
  type        = string
  description = "Main GCS Bucket Name"
}

variable "staging_bq_dataset" {
  type        = string
  description = "Staging BigQuery Dataset ID"
}

variable "bq_connection_name" {
  type        = string
  description = "BigQuery Connection Name"
}

variable "bq_catalog_bucket_name" {
  type        = string
  description = "BigQuery Catalog Bucket Name"
}

variable "rest_catalog_bucket_name" {
  type        = string
  description = "REST Catalog Bucket Name"
}

variable "artifact_repo" {
  type        = string
  description = "Artifact Repository Name"
}

variable "build_service_account" {
  type        = string
  description = "Build Service Account"
}

variable "spark_service_account" {
  type        = string
  description = "Spark Service Account"
}
  