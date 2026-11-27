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


module "project_services" {
  source                      = "terraform-google-modules/project-factory/google//modules/project_services"
  version                     = "~> 18.0"
  project_id                  = var.project_id
  disable_services_on_destroy = false
  disable_dependent_services  = false
  activate_apis = [
    # General container build and registry
    "artifactregistry.googleapis.com",
    "cloudbuild.googleapis.com",
    "run.googleapis.com",
    "compute.googleapis.com",
    "managedkafka.googleapis.com",
    "aiplatform.googleapis.com",
    "dataform.googleapis.com",
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "bigqueryconnection.googleapis.com",
    "bigquerystorage.googleapis.com",
    "biglake.googleapis.com",
    "dataplex.googleapis.com",
    "datalineage.googleapis.com",
    "iam.googleapis.com",
    "logging.googleapis.com",
    "vpcaccess.googleapis.com",
    "servicenetworking.googleapis.com",
  ]

  # Provision service identities and bind required roles before downstream resources.
  activate_api_identities = [
    {
      "api" : "cloudbuild.googleapis.com",
      "roles" : [
        "roles/run.developer",
      ]
    },
    {
      "api" : "dataplex.googleapis.com",
      "roles" : [
        "roles/bigquery.dataViewer",
        "roles/bigquery.jobUser",
        "roles/biglake.viewer",
      ]
    },
  ]
}

data "google_project" "project" {
  project_id = var.project_id
}
