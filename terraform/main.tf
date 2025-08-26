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
  source = "github.com/terraform-google-modules/terraform-google-project-factory.git//modules/project_services?ref=ff00ab5032e7f520eb3961f133966c6ced4fd5ee"
  # commit hash of version 17.0.0
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
    "bigqueryconnection.googleapis.com",
    "bigquerystorage.googleapis.com",
  ]

  # Provide more access to the cloudbuild service account
  activate_api_identities = [
    {
      "api" : "cloudbuild.googleapis.com",
      "roles" : [
        "roles/run.admin",
        # Required for Cloud Run to launch as the normal compute service account
        "roles/iam.serviceAccountUser",
      ]
    },
    {
      "api" : "pubsub.googleapis.com",
      # PubSub publish to Cloud Run
      "roles" : [
        #"roles/iam.serviceAccountUser",
        "roles/iam.serviceAccountTokenCreator",
      ],
    }
  ]
}

data "google_project" "project" {
  project_id = var.project_id
}