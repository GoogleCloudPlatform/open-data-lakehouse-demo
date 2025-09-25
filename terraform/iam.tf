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

# Grant the Dataproc Worker role to the default compute service account.
# This is required for Dataproc Serverless jobs to run with the default compute SA.
resource "google_project_iam_member" "extra_roles_for_compute_sa" {
  for_each = toset([
    "roles/dataproc.worker", 
    "roles/iam.serviceAccountUser", 
    "roles/iam.serviceAccountTokenCreator", 
    "roles/managedkafka.client", 
    "roles/iam.serviceAccountOpenIdTokenCreator",
    "roles/iam.workloadIdentityUser",
    ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}
