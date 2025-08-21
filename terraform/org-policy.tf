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


resource "google_project_organization_policy" "org_policy_vm_external_ip_access" {
  project    = var.project_id
  constraint = "compute.vmExternalIpAccess"
  list_policy {
    allow {
      all = true
    }
  }
}

resource "google_project_organization_policy" "org_policy_require_shielded_vm" {
  project    = var.project_id
  constraint = "compute.requireShieldedVm"
  boolean_policy {
    enforced = false
  }
}

resource "google_project_organization_policy" "org_policy_require_os_login" {
  project    = var.project_id
  constraint = "compute.requireOsLogin"
  boolean_policy {
    enforced = false
  }
}

resource "google_project_organization_policy" "allow_policy_member_domains" {
  constraint = "constraints/iam.allowedPolicyMemberDomains"
  project    = var.project_id
  list_policy {
    allow {
      all = true
    }
  }
}
