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

locals {
  cidr = "10.0.1.0/24"
}

# main VPC
resource "google_compute_network" "open-lakehouse-network" {
  name                    = "open-lakehouse-network"
  project                 = var.project_id
  auto_create_subnetworks = false
}

# VPC Subnet
resource "google_compute_subnetwork" "open-lakehouse-subnetwork" {
  name                     = "${var.region}-open-lakehouse-subnet"
  network                  = google_compute_network.open-lakehouse-network.id
  project                  = var.project_id
  region                   = var.region
  ip_cidr_range            = local.cidr
  private_ip_google_access = true
}

resource "google_compute_firewall" "allow_internal_ingress" {
  project = var.project_id
  name    = "allow-internal-ingress"
  network = google_compute_network.open-lakehouse-network.id

  allow {
    protocol = "all"
  }

  source_ranges = [local.cidr]
  destination_ranges = [local.cidr]
  direction = "INGRESS"
  priority  = 1000
}