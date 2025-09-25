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
  cidr = "10.0.0.0/16"
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
  # destination_ranges = [local.cidr]
  direction = "INGRESS"
  priority  = 1000
}

resource "google_compute_firewall" "allow_kafka_ingress" {
  project = var.project_id
  name    = "allow-kafka-ingress"
  network = google_compute_network.open-lakehouse-network.id

  allow {
    protocol = "tcp"
    ports    = ["9092", "9093", "9101", "29092"]
  }

  source_ranges = ["0.0.0.0/0"]
  direction     = "INGRESS"
  priority      = 1000
}

// setup nat router, as our spark job might need to access maven
resource "google_compute_router" "nat-router" {
  name    = "nat-router"
  project = var.project_id
  region  = "${var.region}"
  network = google_compute_network.open-lakehouse-network.id

  depends_on = [
    google_compute_firewall.allow_internal_ingress
  ]
}

resource "google_compute_router_nat" "nat-config" {
  name                               = "nat-config"
  project                            = var.project_id
  router                             = "${google_compute_router.nat-router.name}"
  region                             = "${var.region}"
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"

  depends_on = [
    google_compute_router.nat-router
  ]
}

# Create an IP address
resource "google_compute_global_address" "private_ip_alloc" {
  name          = "private-ip-alloc"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = google_compute_network.open-lakehouse-network.id
}

# Create a private connection
resource "google_service_networking_connection" "default" {
  network                 = google_compute_network.open-lakehouse-network.id
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_alloc.name]
}

# (Optional) Import or export custom routes
resource "google_compute_network_peering_routes_config" "peering_routes" {
  peering = google_service_networking_connection.default.peering
  network = google_compute_network.open-lakehouse-network.name

  import_custom_routes = true
  export_custom_routes = true
  import_subnet_routes_with_public_ip = true
  export_subnet_routes_with_public_ip = true
}


