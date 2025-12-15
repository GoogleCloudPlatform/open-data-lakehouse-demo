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
  bus_updates_topic     = "bus-updates"
  capacity_alerts_topic = "capacity-alerts"
}

# Kafka Cluster
resource "google_managed_kafka_cluster" "default" {
  cluster_id = "kafka-cluster"
  location   = var.region
  project    = var.project_id

  capacity_config {
    vcpu_count   = 3
    memory_bytes = 3221225472
  }
  gcp_config {
    access_config {
      network_configs {
        subnet = var.subnetwork_id
      }
    }
  }
  rebalance_config {
    mode = "AUTO_REBALANCE_ON_SCALE_UP"
  }
  labels = {
    key = "value"
  }
  timeouts {
    create = "60m"
  }
}

# Kafka Topics
resource "google_managed_kafka_topic" "bus_updates" {
  topic_id           = local.bus_updates_topic
  cluster            = google_managed_kafka_cluster.default.cluster_id
  location           = var.region
  project            = var.project_id
  partition_count    = 2
  replication_factor = 3
}

resource "google_managed_kafka_topic" "capacity_alerts" {
  topic_id           = local.capacity_alerts_topic
  cluster            = google_managed_kafka_cluster.default.cluster_id
  location           = var.region
  project            = var.project_id
  partition_count    = 2
  replication_factor = 3
}

# Kafka Topics ACLs
resource "google_managed_kafka_acl" "bus_updates_topic_sa_acl" {
  acl_id   = "topic/${local.bus_updates_topic}"
  cluster  = google_managed_kafka_cluster.default.cluster_id
  location = var.region
  project  = var.project_id

  acl_entries {
    principal       = "User:${google_service_account.kafka_service_account.email}"
    permission_type = "ALLOW"
    operation       = "ALL"
    host            = "*"
  }
  acl_entries {
    principal       = "User:service-${data.google_project.project.number}@gcp-sa-managedkafka.iam.gserviceaccount.com"
    permission_type = "ALLOW"
    operation       = "ALL"
    host            = "*"
  }
}

resource "google_managed_kafka_acl" "capacity_alerts_topic_sa_acl" {
  acl_id   = "topic/${local.capacity_alerts_topic}"
  cluster  = google_managed_kafka_cluster.default.cluster_id
  location = var.region
  project  = var.project_id

  acl_entries {
    principal       = "User:${google_service_account.kafka_service_account.email}"
    permission_type = "ALLOW"
    operation       = "ALL"
    host            = "*"
  }
  acl_entries {
    principal       = "User:service-${data.google_project.project.number}@gcp-sa-managedkafka.iam.gserviceaccount.com"
    permission_type = "ALLOW"
    operation       = "ALL"
    host            = "*"
  }
}

# Kafka Connect Cluster
resource "google_managed_kafka_connect_cluster" "default" {
  project            = var.project_id
  connect_cluster_id = "my-connect-cluster"
  kafka_cluster      = google_managed_kafka_cluster.default.id
  location           = var.region
  capacity_config {
    vcpu_count   = 12
    memory_bytes = 21474836480
  }
  gcp_config {
    access_config {
      network_configs {
        primary_subnet = var.subnetwork_id
        dns_domain_names = [
          "${google_managed_kafka_cluster.default.cluster_id}.us-central1.managedkafka.${var.project_id}.cloud.goog"
        ]
      }
    }
  }
  timeouts {
    create = "60m"
  }
}

# Kafka Connect Connectors
resource "google_managed_kafka_connector" "bus-updates-bigquery-sink-connector" {
  project         = var.project_id
  connector_id    = "bus-updates-bigquery-sink-connector"
  connect_cluster = google_managed_kafka_connect_cluster.default.connect_cluster_id
  location        = var.region

  configs = {
    "name"                           = "bus-updates-bigquery-sink-connector"
    "project"                        = var.project_id
    "topics"                         = google_managed_kafka_topic.bus_updates.topic_id
    "tasks.max"                      = "3"
    "connector.class"                = "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
    "key.converter"                  = "org.apache.kafka.connect.storage.StringConverter"
    "value.converter"                = "org.apache.kafka.connect.json.JsonConverter"
    "value.converter.schemas.enable" = "false"
    "defaultDataset"                 = google_bigquery_dataset.ridership_lakehouse.dataset_id
  }

  lifecycle {
    ignore_changes = [task_restart_policy]
  }

}

resource "google_managed_kafka_connector" "capactiy-alerts-bigquery-sink-connector" {
  project         = var.project_id
  connector_id    = "capacity-alerts-bigquery-sink-connector"
  connect_cluster = google_managed_kafka_connect_cluster.default.connect_cluster_id
  location        = var.region

  configs = {
    "name"                           = "capacity-alerts-bigquery-sink-connector"
    "project"                        = var.project_id
    "topics"                         = google_managed_kafka_topic.capacity_alerts.topic_id
    "tasks.max"                      = "3"
    "connector.class"                = "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
    "key.converter"                  = "org.apache.kafka.connect.storage.StringConverter"
    "value.converter"                = "org.apache.kafka.connect.json.JsonConverter"
    "value.converter.schemas.enable" = "false"
    "defaultDataset"                 = google_bigquery_dataset.ridership_lakehouse.dataset_id
  }

  lifecycle {
    ignore_changes = [task_restart_policy]
  }
}
