output "bq_dataset_id" {
  value = google_bigquery_dataset.ridership_lakehouse.dataset_id
}

output "kafka_bootstrap" {
  value = "bootstrap.${google_managed_kafka_cluster.default.cluster_id}.${google_managed_kafka_cluster.default.location}.managedkafka.${var.project_id}.cloud.goog:9092"
}

output "kafka_topic" {
  value = google_managed_kafka_topic.bus_updates.topic_id
}

output "kafka_alert_topic" {
  value = google_managed_kafka_topic.capacity_alerts.topic_id
}

output "backend_service_account_email" {
  description = "Service Account Email for Backend services (Kafka, Spark)"
  value       = google_service_account.backend_service_account.email
}

output "bq_connection_name" {
  description = "BigQuery Connection Name"
  value       = google_bigquery_connection.cloud_resources_connection.name
}

output "staging_bq_dataset" {
  description = "Staging BigQuery Dataset ID"
  value       = google_bigquery_dataset.ridership_lakehouse_staging.dataset_id
}
