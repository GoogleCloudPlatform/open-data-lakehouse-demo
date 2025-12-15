
resource "google_service_account" "data_journey_service_account" {
  account_id   = "data-journey-service-account"
  display_name = "Data Journey Service Account"
}

module "app_iam" {
  source = "../common/app-iam"

  project_id            = var.project_id
  service_account_email = google_service_account.data_journey_service_account.email
  roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectUser",
    "roles/biglake.admin",
    "roles/dataproc.editor",
    "roles/dataproc.worker",
    "roles/bigquery.connectionAdmin",
    "roles/iam.serviceAccountUser",
    "roles/storage.expressModeUserAccess",
    "roles/managedkafka.client",
    "roles/storage.admin",
    "roles/logging.logWriter",
    "roles/serviceusage.serviceUsageConsumer",
    "roles/iam.serviceAccountUser"
  ]
}

