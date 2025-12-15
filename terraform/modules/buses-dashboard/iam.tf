
resource "google_service_account" "cloudrun_sa" {
  project      = var.project_id
  account_id   = "buses-dashboard-run-sa"
  display_name = "Cloud Run Service Account for Buses Dashboard"
  description  = "Service account for the Cloud Run service to run PySpark jobs and interact with BigQuery and GCS."
}

# Assigns the Storage Object Viewer role to the service account, allowing it to read objects in GCS buckets.
module "app_iam" {
  source = "../common/app-iam"

  project_id            = var.project_id
  service_account_email = google_service_account.cloudrun_sa.email
  roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/iam.serviceAccountUser",
    "roles/dataproc.editor",
    "roles/dataproc.worker",
    "roles/storage.expressModeUserAccess",
    "roles/managedkafka.client"
  ]
}

