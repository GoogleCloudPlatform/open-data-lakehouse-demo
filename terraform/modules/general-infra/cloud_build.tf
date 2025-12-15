resource "google_service_account" "cloud_build_account" {
  project      = var.project_id
  account_id   = "cloud-build"
  display_name = "Cloud Build Service Account"
  description  = "specific custom service account for Cloud Build"

  depends_on = [
    time_sleep.wait_for_services
  ]
}

resource "google_project_iam_member" "cloud_build_roles" {
  for_each = toset([
    "roles/logging.logWriter",
    "roles/storage.admin",
    "roles/artifactregistry.writer",
    "roles/run.developer",
    "roles/iam.serviceAccountUser",
  ])

  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.cloud_build_account.email}"
}

# Propagation time for change of access policy typically takes 2 minutes
# according to https://cloud.google.com/iam/docs/access-change-propagation
# this wait make sure the policy changes are propagated before proceeding
# with the build
resource "time_sleep" "wait_for_cloud_build_policy_propagation" {
  create_duration = "120s"
  depends_on = [
    google_service_account.cloud_build_account,
    google_project_iam_member.cloud_build_roles,
  ]
}
