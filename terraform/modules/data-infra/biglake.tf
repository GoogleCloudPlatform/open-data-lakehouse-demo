resource "google_biglake_iceberg_catalog" "main" {
  project          = var.project_id
  primary_location = var.region
  name             = var.gcs_iceberg_catalog_bucket
  catalog_type     = "CATALOG_TYPE_GCS_BUCKET"
  credential_mode  = "CREDENTIAL_MODE_END_USER"
}
