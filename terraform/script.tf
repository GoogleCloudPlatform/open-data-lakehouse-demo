# We use a data source to get a hash of our script files
data "archive_file" "scripts" {
  type        = "zip"
  source_dir  = "${path.module}/scripts"
  output_path = "${path.module}/.terraform/scripts.zip"
}

# This resource runs the script
resource "null_resource" "run_python_script" {
  provisioner "local-exec" {
    command = "${path.module}/scripts/copy-data.sh"

    # Optional: Pass Terraform data to the script as environment variables
    environment = {
      BUCKET_NAME = google_storage_bucket.data_lakehouse_bucket.name
    }
  }
}