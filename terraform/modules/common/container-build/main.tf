terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 4.0"
    }
  }
}

resource "null_resource" "run_build_script" {
  provisioner "local-exec" {
    command = var.build_script_path

    environment = merge({
      PROJECT_ID      = var.project_id
      REGION          = var.region
      TAG             = var.image_name_and_tag
      SERVICE_ACCOUNT = var.build_service_account
    }, var.extra_env_vars)
  }

  triggers = {
    always_run = var.trigger_content_hash
  }
}
