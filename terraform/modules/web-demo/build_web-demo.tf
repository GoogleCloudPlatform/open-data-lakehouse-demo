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
  artifact_repo = var.artifact_repo
  image_name    = "open-lakehouse-demo-webapp"
  files_root    = "${path.module}/../../../web-demo"
  web_demo_fileset = setunion(
    fileset(local.files_root, "**/*.py"),
    fileset(local.files_root, "Dockerfile"),
    fileset(local.files_root, "pyproject.toml"),
    fileset(local.files_root, "uv.lock"),
  )
  web_demo_content_hash = sha512(join("", [for f in
  local.web_demo_fileset : filesha512("${path.module}/../../../web-demo/${f}")]))
  image_name_and_tag = "${var.region}-docker.pkg.dev/${var.project_id}/${local.artifact_repo}/${local.image_name}:latest"
}

resource "terraform_data" "build_trigger" {
  input = local.web_demo_content_hash
}


resource "local_file" "config_file" {
  lifecycle {
    replace_triggered_by = [
      terraform_data.build_trigger
    ]
  }
  content = templatefile("${path.module}/templates/app_config.yaml.template", {
    project_id : var.project_id
    location : var.region
    full_bq_connection_name : var.full_bq_connection_name
    gcs_bucket_name : var.gcs_main_bucket
    iceberg_catalog_bucket_name : var.iceberg_catalog_bucket_name
    subnetwork_id : var.subnetwork_id
    spark_service_account : var.spark_service_account_email
    staging_bq_dataset : var.bq_staging_dataset_id
    main_bq_dataset : var.bq_dataset_id
    kafka_bootstrap : var.kafka_bootstrap
    kafka_topic : var.kafka_topic
    kafka_alert_topic : var.kafka_alert_topic
    spark_tmp_bucket : var.spark_tmp_bucket

  })
  filename = "${local.files_root}/web_demo/app_config.yaml"
}

module "container_build" {
  source = "../common/container-build"

  project_id            = var.project_id
  region                = var.region
  image_name_and_tag    = local.image_name_and_tag
  build_service_account = var.build_service_account
  build_script_path     = "${path.module}/scripts/build-web-demo.sh"
  trigger_content_hash  = local.web_demo_content_hash

  extra_env_vars = {
    WEBAPP_DIR = local.files_root
  }
}

moved {
  from = null_resource.run_build_script
  to   = module.container_build.null_resource.run_build_script
}
