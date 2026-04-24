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
  artifact_repo        = var.artifact_repo
  image_name           = "open-lakehouse-demo-data-journey"
  data_journey_fileset = setunion(
    fileset("${path.module}/../../../data-journey", "notebooks/**"),
    fileset("${path.module}/../../../data-journey", "Dockerfile"),
    fileset("${path.module}/../../../data-journey", "pyproject.toml"),
    fileset("${path.module}/../../../data-journey", "uv.lock")
  )
  data_journey_content_hash = sha512(join("", [for f in
  local.data_journey_fileset : filesha512("${path.module}/../../../data-journey/${f}")]))
  image_name_and_tag = "${var.region}-docker.pkg.dev/${var.project_id}/${local.artifact_repo}/${local.image_name}:latest"
}

module "container_build" {
  source = "../common/container-build"

  project_id            = var.project_id
  region                = var.region
  image_name_and_tag    = local.image_name_and_tag
  build_service_account = var.build_service_account
  build_script_path     = "${path.module}/scripts/build-data-journey.sh"
  trigger_content_hash  = local.data_journey_content_hash
  

  extra_env_vars = {
    DATA_JOURNEY_DIR = "${path.module}/../../../data-journey"
  }
}

moved {
  from = null_resource.run_build_script
  to   = module.container_build.null_resource.run_build_script
}
