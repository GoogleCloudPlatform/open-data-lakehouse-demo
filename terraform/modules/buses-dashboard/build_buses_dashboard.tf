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


# a null resource that is connected to the content_hash from build.tf
# (This might be needed if we want to trigger build on file changes, but we have run_build_buses_dashboard below)
# The original file had deployment_trigger for the default service. We can remove it or adapt it.
# For now, I'll remove the default service parts.

locals {
  buses_dashboard_image_name = "buses-dashboard"
  buses_dashboard_fileset = setunion(
    fileset("${path.module}/../../../buses-dashboard", "src/**"),
    fileset("${path.module}/../../../buses-dashboard", "public/**"),
    fileset("${path.module}/../../../buses-dashboard", "Dockerfile"),
    fileset("${path.module}/../../../buses-dashboard", "package.json"),
    fileset("${path.module}/../../../buses-dashboard", "package-lock.json"),
    fileset("${path.module}/../../../buses-dashboard", "tsconfig.json"),
    fileset("${path.module}/../../../buses-dashboard", "next.config.ts"),
    fileset("${path.module}/../../../buses-dashboard", "postcss.config.mjs"),
    fileset("${path.module}/../../../buses-dashboard", "eslint.config.mjs")
  )
  buses_dashboard_content_hash = sha512(
    join(
      "",
      [
        for f in local.buses_dashboard_fileset :
        filesha512("${path.module}/../../../buses-dashboard/${f}")
      ]
    )
  )
  buses_dashboard_image_name_and_tag = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_repo}/${local.buses_dashboard_image_name}:latest"
}

module "container_build" {
  source = "../common/container-build"

  project_id            = var.project_id
  region                = var.region
  image_name_and_tag    = local.buses_dashboard_image_name_and_tag
  build_service_account = var.build_service_account
  build_script_path     = "${path.module}/scripts/build-buses-dashboard.sh"
  trigger_content_hash  = local.buses_dashboard_content_hash

  extra_env_vars = {
    WEBAPP_DIR = "${path.module}/../../../buses-dashboard"
  }
}

moved {
  from = null_resource.run_build_script
  to   = module.container_build.null_resource.run_build_script
}
