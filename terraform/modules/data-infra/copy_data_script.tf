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

# This resource runs the script to copy data from our open access bucket to the main bucket
resource "null_resource" "run_copy_script" {
  provisioner "local-exec" {
    command = "${path.module}/scripts/copy-data.sh"

    environment = {
      BUCKET_NAME = var.gcs_main_bucket
    }
  }
  triggers = {
    always_run = timestamp()
  }
}
