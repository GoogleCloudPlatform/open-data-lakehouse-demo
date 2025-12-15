variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  description = "GCP Region"
}

variable "image_name_and_tag" {
  type        = string
  description = "Full image name and tag"
}

variable "build_service_account" {
  type        = string
  description = "Cloud Build Service Account Email"
}

variable "build_script_path" {
  type        = string
  description = "Path to the build script"
}

variable "trigger_content_hash" {
  type        = string
  description = "Content hash to trigger the build"
}

variable "extra_env_vars" {
  type        = map(string)
  description = "Extra environment variables for the build script"
  default     = {}
}
