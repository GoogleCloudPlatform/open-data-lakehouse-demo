variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "service_account_email" {
  type        = string
  description = "Service Account Email"
}

variable "roles" {
  type        = list(string)
  description = "List of IAM roles to assign"
}
