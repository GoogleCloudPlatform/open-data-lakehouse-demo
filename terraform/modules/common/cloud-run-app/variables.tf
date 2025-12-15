variable "project_id" {
  type        = string
  description = "GCP Project ID"
}

variable "region" {
  type        = string
  description = "GCP Region"
}

variable "service_name" {
  type        = string
  description = "Cloud Run Service Name"
}

variable "image" {
  type        = string
  description = "Container Image URL"
}

variable "network_id" {
  type        = string
  description = "VPC Network ID"
}

variable "subnetwork_id" {
  type        = string
  description = "VPC Subnetwork ID"
}

variable "service_account_email" {
  type        = string
  description = "Service Account Email for the Cloud Run service"
}

variable "env_vars" {
  type        = map(string)
  description = "Environment variables"
  default     = {}
}

variable "container_port" {
  type        = number
  description = "Container Port"
  default     = 8080
}

variable "limits" {
  type        = map(string)
  description = "Resource limits"
  default = {
    cpu    = "1000m"
    memory = "2Gi"
  }
}

variable "ingress" {
  type        = string
  description = "Ingress traffic"
  default     = "INGRESS_TRAFFIC_ALL"
}

variable "invoker_iam_members" {
  type        = list(string)
  description = "IAM members to allow invoker access"
  default     = ["allUsers"]
}
