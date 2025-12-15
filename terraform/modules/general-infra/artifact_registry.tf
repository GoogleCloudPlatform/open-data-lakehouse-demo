locals {
  artifact_repo = "open-lakehouse-demo-docker-repo"
}

resource "google_artifact_registry_repository" "docker_repo" {
  project       = var.project_id
  format        = "DOCKER"
  location      = var.region
  repository_id = local.artifact_repo
  description   = "Docker containers"

  depends_on = [
    time_sleep.wait_for_services
  ]
}
