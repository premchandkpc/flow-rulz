# App Module Outputs
output "namespace" {
  description = "Application Kubernetes Namespace"
  value       = kubernetes_namespace.app.metadata[0].name
}

output "api_service_name" {
  description = "API Service Name"
  value       = kubernetes_service.api.metadata[0].name
}

output "api_service_namespace" {
  description = "API Service Namespace"
  value       = kubernetes_service.api.metadata[0].namespace
}

output "worker_service_name" {
  description = "Worker Service Name"
  value       = kubernetes_service.worker.metadata[0].name
}

output "worker_service_namespace" {
  description = "Worker Service Namespace"
  value       = kubernetes_service.worker.metadata[0].namespace
}

output "console_service_name" {
  description = "Console Service Name"
  value       = var.enable_console ? kubernetes_service.console[0].metadata[0].name : null
}

output "console_service_namespace" {
  description = "Console Service Namespace"
  value       = var.enable_console ? kubernetes_service.console[0].metadata[0].namespace : null
}

output "config_map_name" {
  description = "ConfigMap Name"
  value       = kubernetes_config_map.app_config.metadata[0].name
}

output "secret_name" {
  description = "Secret Name"
  value       = kubernetes_secret.app_secrets.metadata[0].name
}

output "service_account_name" {
  description = "Service Account Name"
  value       = kubernetes_service_account.app.metadata[0].name
}

output "api_deployment_name" {
  description = "API Deployment Name"
  value       = kubernetes_deployment.api.metadata[0].name
}

output "worker_deployment_name" {
  description = "Worker Deployment Name"
  value       = kubernetes_deployment.worker.metadata[0].name
}