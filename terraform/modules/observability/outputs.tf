# Observability Module Outputs
output "prometheus_namespace" {
  description = "Prometheus Kubernetes Namespace"
  value       = var.enable_prometheus ? kubernetes_namespace.monitoring.metadata[0].name : null
}

output "grafana_namespace" {
  description = "Grafana Kubernetes Namespace"
  value       = var.enable_grafana ? kubernetes_namespace.monitoring.metadata[0].name : null
}

output "loki_namespace" {
  description = "Loki Kubernetes Namespace"
  value       = var.enable_loki ? kubernetes_namespace.logging.metadata[0].name : null
}

output "prometheus_service_name" {
  description = "Prometheus Service Name"
  value       = var.enable_prometheus ? "prometheus-kube-prometheus-prometheus" : null
}

output "grafana_service_name" {
  description = "Grafana Service Name"
  value       = var.enable_grafana ? "prometheus-grafana" : null
}

output "loki_service_name" {
  description = "Loki Service Name"
  value       = var.enable_loki ? "loki" : null
}

output "alertmanager_service_name" {
  description = "Alertmanager Service Name"
  value       = var.enable_prometheus ? "prometheus-kube-prometheus-alertmanager" : null
}