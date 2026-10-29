# Observability Module Variables
variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "cluster_name" {
  description = "EKS Cluster Name"
  type        = string
}

variable "app_namespace" {
  description = "Application Kubernetes Namespace"
  type        = string
}

variable "enable_prometheus" {
  description = "Enable Prometheus monitoring stack"
  type        = bool
  default     = true
}

variable "enable_grafana" {
  description = "Enable Grafana dashboards"
  type        = bool
  default     = true
}

variable "enable_loki" {
  description = "Enable Loki log aggregation"
  type        = bool
  default     = true
}

variable "enable_nats_monitoring" {
  description = "Enable NATS monitoring"
  type        = bool
  default     = true
}

variable "prometheus_chart_version" {
  description = "Prometheus Helm chart version"
  type        = string
  default     = "45.0.0"
}

variable "prometheus_retention" {
  description = "Prometheus data retention period"
  type        = string
  default     = "15d"
}

variable "prometheus_storage_size" {
  description = "Prometheus storage size"
  type        = string
  default     = "50Gi"
}

variable "prometheus_storage_class" {
  description = "Prometheus storage class"
  type        = string
  default     = "gp3"
}

variable "loki_chart_version" {
  description = "Loki Helm chart version"
  type        = string
  default     = "2.9.0"
}

variable "loki_retention" {
  description = "Loki data retention period"
  type        = string
  default     = "30d"
}

variable "loki_storage_size" {
  description = "Loki storage size"
  type        = string
  default     = "50Gi"
}

variable "loki_storage_class" {
  description = "Loki storage class"
  type        = string
  default     = "gp3"
}

variable "grafana_admin_password" {
  description = "Grafana admin password"
  type        = string
  sensitive   = true
  default     = ""
}

variable "grafana_domain" {
  description = "Grafana domain name"
  type        = string
  default     = "grafana.example.com"
}