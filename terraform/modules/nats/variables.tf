# NATS Module Variables
variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "eks_node_group_security_group_id" {
  description = "EKS Node Group Security Group ID"
  type        = string
}

variable "nats_version" {
  description = "NATS version to deploy"
  type        = string
  default     = "2.11"
}

variable "nats_helm_chart_version" {
  description = "NATS Helm chart version"
  type        = string
  default     = "1.2.0"
}

variable "nats_image_repository" {
  description = "NATS Docker image repository"
  type        = string
  default     = "nats"
}

variable "nats_jetstream_enabled" {
  description = "Enable JetStream on NATS"
  type        = bool
  default     = true
}

variable "nats_jetstream_memory_storage_limit" {
  description = "JetStream memory storage limit (e.g., 4Gi)"
  type        = string
  default     = "4Gi"
}

variable "nats_jetstream_disk_storage_limit" {
  description = "JetStream disk storage limit (e.g., 50Gi)"
  type        = string
  default     = "50Gi"
}

variable "nats_cluster_size" {
  description = "Number of NATS server replicas"
  type        = number
  default     = 3
}