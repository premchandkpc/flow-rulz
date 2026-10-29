# App Module Variables
variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "irsa_role_arn" {
  description = "IAM Role ARN for IRSA"
  type        = string
}

variable "app_repository" {
  description = "Docker repository for the FlowRulz application images"
  type        = string
  default     = "flowrulz"
}

variable "app_image_tag" {
  description = "Docker image tag for the FlowRulz application"
  type        = string
  default     = "latest"
}

variable "app_env" {
  description = "Application environment"
  type        = string
  default     = "production"
}

variable "log_level" {
  description = "Log level for the application"
  type        = string
  default     = "info"
}

variable "console_origin" {
  description = "Console origin URL for CORS"
  type        = string
  default     = "https://console.example.com"
}

# Database Configuration
variable "database_url" {
  description = "Database connection URL"
  type        = string
  sensitive   = true
}

# NATS Configuration
variable "nats_url" {
  description = "NATS connection URL"
  type        = string
}

variable "nats_creds" {
  description = "NATS credentials (if using auth)"
  type        = string
  sensitive   = true
  default     = ""
}

# Redis Configuration
variable "redis_addr" {
  description = "Redis address"
  type        = string
}

variable "redis_password" {
  description = "Redis password"
  type        = string
  sensitive   = true
  default     = ""
}

# Worker Configuration
variable "num_shards" {
  description = "Number of shards for event partitioning"
  type        = number
  default     = 4096
}

variable "api_replicas" {
  description = "Number of API replicas"
  type        = number
  default     = 2
}

variable "worker_replicas" {
  description = "Number of worker replicas"
  type        = number
  default     = 3
}

variable "max_global_in_flight" {
  description = "Maximum global in-flight events"
  type        = number
  default     = 100
}

variable "fetch_batch_size" {
  description = "Fetch batch size for NATS consumer"
  type        = number
  default     = 100
}

variable "lease_ttl_ms" {
  description = "Lease TTL in milliseconds"
  type        = number
  default     = 30000
}

variable "lease_renewal_interval_ms" {
  description = "Lease renewal interval in milliseconds"
  type        = number
  default     = 10000
}

# Resource limits
variable "api_cpu_request" {
  description = "CPU request for API pods"
  type        = string
  default     = "500m"
}

variable "api_memory_request" {
  description = "Memory request for API pods"
  type        = string
  default     = "512Mi"
}

variable "api_cpu_limit" {
  description = "CPU limit for API pods"
  type        = string
  default     = "1000m"
}

variable "api_memory_limit" {
  description = "Memory limit for API pods"
  type        = string
  default     = "1Gi"
}

variable "worker_cpu_request" {
  description = "CPU request for worker pods"
  type        = string
  default     = "1000m"
}

variable "worker_memory_request" {
  description = "Memory request for worker pods"
  type        = string
  default     = "1Gi"
}

variable "worker_cpu_limit" {
  description = "CPU limit for worker pods"
  type        = string
  default     = "2000m"
}

variable "worker_memory_limit" {
  description = "Memory limit for worker pods"
  type        = string
  default     = "2Gi"
}

# Autoscaling
variable "api_min_replicas" {
  description = "Minimum API replicas"
  type        = number
  default     = 2
}

variable "api_max_replicas" {
  description = "Maximum API replicas"
  type        = number
  default     = 10
}

variable "api_target_cpu_utilization" {
  description = "Target CPU utilization for API HPA"
  type        = number
  default     = 70
}

variable "api_target_memory_utilization" {
  description = "Target memory utilization for API HPA"
  type        = number
  default     = 80
}

variable "worker_min_replicas" {
  description = "Minimum worker replicas"
  type        = number
  default     = 2
}

variable "worker_max_replicas" {
  description = "Maximum worker replicas"
  type        = number
  default     = 10
}

variable "worker_target_cpu_utilization" {
  description = "Target CPU utilization for Worker HPA"
  type        = number
  default     = 70
}

variable "worker_target_memory_utilization" {
  description = "Target memory utilization for Worker HPA"
  type        = number
  default     = 80
}

# Security
variable "require_auth" {
  description = "Require authentication for API access"
  type        = bool
  default     = true
}

variable "jwt_secret" {
  description = "JWT secret for authentication (min 32 chars)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "trusted_proxy_token" {
  description = "Trusted proxy token for production auth"
  type        = string
  sensitive   = true
  default     = ""
}

# Features
variable "enable_console" {
  description = "Enable console deployment"
  type        = bool
  default     = true
}

variable "enable_network_policies" {
  description = "Enable Kubernetes Network Policies"
  type        = bool
  default     = true
}