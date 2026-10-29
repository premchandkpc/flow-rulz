# ElastiCache Module Variables
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

variable "elasticache_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.r6g.xlarge"
}

variable "elasticache_num_cache_nodes" {
  description = "Number of cache nodes"
  type        = number
  default     = 2

  validation {
    condition     = var.elasticache_num_cache_nodes >= 2
    error_message = "elasticache_num_cache_nodes must be at least 2 so automatic failover has a replica."
  }
}

variable "elasticache_engine_version" {
  description = "Redis engine version"
  type        = string
  default     = "7.2"
}

variable "elasticache_port" {
  description = "Redis port"
  type        = number
  default     = 6379
}

variable "elasticache_transit_encryption_enabled" {
  description = "Require TLS for Redis connections. Only enable this when every client supports TLS; the current FlowRulz Go client does not."
  type        = bool
  default     = false
}

variable "elasticache_auth_token" {
  description = "Redis auth token (requires transit encryption; leave empty to disable authentication)"
  type        = string
  sensitive   = true
  default     = ""
}