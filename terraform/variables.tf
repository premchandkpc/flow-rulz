# FlowRulz Terraform Variables
# These variables control the deployment configuration

# =============================================================================
# General Configuration
# =============================================================================
variable "environment" {
  description = "Environment name (e.g., production, staging, development)"
  type        = string
  validation {
    condition     = contains(["production", "staging", "development"], var.environment)
    error_message = "Environment must be production, staging, or development."
  }
}

variable "aws_region" {
  description = "AWS region for deployment"
  type        = string
  default     = "us-east-1"
}

variable "owner" {
  description = "Owner/team responsible for this deployment"
  type        = string
  default     = "platform-team"
}

variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
  default     = "flow-rulz"
}

# =============================================================================
# VPC Configuration
# =============================================================================
variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "private_subnet_cidrs" {
  description = "CIDR blocks for private subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
}

variable "public_subnet_cidrs" {
  description = "CIDR blocks for public subnets (one per AZ)"
  type        = list(string)
  default     = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]
}

variable "availability_zones" {
  description = "Availability zones to use"
  type        = list(string)
  default     = ["us-east-1a", "us-east-1b", "us-east-1c"]
}

# =============================================================================
# EKS Configuration
# =============================================================================
variable "eks_version" {
  description = "EKS Kubernetes version"
  type        = string
  default     = "1.28"
}

variable "eks_cluster_endpoint_public_access" {
  description = "Whether the EKS cluster endpoint is publicly accessible"
  type        = bool
  default     = true
}

variable "eks_cluster_endpoint_private_access" {
  description = "Whether the EKS cluster endpoint has private access"
  type        = bool
  default     = true
}

variable "eks_cluster_endpoint_public_access_cidrs" {
  description = "CIDR blocks allowed to access the public cluster endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

# =============================================================================
# Node Group Configuration
# =============================================================================
variable "eks_node_group_instance_types" {
  description = "Instance types for EKS managed node groups"
  type        = list(string)
  default     = ["m6i.xlarge", "m6i.2xlarge"]
}

variable "eks_node_group_min_size" {
  description = "Minimum size of the node group"
  type        = number
  default     = 3
}

variable "eks_node_group_max_size" {
  description = "Maximum size of the node group"
  type        = number
  default     = 10
}

variable "eks_node_group_desired_size" {
  description = "Desired size of the node group"
  type        = number
  default     = 3
}

variable "eks_node_group_volume_size" {
  description = "Volume size (in GiB) for worker nodes"
  type        = number
  default     = 50
}

variable "eks_node_group_volume_type" {
  description = "Volume type for worker nodes"
  type        = string
  default     = "gp3"
}

# =============================================================================
# PostgreSQL (RDS) Configuration
# =============================================================================
variable "rds_instance_class" {
  description = "RDS instance class"
  type        = string
  default     = "db.r6g.xlarge"
}

variable "rds_allocated_storage" {
  description = "Allocated storage for RDS (in GiB)"
  type        = number
  default     = 100
}

variable "rds_max_allocated_storage" {
  description = "Maximum allocated storage for RDS autoscaling (in GiB)"
  type        = number
  default     = 500
}

variable "rds_engine_version" {
  description = "PostgreSQL engine version"
  type        = string
  default     = "16.2"
}

variable "rds_multi_az" {
  description = "Enable Multi-AZ deployment for RDS"
  type        = bool
  default     = true
}

variable "rds_backup_retention_period" {
  description = "Backup retention period in days"
  type        = number
  default     = 7
}

variable "rds_deletion_protection" {
  description = "Enable deletion protection for RDS"
  type        = bool
  default     = true
}

variable "rds_username" {
  description = "Master username for RDS"
  type        = string
  default     = "flowrulz"
}

# =============================================================================
# ElastiCache (Redis) Configuration
# =============================================================================
variable "elasticache_node_type" {
  description = "ElastiCache node type"
  type        = string
  default     = "cache.r6g.xlarge"
}

variable "elasticache_num_cache_nodes" {
  description = "Number of cache nodes"
  type        = number
  default     = 2
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
  description = "Require TLS for Redis connections. Leave false while the FlowRulz Go client has no TLS support; enabling it without a TLS-capable client breaks application startup."
  type        = bool
  default     = false
}

# =============================================================================
# NATS Configuration
# =============================================================================
variable "nats_version" {
  description = "NATS version to deploy"
  type        = string
  default     = "2.11"
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

# =============================================================================
# Application Configuration
# =============================================================================
variable "app_image_tag" {
  description = "Docker image tag for the FlowRulz application"
  type        = string
  default     = "latest"
}

variable "app_repository" {
  description = "Docker repository for the FlowRulz application images"
  type        = string
  default     = "flowrulz"
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

# =============================================================================
# Application Configuration (Environment Variables)
# =============================================================================
variable "num_shards" {
  description = "Number of shards for event partitioning"
  type        = number
  default     = 4096
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

variable "log_level" {
  description = "Log level for the application"
  type        = string
  default     = "info"
}

variable "app_env" {
  description = "Application environment"
  type        = string
  default     = "production"
}

# =============================================================================
# Security Configuration
# =============================================================================
# Not wired: Pod Security Policies were removed in Kubernetes 1.25+ and no
# module consumes this. Kept as a documented gap (see terraform/README.md).
variable "enable_pod_security_policies" {
  description = "Enable Pod Security Policies (deprecated in K8s 1.25+)"
  type        = bool
  default     = false
}

variable "enable_network_policies" {
  description = "Enable Kubernetes Network Policies"
  type        = bool
  default     = true
}

variable "jwt_secret" {
  description = "JWT secret for authentication (min 32 chars)"
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = var.jwt_secret == "" || length(var.jwt_secret) >= 32
    error_message = "jwt_secret must be empty (one is generated) or at least 32 characters."
  }
}

variable "require_auth" {
  description = "Require authentication for API access"
  type        = bool
  default     = true
}

variable "trusted_proxy_token" {
  description = "Trusted proxy token for production auth"
  type        = string
  sensitive   = true
  default     = ""
}

# =============================================================================
# Observability Configuration
# =============================================================================
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

variable "prometheus_retention" {
  description = "Prometheus data retention period"
  type        = string
  default     = "15d"
}

variable "loki_retention" {
  description = "Loki data retention period"
  type        = string
  default     = "30d"
}

variable "grafana_admin_password" {
  description = "Grafana admin password (min 16 chars); empty means generate one"
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = var.grafana_admin_password == "" || length(var.grafana_admin_password) >= 16
    error_message = "grafana_admin_password must be empty (one is generated) or at least 16 characters."
  }
}

# =============================================================================
# ALB/Ingress Configuration
# =============================================================================
# Not wired: no module installs an ingress controller or ALB yet, so none of
# the alb_* variables are consumed. See terraform/README.md "Known gaps".
variable "alb_certificate_arn" {
  description = "ACM certificate ARN for ALB HTTPS listener"
  type        = string
  default     = ""
}

variable "alb_domain_name" {
  description = "Domain name for ALB ingress"
  type        = string
  default     = ""
}

variable "alb_subdomain" {
  description = "Subdomain for ALB ingress (e.g., api, flow-rulz)"
  type        = string
  default     = "api"
}

variable "alb_hosted_zone_id" {
  description = "Route53 hosted zone ID for DNS records"
  type        = string
  default     = ""
}

# =============================================================================
# Backup/Disaster Recovery Configuration
# =============================================================================
# Not wired: the RDS module creates an in-region read replica for production
# only; cross-region replication and dr_region are not implemented anywhere.
# See terraform/README.md "Known gaps".
variable "enable_cross_region_replication" {
  description = "Enable cross-region replication for RDS"
  type        = bool
  default     = false
}

variable "dr_region" {
  description = "Disaster recovery region"
  type        = string
  default     = "us-west-2"
}

# =============================================================================
# Cost Optimization
# =============================================================================
variable "enable_spot_instances" {
  description = "Enable spot instances for worker nodes"
  type        = bool
  default     = false
}

# Not wired: enable_spot_instances is consumed by the EKS module, but no
# module accepts a separate spot instance type list. See terraform/README.md.
variable "spot_instance_types" {
  description = "Spot instance types (if different from on-demand)"
  type        = list(string)
  default     = ["m6i.xlarge", "m6i.2xlarge", "m5.xlarge", "m5.2xlarge"]
}

# =============================================================================
# Output Configuration
# =============================================================================
variable "enable_outputs" {
  description = "Enable detailed outputs"
  type        = bool
  default     = true
}