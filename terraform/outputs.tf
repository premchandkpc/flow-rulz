# Core outputs (always available). Secrets (passwords, tokens, DATABASE_URL)
# are deliberately never exported here.

output "vpc_id" {
  description = "VPC ID"
  value       = module.vpc.vpc_id
}

output "eks_cluster_name" {
  description = "EKS cluster name"
  value       = module.eks.cluster_name
}

output "eks_cluster_endpoint" {
  description = "EKS cluster API endpoint"
  value       = module.eks.cluster_endpoint
}

output "database_endpoint" {
  description = "RDS PostgreSQL endpoint (host:port)"
  value       = module.rds.db_instance_endpoint
}

output "database_name" {
  description = "RDS PostgreSQL database name"
  value       = module.rds.db_instance_name
}

output "redis_primary_endpoint" {
  description = "ElastiCache Redis primary endpoint (host:port)"
  value       = local.redis_addr
}

output "nats_url" {
  description = "In-cluster NATS URL"
  value       = local.nats_url
}

output "app_namespace" {
  description = "Kubernetes namespace hosting FlowRulz workloads"
  value       = module.app.namespace
}

output "app_service_account" {
  description = "Kubernetes service account used by API, worker and console"
  value       = module.app.service_account_name
}

output "app_irsa_role_arn" {
  description = "IAM role ARN trusted by the application service account"
  value       = aws_iam_role.app_irsa.arn
}

# Consolidated view of one deployment; null when enable_outputs is false.
output "deployment_summary" {
  description = "Consolidated deployment details (null when enable_outputs is false)"
  value = var.enable_outputs ? {
    environment            = var.environment
    region                 = var.aws_region
    vpc_id                 = module.vpc.vpc_id
    eks_cluster_name       = module.eks.cluster_name
    eks_cluster_endpoint   = module.eks.cluster_endpoint
    database_endpoint      = module.rds.db_instance_endpoint
    redis_primary_endpoint = local.redis_addr
    nats_url               = local.nats_url
    app_namespace          = module.app.namespace
    app_irsa_role_arn      = aws_iam_role.app_irsa.arn
  } : null
}
