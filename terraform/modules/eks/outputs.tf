# EKS Module Outputs
output "cluster_id" {
  description = "EKS Cluster ID"
  value       = module.eks.cluster_id
}

output "cluster_name" {
  description = "EKS Cluster Name"
  value       = module.eks.cluster_name
}

output "cluster_endpoint" {
  description = "EKS Cluster Endpoint"
  value       = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  description = "EKS Cluster Certificate Authority Data"
  value       = module.eks.cluster_certificate_authority_data
}

output "cluster_arn" {
  description = "EKS Cluster ARN"
  value       = module.eks.cluster_arn
}

output "cluster_version" {
  description = "EKS Cluster Version"
  value       = module.eks.cluster_version
}

output "cluster_security_group_id" {
  description = "EKS Cluster Security Group ID"
  value       = aws_security_group.eks_cluster.id
}

output "node_group_security_group_id" {
  description = "EKS Node Group Security Group ID"
  value       = aws_security_group.eks_node_group.id
}

output "kms_key_arn" {
  description = "KMS Key ARN for EKS secrets encryption"
  value       = aws_kms_key.eks_secrets.arn
}

output "kms_key_id" {
  description = "KMS Key ID for EKS secrets encryption"
  value       = aws_kms_key.eks_secrets.key_id
}

output "node_group_role_arn" {
  description = "IAM Role ARN for EKS Node Groups"
  value       = aws_iam_role.eks_node_group.arn
}

output "node_group_role_name" {
  description = "IAM Role Name for EKS Node Groups"
  value       = aws_iam_role.eks_node_group.name
}

output "oidc_provider_arn" {
  description = "OIDC Provider ARN"
  value       = module.eks.oidc_provider_arn
}

output "oidc_provider" {
  description = "OIDC provider URL without the https:// scheme (use for IRSA condition keys)"
  value       = module.eks.oidc_provider
}