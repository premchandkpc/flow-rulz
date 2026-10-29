# ElastiCache Module Outputs
output "replication_group_id" {
  description = "ElastiCache Replication Group ID"
  value       = aws_elasticache_replication_group.redis.replication_group_id
}

output "primary_endpoint_address" {
  description = "Primary endpoint address"
  value       = aws_elasticache_replication_group.redis.primary_endpoint_address
}

output "primary_endpoint_port" {
  description = "Primary endpoint port"
  value       = aws_elasticache_replication_group.redis.port
}

output "reader_endpoint_address" {
  description = "Reader endpoint address"
  value       = aws_elasticache_replication_group.redis.reader_endpoint_address
}

output "reader_endpoint_port" {
  description = "Reader endpoint port"
  value       = aws_elasticache_replication_group.redis.port
}

output "member_clusters" {
  description = "Member cluster IDs"
  value       = aws_elasticache_replication_group.redis.member_clusters
}

output "security_group_id" {
  description = "ElastiCache Security Group ID"
  value       = aws_security_group.elasticache.id
}

output "subnet_group_name" {
  description = "ElastiCache Subnet Group Name"
  value       = aws_elasticache_subnet_group.redis.name
}

output "parameter_group_name" {
  description = "ElastiCache Parameter Group Name"
  value       = aws_elasticache_parameter_group.redis.name
}