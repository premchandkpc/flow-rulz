# RDS Module Outputs
output "db_instance_id" {
  description = "RDS Instance ID"
  value       = aws_db_instance.rds.id
}

output "db_instance_arn" {
  description = "RDS Instance ARN"
  value       = aws_db_instance.rds.arn
}

output "db_instance_endpoint" {
  description = "RDS Instance Endpoint"
  value       = aws_db_instance.rds.endpoint
}

output "db_instance_port" {
  description = "RDS Instance Port"
  value       = aws_db_instance.rds.port
}

output "db_instance_address" {
  description = "RDS Instance Address"
  value       = aws_db_instance.rds.address
}

output "db_instance_username" {
  description = "RDS Instance Username"
  value       = aws_db_instance.rds.username
}

output "db_instance_password" {
  description = "RDS Instance Password"
  value       = aws_db_instance.rds.password
  sensitive   = true
}

output "db_instance_name" {
  description = "RDS Instance Database Name"
  value       = aws_db_instance.rds.db_name
}

output "db_security_group_id" {
  description = "RDS Security Group ID"
  value       = aws_security_group.rds.id
}

output "db_subnet_group_name" {
  description = "RDS DB Subnet Group Name"
  value       = aws_db_subnet_group.rds.name
}

output "kms_key_arn" {
  description = "KMS Key ARN for RDS encryption"
  value       = aws_kms_key.rds.arn
}

output "read_replica_endpoint" {
  description = "Read Replica Endpoint (if enabled)"
  value       = var.rds_multi_az && var.environment == "production" ? aws_db_instance.rds_read_replica[0].endpoint : null
}