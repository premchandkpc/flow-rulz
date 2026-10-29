# FlowRulz staging configuration.
# Usage: terraform plan -var-file=environments/staging/terraform.tfvars
# Secrets (jwt_secret, grafana_admin_password, trusted_proxy_token) are passed
# via TF_VAR_* environment variables and are never committed here.

environment = "staging"
aws_region  = "us-east-1"

availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

# Cluster capacity
eks_node_group_min_size     = 2
eks_node_group_max_size     = 4
eks_node_group_desired_size = 2

# Application capacity
api_replicas    = 2
worker_replicas = 2

# Data layer (single-AZ, no deletion protection)
rds_instance_class          = "db.r6g.large"
rds_allocated_storage       = 50
rds_multi_az                = false
rds_deletion_protection     = false
rds_backup_retention_period = 3

elasticache_node_type = "cache.r6g.large"

# Security
app_env                 = "staging"
log_level               = "info"
require_auth            = true
enable_network_policies = true

# Observability
enable_prometheus    = true
enable_grafana       = true
enable_loki          = true
prometheus_retention = "3d"
loki_retention       = "7d"
