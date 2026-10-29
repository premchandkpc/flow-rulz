# FlowRulz production configuration.
# Usage: terraform plan -var-file=environments/production/terraform.tfvars
# Secrets (jwt_secret, grafana_admin_password, trusted_proxy_token) are passed
# via TF_VAR_* environment variables and are never committed here.

environment = "production"
aws_region  = "us-east-1"

availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

# Cluster capacity
eks_node_group_min_size     = 3
eks_node_group_max_size     = 10
eks_node_group_desired_size = 3

# Application capacity
api_replicas    = 2
worker_replicas = 3

# Data layer
rds_instance_class          = "db.r6g.xlarge"
rds_multi_az                = true
rds_deletion_protection     = true
rds_backup_retention_period = 7

# Security
require_auth            = true
enable_network_policies = true

# Tighten the public cluster endpoint before applying (example: VPN or office
# CIDR); the default allows 0.0.0.0/0.
# eks_cluster_endpoint_public_access_cidrs = ["203.0.113.10/32"]

# Observability
enable_prometheus    = true
enable_grafana       = true
enable_loki          = true
prometheus_retention = "15d"
loki_retention       = "30d"
