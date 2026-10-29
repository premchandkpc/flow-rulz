# FlowRulz root module
#
# Composition order (implicit through module outputs):
#   vpc -> eks -> (rds, elasticache, nats, app) -> observability
#
# Providers in providers.tf read their Kubernetes/Helm configuration from
# module.eks, so every Kubernetes/Helm resource implicitly waits for the
# cluster. Secrets are generated here (random provider) when not supplied via
# TF_VAR_* and land in Terraform state, which the S3 backend keeps encrypted.

locals {
  # Must match kubernetes_namespace.app in modules/app (the IRSA trust policy
  # cannot use module.app.namespace without creating a cycle).
  app_namespace = "${var.project_name}-${var.environment}"

  # Must match kubernetes_service_account.app.name in modules/app.
  app_service_account = "flowrulz"

  oidc_provider = module.eks.oidc_provider

  # Empty jwt_secret means "generate one"; a supplied value must be >= 32 chars
  # (enforced by variable validation).
  jwt_secret = var.jwt_secret != "" ? var.jwt_secret : random_password.jwt[0].result

  # Redis authentication requires transit encryption, and the FlowRulz Go
  # client cannot speak TLS yet, so both stay disabled unless explicitly
  # enabled for a TLS-capable client.
  redis_auth_token = var.elasticache_transit_encryption_enabled ? random_password.redis[0].result : ""

  # Empty values mean "generate one" so no component ships with a blank
  # credential.
  grafana_admin_password = var.grafana_admin_password != "" ? var.grafana_admin_password : random_password.grafana[0].result

  database_url = "postgres://${var.rds_username}:${random_password.rds.result}@${module.rds.db_instance_address}:${module.rds.db_instance_port}/${module.rds.db_instance_name}?sslmode=require"

  nats_url = "nats://${module.nats.nats_service_name}:${module.nats.nats_client_port}"

  redis_addr = "${module.elasticache.primary_endpoint_address}:${module.elasticache.primary_endpoint_port}"
}

# -----------------------------------------------------------------------------
# Generated secrets
# -----------------------------------------------------------------------------

# Alphanumeric only so the RDS password is safe to embed in DATABASE_URL
# without URL encoding.
resource "random_password" "rds" {
  length  = 32
  special = false
}

# ElastiCache auth tokens must be 16-128 printable characters. Only generated
# when transit encryption (and therefore authentication) is enabled.
resource "random_password" "redis" {
  count = var.elasticache_transit_encryption_enabled ? 1 : 0

  length  = 32
  special = false
}

resource "random_password" "jwt" {
  count = var.jwt_secret == "" ? 1 : 0

  length  = 48
  special = false
}

resource "random_password" "grafana" {
  count = var.grafana_admin_password == "" ? 1 : 0

  length  = 32
  special = false
}

# -----------------------------------------------------------------------------
# IRSA role for the application service account
# -----------------------------------------------------------------------------

# Trust is scoped to the FlowRulz service account only. The runtime reaches
# PostgreSQL, NATS and Redis inside the VPC and calls no AWS APIs, so the role
# intentionally has no attached permissions yet; attach a policy here if that
# changes.
resource "aws_iam_role" "app_irsa" {
  name = "${var.project_name}-${var.environment}-app-irsa"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Federated = module.eks.oidc_provider_arn
      }
      Action = "sts:AssumeRoleWithWebIdentity"
      Condition = {
        StringEquals = {
          "${local.oidc_provider}:sub" = "system:serviceaccount:${local.app_namespace}:${local.app_service_account}"
          "${local.oidc_provider}:aud" = "sts.amazonaws.com"
        }
      }
    }]
  })

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# -----------------------------------------------------------------------------
# Modules
# -----------------------------------------------------------------------------

module "vpc" {
  source = "./modules/vpc"

  project_name         = var.project_name
  environment          = var.environment
  vpc_cidr             = var.vpc_cidr
  private_subnet_cidrs = var.private_subnet_cidrs
  public_subnet_cidrs  = var.public_subnet_cidrs
  availability_zones   = var.availability_zones
}

module "eks" {
  source = "./modules/eks"

  project_name = var.project_name
  environment  = var.environment
  aws_region   = var.aws_region

  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids

  eks_version                              = var.eks_version
  eks_cluster_endpoint_public_access       = var.eks_cluster_endpoint_public_access
  eks_cluster_endpoint_private_access      = var.eks_cluster_endpoint_private_access
  eks_cluster_endpoint_public_access_cidrs = var.eks_cluster_endpoint_public_access_cidrs

  eks_node_group_instance_types = var.eks_node_group_instance_types
  eks_node_group_min_size       = var.eks_node_group_min_size
  eks_node_group_max_size       = var.eks_node_group_max_size
  eks_node_group_desired_size   = var.eks_node_group_desired_size
  eks_node_group_volume_size    = var.eks_node_group_volume_size
  eks_node_group_volume_type    = var.eks_node_group_volume_type

  enable_spot_instances = var.enable_spot_instances
}

module "rds" {
  source = "./modules/rds"

  project_name = var.project_name
  environment  = var.environment

  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids

  eks_node_group_security_group_id = module.eks.node_group_security_group_id

  rds_instance_class          = var.rds_instance_class
  rds_allocated_storage       = var.rds_allocated_storage
  rds_max_allocated_storage   = var.rds_max_allocated_storage
  rds_engine_version          = var.rds_engine_version
  rds_multi_az                = var.rds_multi_az
  rds_backup_retention_period = var.rds_backup_retention_period
  rds_deletion_protection     = var.rds_deletion_protection
  rds_username                = var.rds_username
  rds_password                = random_password.rds.result
}

module "elasticache" {
  source = "./modules/elasticache"

  project_name = var.project_name
  environment  = var.environment

  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids

  eks_node_group_security_group_id = module.eks.node_group_security_group_id

  elasticache_node_type                  = var.elasticache_node_type
  elasticache_num_cache_nodes            = var.elasticache_num_cache_nodes
  elasticache_engine_version             = var.elasticache_engine_version
  elasticache_port                       = var.elasticache_port
  elasticache_transit_encryption_enabled = var.elasticache_transit_encryption_enabled
  elasticache_auth_token                 = local.redis_auth_token
}

module "nats" {
  source = "./modules/nats"

  project_name = var.project_name
  environment  = var.environment

  vpc_id             = module.vpc.vpc_id
  private_subnet_ids = module.vpc.private_subnet_ids

  eks_node_group_security_group_id = module.eks.node_group_security_group_id

  nats_version                        = var.nats_version
  nats_jetstream_enabled              = var.nats_jetstream_enabled
  nats_jetstream_memory_storage_limit = var.nats_jetstream_memory_storage_limit
  nats_jetstream_disk_storage_limit   = var.nats_jetstream_disk_storage_limit
}

module "app" {
  source = "./modules/app"

  project_name = var.project_name
  environment  = var.environment

  irsa_role_arn = aws_iam_role.app_irsa.arn

  app_repository = var.app_repository
  app_image_tag  = var.app_image_tag
  app_env        = var.app_env
  log_level      = var.log_level

  database_url   = local.database_url
  nats_url       = local.nats_url
  nats_creds     = ""
  redis_addr     = local.redis_addr
  redis_password = local.redis_auth_token

  num_shards                = var.num_shards
  api_replicas              = var.api_replicas
  worker_replicas           = var.worker_replicas
  max_global_in_flight      = var.max_global_in_flight
  fetch_batch_size          = var.fetch_batch_size
  lease_ttl_ms              = var.lease_ttl_ms
  lease_renewal_interval_ms = var.lease_renewal_interval_ms

  api_cpu_request       = var.api_cpu_request
  api_memory_request    = var.api_memory_request
  api_cpu_limit         = var.api_cpu_limit
  api_memory_limit      = var.api_memory_limit
  worker_cpu_request    = var.worker_cpu_request
  worker_memory_request = var.worker_memory_request
  worker_cpu_limit      = var.worker_cpu_limit
  worker_memory_limit   = var.worker_memory_limit

  require_auth        = var.require_auth
  jwt_secret          = local.jwt_secret
  trusted_proxy_token = var.trusted_proxy_token

  enable_network_policies = var.enable_network_policies
}

module "observability" {
  source = "./modules/observability"

  project_name = var.project_name
  environment  = var.environment

  cluster_name  = module.eks.cluster_name
  app_namespace = module.app.namespace

  enable_prometheus      = var.enable_prometheus
  enable_grafana         = var.enable_grafana
  enable_loki            = var.enable_loki
  prometheus_retention   = var.prometheus_retention
  loki_retention         = var.loki_retention
  grafana_admin_password = local.grafana_admin_password
}
