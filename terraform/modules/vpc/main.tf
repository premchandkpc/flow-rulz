# VPC Module
# Creates a VPC with public and private subnets across multiple AZs

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = "${var.project_name}-${var.environment}-vpc"
  cidr = var.vpc_cidr

  azs             = var.availability_zones
  private_subnets = var.private_subnet_cidrs
  public_subnets  = var.public_subnet_cidrs

  enable_nat_gateway   = true
  single_nat_gateway   = var.environment != "production"
  enable_dns_hostnames = true
  enable_dns_support   = true

  # Enable VPC Flow Logs for security auditing
  enable_flow_log                           = true
  flow_log_destination_type                 = "cloud-watch-logs"
  create_flow_log_cloudwatch_log_group      = true
  flow_log_cloudwatch_log_group_name_prefix = "/aws/vpc/flowlogs/${var.project_name}-${var.environment}-"
  flow_log_traffic_type                     = "ALL"

  # VPC Tags
  tags = {
    Name        = "${var.project_name}-${var.environment}-vpc"
    Environment = var.environment
  }

  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }

  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

# VPC Endpoints for AWS services (cost optimization and security)
# These are standalone resources because their subnet IDs are outputs of
# module.vpc; passing those outputs into module.vpc's own arguments would be a
# self-reference cycle.
locals {
  interface_vpc_endpoints = {
    ecr_api        = "ecr.api"
    ecr_dkr        = "ecr.dkr"
    logs           = "logs"
    monitoring     = "monitoring"
    sts            = "sts"
    kms            = "kms"
    secretsmanager = "secretsmanager"
  }
}

data "aws_region" "current" {}

# Security group for interface endpoints: HTTPS from within the VPC only
resource "aws_security_group" "vpc_endpoints" {
  name        = "${var.project_name}-${var.environment}-vpc-endpoints"
  description = "Security group for interface VPC endpoints"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-vpc-endpoints"
  }
}

resource "aws_vpc_endpoint" "interface" {
  for_each = local.interface_vpc_endpoints

  vpc_id              = module.vpc.vpc_id
  service_name        = "com.amazonaws.${data.aws_region.current.name}.${each.value}"
  vpc_endpoint_type   = "Interface"
  subnet_ids          = module.vpc.private_subnets
  security_group_ids  = [aws_security_group.vpc_endpoints.id]
  private_dns_enabled = true

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-${each.key}"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id            = module.vpc.vpc_id
  service_name      = "com.amazonaws.${data.aws_region.current.name}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = module.vpc.private_route_table_ids

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-s3"
  }
}