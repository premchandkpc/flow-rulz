# ElastiCache Module
# Creates an ElastiCache Redis cluster with replication

resource "aws_security_group" "elasticache" {
  name        = "${var.project_name}-${var.environment}-elasticache"
  description = "Security group for ElastiCache Redis"
  vpc_id      = var.vpc_id

  # Allow inbound from EKS node groups
  ingress {
    from_port       = var.elasticache_port
    to_port         = var.elasticache_port
    protocol        = "tcp"
    security_groups = [var.eks_node_group_security_group_id]
  }

  # Allow all outbound
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-elasticache"
  }
}

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${var.project_name}-${var.environment}-redis-subnet"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_parameter_group" "redis" {
  name   = "${var.project_name}-${var.environment}-redis-${var.elasticache_engine_version}"
  family = "redis${replace(var.elasticache_engine_version, "\\.", "")}"

  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }

  parameter {
    name  = "timeout"
    value = "300"
  }

  parameter {
    name  = "tcp-keepalive"
    value = "60"
  }
}

# CloudWatch log groups for ElastiCache log delivery
resource "aws_cloudwatch_log_group" "elasticache_slow_log" {
  name              = "/aws/elasticache/${var.project_name}-${var.environment}/slow-log"
  retention_in_days = 30

  tags = {
    Environment = var.environment
  }
}

resource "aws_cloudwatch_log_group" "elasticache_engine_log" {
  name              = "/aws/elasticache/${var.project_name}-${var.environment}/engine-log"
  retention_in_days = 30

  tags = {
    Environment = var.environment
  }
}

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${var.project_name}-${var.environment}-redis"
  description          = "Redis cluster for ${var.project_name} ${var.environment}"

  engine               = "redis"
  engine_version       = var.elasticache_engine_version
  node_type            = var.elasticache_node_type
  port                 = var.elasticache_port
  parameter_group_name = aws_elasticache_parameter_group.redis.name

  # Non-cluster mode: the application dials the primary endpoint with a
  # standalone Redis client, which cluster mode (num_node_groups) would not
  # support.
  num_cache_clusters = var.elasticache_num_cache_nodes

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [aws_security_group.elasticache.id]

  automatic_failover_enabled = true
  multi_az_enabled           = true

  at_rest_encryption_enabled = true
  transit_encryption_enabled = var.elasticache_transit_encryption_enabled
  auth_token                 = var.elasticache_auth_token != "" ? var.elasticache_auth_token : null

  snapshot_retention_limit = 7
  snapshot_window          = "03:00-04:00"
  maintenance_window       = "sun:05:00-sun:06:00"

  log_delivery_configuration {
    destination      = aws_cloudwatch_log_group.elasticache_slow_log.name
    destination_type = "cloudwatch-logs"
    log_type         = "slow-log"
    log_format       = "json"
  }

  log_delivery_configuration {
    destination      = aws_cloudwatch_log_group.elasticache_engine_log.name
    destination_type = "cloudwatch-logs"
    log_type         = "engine-log"
    log_format       = "json"
  }

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-redis"
  }
}