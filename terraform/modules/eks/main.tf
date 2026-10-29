# EKS Module
# Creates an EKS cluster with managed node groups

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 19.0"

  cluster_name    = "${var.project_name}-${var.environment}"
  cluster_version = var.eks_version

  vpc_id                               = var.vpc_id
  subnet_ids                           = var.private_subnet_ids
  cluster_endpoint_public_access       = var.eks_cluster_endpoint_public_access
  cluster_endpoint_private_access      = var.eks_cluster_endpoint_private_access
  cluster_endpoint_public_access_cidrs = var.eks_cluster_endpoint_public_access_cidrs

  # Additional cluster security group (created in this module)
  cluster_additional_security_group_ids = [aws_security_group.eks_cluster.id]

  # EKS Managed Node Groups
  eks_managed_node_group_defaults = {
    ami_type       = "AL2_x86_64"
    instance_types = var.eks_node_group_instance_types

    # Attach the shared node group security group used by RDS, ElastiCache
    # and NATS ingress rules, in addition to the node group's own SG.
    vpc_security_group_ids = [aws_security_group.eks_node_group.id]

    # Use capacity blocks for cost optimization
    capacity_type = var.enable_spot_instances ? "SPOT" : "ON_DEMAND"

    # Labels for workload scheduling
    labels = {
      Environment = var.environment
      NodeGroup   = "general"
      Workload    = "flow-rulz"
    }

    # Taints for system pods
    taints = []

    # Tags
    tags = {
      Environment = var.environment
      NodeGroup   = "general"
    }
  }

  eks_managed_node_groups = {
    general = {
      name           = "general"
      instance_types = var.eks_node_group_instance_types

      min_size     = var.eks_node_group_min_size
      max_size     = var.eks_node_group_max_size
      desired_size = var.eks_node_group_desired_size

      # Volume configuration
      disk_size = var.eks_node_group_volume_size
      disk_type = var.eks_node_group_volume_type

      # Use spot if enabled
      capacity_type = var.enable_spot_instances ? "SPOT" : "ON_DEMAND"

      # Labels for workload scheduling
      labels = {
        Environment = var.environment
        NodeGroup   = "general"
        Workload    = "flow-rulz"
      }

      # Update config for zero-downtime updates
      update_config = {
        max_unavailable_percentage = 25
      }
    }

    # Dedicated node group for worker pods (higher CPU/memory)
    workers = {
      name           = "workers"
      instance_types = ["m6i.2xlarge", "m6i.4xlarge"]

      min_size     = var.worker_min_size
      max_size     = var.worker_max_size
      desired_size = var.worker_desired_size

      disk_size = var.eks_node_group_volume_size
      disk_type = var.eks_node_group_volume_type

      capacity_type = var.enable_spot_instances ? "SPOT" : "ON_DEMAND"

      labels = {
        Environment = var.environment
        NodeGroup   = "workers"
        Workload    = "flow-rulz-workers"
      }

      taints = [
        {
          key    = "workload"
          value  = "flow-rulz-workers"
          effect = "NO_SCHEDULE"
        }
      ]

      update_config = {
        max_unavailable_percentage = 25
      }
    }
  }

  # Fargate profile for system pods (optional)
  fargate_profile_defaults = {
    subnet_ids = var.private_subnet_ids
  }

  fargate_profiles = {
    system = {
      name = "system"
      selectors = [
        {
          namespace = "kube-system"
        },
        {
          namespace = "monitoring"
        },
        {
          namespace = "logging"
        }
      ]
    }
  }

  # Enable IRSA (IAM Roles for Service Accounts)
  enable_irsa = true

  # Cluster encryption (KMS key created in this module)
  cluster_encryption_config = [{
    provider_key_arn = aws_kms_key.eks_secrets.arn
    resources        = ["secrets"]
  }]

  # CloudWatch logging
  cluster_enabled_log_types = ["api", "audit", "authenticator", "controllerManager", "scheduler"]

  # Tags
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Role for EKS Node Groups
resource "aws_iam_role" "eks_node_group" {
  name = "${var.project_name}-${var.environment}-eks-node-group"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })
}

# IAM Policy Attachments for Node Groups
resource "aws_iam_role_policy_attachment" "eks_node_group_AmazonEKSWorkerNodePolicy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_node_group.name
}

resource "aws_iam_role_policy_attachment" "eks_node_group_AmazonEKS_CNI_Policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_node_group.name
}

resource "aws_iam_role_policy_attachment" "eks_node_group_AmazonEC2ContainerRegistryReadOnly" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_node_group.name
}

resource "aws_iam_role_policy_attachment" "eks_node_group_CloudWatchAgentServerPolicy" {
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
  role       = aws_iam_role.eks_node_group.name
}

# Custom policy for node group to access secrets, SSM, etc.
resource "aws_iam_policy" "eks_node_group_custom" {
  name        = "${var.project_name}-${var.environment}-eks-node-group-custom"
  description = "Custom policy for EKS node group"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue",
          "secretsmanager:DescribeSecret",
          "ssm:GetParameter",
          "ssm:GetParameters",
          "ssm:GetParametersByPath",
          "kms:Decrypt",
          "kms:GenerateDataKey"
        ]
        Resource = [
          "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.project_name}-${var.environment}/*",
          "arn:aws:ssm:${var.aws_region}:${data.aws_caller_identity.current.account_id}:parameter/${var.project_name}/${var.environment}/*",
          "arn:aws:kms:${var.aws_region}:${data.aws_caller_identity.current.account_id}:key/*"
        ]
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "eks_node_group_custom" {
  policy_arn = aws_iam_policy.eks_node_group_custom.arn
  role       = aws_iam_role.eks_node_group.name
}

# Data source for current account
data "aws_caller_identity" "current" {}

# Security Group for EKS Cluster
# Cross-SG rules live in aws_security_group_rule resources below so the cluster
# and node group security groups can reference each other (and themselves)
# without creating a dependency cycle between the two resources.
resource "aws_security_group" "eks_cluster" {
  name        = "${var.project_name}-${var.environment}-eks-cluster"
  description = "Security group for EKS cluster control plane"
  vpc_id      = var.vpc_id

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-eks-cluster"
  }
}

# Security Group for EKS Node Groups
resource "aws_security_group" "eks_node_group" {
  name        = "${var.project_name}-${var.environment}-eks-node-group"
  description = "Security group for EKS node groups"
  vpc_id      = var.vpc_id

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-eks-node-group"
  }
}

# Allow inbound from node group security group (control plane to nodes)
resource "aws_security_group_rule" "cluster_ingress_from_nodes" {
  type                     = "ingress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_cluster.id
  source_security_group_id = aws_security_group.eks_node_group.id
}

# Allow outbound from control plane to node group
resource "aws_security_group_rule" "cluster_egress_to_nodes" {
  type                     = "egress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.eks_cluster.id
  source_security_group_id = aws_security_group.eks_node_group.id
}

# Allow all traffic within the node group
resource "aws_security_group_rule" "node_group_ingress_self" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 0
  protocol                 = "-1"
  security_group_id        = aws_security_group.eks_node_group.id
  source_security_group_id = aws_security_group.eks_node_group.id
}

# Allow inbound from cluster control plane (kubelet/API to node)
resource "aws_security_group_rule" "node_group_ingress_from_cluster" {
  type                     = "ingress"
  from_port                = 1025
  to_port                  = 65535
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_node_group.id
  source_security_group_id = aws_security_group.eks_cluster.id
}

# Allow outbound to cluster control plane
resource "aws_security_group_rule" "node_group_egress_to_cluster" {
  type                     = "egress"
  from_port                = 443
  to_port                  = 443
  protocol                 = "tcp"
  security_group_id        = aws_security_group.eks_node_group.id
  source_security_group_id = aws_security_group.eks_cluster.id
}

# Allow all outbound (image pulls, AWS APIs via VPC endpoints)
resource "aws_security_group_rule" "node_group_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  security_group_id = aws_security_group.eks_node_group.id
  cidr_blocks       = ["0.0.0.0/0"]
}

# KMS Key for EKS Secrets Encryption
resource "aws_kms_key" "eks_secrets" {
  description             = "KMS key for EKS secrets encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Allow administration of the key"
        Effect = "Allow"
        Principal = {
          AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root"
        }
        Action   = "kms:*"
        Resource = "*"
      },
      {
        Sid    = "Allow EKS to use the key"
        Effect = "Allow"
        Principal = {
          Service = "eks.amazonaws.com"
        }
        Action = [
          "kms:Encrypt",
          "kms:Decrypt",
          "kms:ReEncrypt*",
          "kms:GenerateDataKey*",
          "kms:DescribeKey"
        ]
        Resource = "*"
      }
    ]
  })

  tags = {
    Environment = var.environment
    Name        = "${var.project_name}-${var.environment}-eks-secrets"
  }
}

resource "aws_kms_alias" "eks_secrets" {
  name          = "alias/${var.project_name}-${var.environment}-eks-secrets"
  target_key_id = aws_kms_key.eks_secrets.key_id
}