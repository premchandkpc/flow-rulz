# NATS Module
# Deploys NATS with JetStream on EKS using Helm

resource "aws_security_group" "nats" {
  name        = "${var.project_name}-${var.environment}-nats"
  description = "Security group for NATS"
  vpc_id      = var.vpc_id

  # Allow inbound from EKS node groups (NATS client port)
  ingress {
    from_port       = 4222
    to_port         = 4222
    protocol        = "tcp"
    security_groups = [var.eks_node_group_security_group_id]
  }

  # Allow inbound from EKS node groups (NATS monitoring port)
  ingress {
    from_port       = 8222
    to_port         = 8222
    protocol        = "tcp"
    security_groups = [var.eks_node_group_security_group_id]
  }

  # Allow inbound from EKS node groups (JetStream/Leafnode ports)
  ingress {
    from_port       = 7422
    to_port         = 7422
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
    Name        = "${var.project_name}-${var.environment}-nats"
  }
}

# Kubernetes namespace for NATS
resource "kubernetes_namespace" "nats" {
  metadata {
    name = "nats"
    labels = {
      name = "nats"
    }
  }
}

# NATS Helm Release
resource "helm_release" "nats" {
  name       = "nats"
  repository = "https://nats-io.github.io/k8s/helm/charts/"
  chart      = "nats"
  version    = var.nats_helm_chart_version
  namespace  = kubernetes_namespace.nats.metadata[0].name

  values = [
    templatefile("${path.module}/values.yaml.tpl", {
      nats_version           = var.nats_version
      jetstream_enabled      = var.nats_jetstream_enabled
      jetstream_mem_storage  = var.nats_jetstream_memory_storage_limit
      jetstream_disk_storage = var.nats_jetstream_disk_storage_limit
      cluster_size           = var.nats_cluster_size
      image_repository       = var.nats_image_repository
      image_tag              = var.nats_version
      security_group_id      = aws_security_group.nats.id
      vpc_id                 = var.vpc_id
      private_subnet_ids     = var.private_subnet_ids
    })
  ]
}