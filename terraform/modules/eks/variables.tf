# EKS Module Variables
variable "project_name" {
  description = "Project name used for naming resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}

variable "eks_version" {
  description = "EKS Kubernetes version"
  type        = string
  default     = "1.28"
}

variable "eks_cluster_endpoint_public_access" {
  description = "Whether the EKS cluster endpoint is publicly accessible"
  type        = bool
  default     = true
}

variable "eks_cluster_endpoint_private_access" {
  description = "Whether the EKS cluster endpoint has private access"
  type        = bool
  default     = true
}

variable "eks_cluster_endpoint_public_access_cidrs" {
  description = "CIDR blocks allowed to access the public cluster endpoint"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "eks_node_group_instance_types" {
  description = "Instance types for EKS managed node groups"
  type        = list(string)
  default     = ["m6i.xlarge", "m6i.2xlarge"]
}

variable "eks_node_group_min_size" {
  description = "Minimum size of the node group"
  type        = number
  default     = 3
}

variable "eks_node_group_max_size" {
  description = "Maximum size of the node group"
  type        = number
  default     = 10
}

variable "eks_node_group_desired_size" {
  description = "Desired size of the node group"
  type        = number
  default     = 3
}

variable "eks_node_group_volume_size" {
  description = "Volume size (in GiB) for worker nodes"
  type        = number
  default     = 50
}

variable "eks_node_group_volume_type" {
  description = "Volume type for worker nodes"
  type        = string
  default     = "gp3"
}

variable "worker_min_size" {
  description = "Minimum size of the worker node group"
  type        = number
  default     = 2
}

variable "worker_max_size" {
  description = "Maximum size of the worker node group"
  type        = number
  default     = 8
}

variable "worker_desired_size" {
  description = "Desired size of the worker node group"
  type        = number
  default     = 3
}

variable "enable_spot_instances" {
  description = "Enable spot instances for worker nodes"
  type        = bool
  default     = false
}