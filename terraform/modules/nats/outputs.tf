# NATS Module Outputs
output "nats_service_name" {
  description = "NATS Service Name"
  value       = "nats.nats.svc.cluster.local"
}

output "nats_client_port" {
  description = "NATS Client Port"
  value       = 4222
}

output "nats_monitor_port" {
  description = "NATS Monitor Port"
  value       = 8222
}

output "nats_cluster_port" {
  description = "NATS Cluster Port"
  value       = 6222
}

output "nats_jetstream_enabled" {
  description = "Whether JetStream is enabled"
  value       = var.nats_jetstream_enabled
}

output "security_group_id" {
  description = "NATS Security Group ID"
  value       = aws_security_group.nats.id
}

output "namespace" {
  description = "NATS Kubernetes Namespace"
  value       = kubernetes_namespace.nats.metadata[0].name
}