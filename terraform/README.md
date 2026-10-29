# FlowRulz Terraform

Root Terraform module that deploys FlowRulz on AWS: VPC, EKS, RDS
(PostgreSQL), ElastiCache (Redis), NATS with JetStream, the application
workloads (API, worker, console) and the observability stack.

> **Status:** the configuration passes `terraform fmt -check` and
> `terraform validate`, but no `terraform plan` or `terraform apply` has been
> executed from this repository. Treat every resource as created-for-the-first
> time; see "Known gaps" before relying on it.

## Layout

| Path | Responsibility |
|---|---|
| `versions.tf` | Terraform/provider version constraints and the (partial) S3 backend |
| `providers.tf` | AWS, Kubernetes, Helm provider configuration (Kubernetes/Helm auth is derived from the EKS cluster) |
| `variables.tf` | Root input surface, with per-variable notes on anything not wired |
| `main.tf` | Module composition, generated secrets and the application IRSA role |
| `outputs.tf` | Deployment outputs (never secrets) plus an optional `deployment_summary` |
| `modules/vpc` | VPC, subnets, NAT, flow logs, VPC endpoints |
| `modules/eks` | EKS cluster, managed node groups, cluster/node security groups, secrets KMS key |
| `modules/rds` | RDS PostgreSQL (Multi-AZ optional, in-region read replica for production) |
| `modules/elasticache` | ElastiCache Redis replication group with CloudWatch log delivery |
| `modules/nats` | NATS with JetStream (Helm) |
| `modules/app` | Namespace, ConfigMap/Secret, API/worker/console Deployments, Services, HPAs, PDBs, NetworkPolicies |
| `modules/observability` | kube-prometheus-stack, Grafana dashboards, Loki, ServiceMonitors, alerts |
| `environments/*/terraform.tfvars` | Per-environment values (no secrets) |

## Prerequisites

- Terraform >= 1.6
- AWS credentials with permissions for VPC/EKS/RDS/ElastiCache/IAM/CloudWatch
- AWS CLI on the host that runs Terraform (the Kubernetes/Helm providers use
  `aws eks update-kubeconfig` via an `exec` block)
- An S3 bucket and DynamoDB table for state (the backend does not create them):

```sh
aws s3api create-bucket --bucket flow-rulz-terraform-state --region us-east-1
aws s3api put-bucket-versioning \
  --bucket flow-rulz-terraform-state \
  --versioning-configuration Status=Enabled
aws dynamodb create-table \
  --table-name terraform-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

## Workflow

The S3 backend is only partially configured: the state `key` is supplied per
environment so staging never shares production state.

```sh
cd terraform

# Production
terraform init -reconfigure -backend-config="key=production/terraform.tfstate"
terraform plan  -var-file=environments/production/terraform.tfvars
terraform apply -var-file=environments/production/terraform.tfvars

# Staging
terraform init -reconfigure -backend-config="key=staging/terraform.tfstate"
terraform plan  -var-file=environments/staging/terraform.tfvars
terraform apply -var-file=environments/staging/terraform.tfvars
```

Static verification (no AWS account or state needed):

```sh
terraform fmt -check -recursive
terraform init -backend=false
terraform validate
```

## Secrets

Sensitive values are never committed. Pass them as `TF_VAR_*` environment
variables:

| Variable | Behavior when unset |
|---|---|
| `TF_VAR_jwt_secret` | A 48-character secret is generated and stored in state |
| `TF_VAR_grafana_admin_password` | A 32-character secret is generated and stored in state |
| `TF_VAR_trusted_proxy_token` | Empty (trusted-proxy auth stays disabled) |
| RDS password / Redis auth token | Generated; the Redis token only exists when transit encryption is enabled |

Generated secrets live in Terraform state, which the S3 backend keeps
encrypted; restrict access to the state bucket accordingly.

## Known gaps

Variables declared in `variables.tf` but not consumed by any module (marked
with comments there):

- `alb_certificate_arn`, `alb_domain_name`, `alb_subdomain`,
  `alb_hosted_zone_id` - no module installs an ingress controller or ALB, so
  the workloads have no external ingress yet (the NetworkPolicies reference an
  `ingress-nginx` namespace that this configuration does not create).
- `enable_pod_security_policies` - PodSecurityPolicy was removed in Kubernetes
  1.25+.
- `enable_cross_region_replication`, `dr_region` - the RDS module only creates
  an in-region read replica for production.
- `spot_instance_types` - `enable_spot_instances` is honored, but no module
  accepts a separate spot instance type list.

Other gaps:

- **Redis TLS:** ElastiCache transit encryption defaults to off because the
  FlowRulz Go client (`internal/adapters/redis`) has no TLS support.
  `elasticache_transit_encryption_enabled = true` would break application
  startup until the client gains TLS support.
- **IRSA:** the application IAM role trusts only the FlowRulz service account
  but has no attached permissions (the runtime calls no AWS APIs).
- **Grafana domain:** `grafana_domain` stays at the module default
  `grafana.example.com`; there is no root-level knob for it yet.
- **ElastiCache cluster mode:** the replication group uses non-cluster mode
  (`num_cache_clusters`) because the application dials the primary endpoint
  with a standalone Redis client.
