# Security policy

## Supported versions

This repository is an active development snapshot. No tagged release support matrix is currently declared. Security fixes should target the current main branch; deployers are responsible for rebuilding and redeploying after a fix is merged.

## Reporting a vulnerability

Please do not publish exploit details, credentials, customer data, or proof-of-concept payloads in a public issue.

Use the repository Security tab to submit a private vulnerability report when private vulnerability reporting is available. If that option is unavailable, contact the repository maintainers privately before publishing details.

Include the affected commit or version, impact, prerequisites, a minimal reproduction, and any practical mitigation. Do not include real secrets or sensitive production payloads.

## Current security limitations

The HTTP API does not yet implement authentication or authorization. The tenant middleware currently accepts caller-controlled X-Tenant-ID headers and tenant_id query parameters, with a default tenant fallback. Those values are not authenticated identity. Treat the API as development-only and keep it on loopback/private networks until a trusted identity resolver, per-resource tenant authorization, and cross-tenant denial tests are implemented.

Local Compose credentials are examples only and must not be reused in production. Review outbound destination validation, TLS configuration, secret handling, and idempotency behavior before deployment.
