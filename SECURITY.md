# Security policy

## Supported versions

This repository is an active development snapshot. No tagged release support matrix is currently declared. Security fixes should target the current main branch; deployers are responsible for rebuilding and redeploying after a fix is merged.

## Reporting a vulnerability

Do not publish exploit details, credentials, customer data, or proof-of-concept payloads in a public issue. Use the repository Security tab for private vulnerability reporting when available, otherwise contact the maintainers privately. Include the affected commit/version, impact, prerequisites, a minimal reproduction, and mitigation. Never include real secrets.

## Current authentication behavior (source-reviewed; not security-certified)

The previous policy incorrectly stated that the HTTP API had no authentication or authorization. Current source implements optional/required JWT authentication in `internal/auth` and `internal/adapters/http/router.go`, plus a trusted-proxy tenant-identity path in `internal/adapters/http/middleware/middleware.go`.

- In `APP_ENV=development|dev|test`, tenant middleware permits caller-supplied `X-Tenant-ID` or `tenant_id` (and defaults to `default`). This is for local testing only.
- Outside those environments, runtime tenant identity comes from a validated JWT claim or from `X-Authenticated-Tenant-ID` after `X-Proxy-Auth` is checked against `FLOWRULZ_TRUSTED_PROXY_TOKEN`. The proxy credential comparison is constant-time. A missing proxy token fails closed for proxy-only requests.
- JWT validation checks signing method, signature, expiry and configured issuer/audience. The `REQUIRE_AUTH` setting determines whether management requests must carry JWT credentials; see `docs/operations.md` for configuration.
- Global configuration routes require appropriate authenticated scopes (for example `rules:admin`, `domains:admin`, `subscriptions:admin`, `contracts:admin` or platform admin). A proxy credential alone does **not** grant global configuration access.
- `GET /health` and `GET /ready` are public probes by design. Readiness may report unavailable when required authentication is not configured.

**Important:** Presence of these mechanisms does not establish complete multi-tenant authorization, secure producer identity, or production readiness. Route-level authorization, cross-tenant negative tests, trusted-proxy header stripping, NATS subject ACLs and event-envelope tenant authenticity require end-to-end verification. Never expose this API directly to untrusted networks or claim multi-tenant isolation based solely on middleware presence.

## Deployment requirements and remaining limitations

1. Authenticate and authorize at the trusted proxy; strip all inbound `X-Proxy-Auth`, `X-Authenticated-Tenant-ID`, and `X-Authenticated-Scopes` headers and replace them with proxy-controlled values. Keep API access private to that proxy.
2. Use high-entropy credentials and secrets; configure transport TLS and rotation. Local Compose credentials are examples, never production credentials.
3. Authorize event producers at NATS and bind broker identity to permitted tenant/partition metadata. A tenant ID inside an event is not itself proof of identity.
4. Test negative cross-tenant access for every handler and broker operation, including quarantine, replay, tracing and global configuration.
5. Validate outbound destinations, payload sizes, retention/deletion, audit logging and idempotency before external deployment.

Tracked work: [SEC-01, SEC-02 and SEC-03](docs/TODO.md). Current implementation details: [architecture map](docs/architecture/map.md), [operations guide](docs/operations.md).
