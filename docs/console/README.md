# FlowRulz React Operations Console

A React + TypeScript console for the existing FlowRulz management API.

## Run locally

Requires Node.js 20+.

```sh
cd web/console
npm install
npm run dev
```

Open the URL printed by Vite (normally http://127.0.0.1:5173). The console defaults to the same-origin `/api` proxy to avoid browser CORS failures; Vite forwards it to the API at http://127.0.0.1:8080. For another deployment, configure the API base URL to a same-origin reverse-proxy path or to an API origin with explicit CORS.

Production build:

```sh
npm ci
npm run build
npm run preview
```

## Features

- Overview with API liveness, active rules, worker telemetry, and cumulative execution counters.
- API explorer for the routes currently registered in `internal/adapters/http/router.go`.
- Contract registration and server-side generation targets: Go, Java, Protobuf, and JSON Schema.
- Rule deployment via compile-and-activate and tenant-scoped active-rule inspection.
- Worker process monitoring from separately configured Prometheus `/metrics` endpoints. Enter one endpoint per worker in Settings. Metrics are process-local and must not be exposed publicly.
- Logs from a separately configured Loki-compatible query API. The FlowRulz API does not currently expose a log-tail endpoint; this console does not fabricate log data.

## API and security notes

- Settings are kept in browser localStorage. Avoid shared browsers and use short-lived, least-privilege tokens.
- The UI adds `X-Tenant-ID` to API requests and uses an optional bearer token. The server remains responsible for authentication, authorization, and tenant isolation.
- Contract mutation and generation require the relevant admin scope outside local development.
- Code generation writes to a server-managed temporary directory. The current API returns generated paths, not a downloadable artifact.
- Prometheus worker endpoints are unauthenticated and must remain on private networks or behind an authenticated reverse proxy; the API bearer token is not sent to worker metrics. Configure exact-origin CORS/reverse-proxy rules. Do not use wildcard CORS or expose raw metrics publicly.
- Worker counts and per-rule execution numbers are sourced from each configured worker's metrics endpoint. If a metric is absent, the UI shows an unavailable value rather than guessing.

## Current limitations

- No server-side aggregation service is added by this UI. To monitor multiple worker replicas, configure each worker's private metrics endpoint or use an approved metrics gateway.
- The current runtime metrics do not identify a worker in a shared central registry; per-worker comparison requires scraping each worker independently.
- Loki is optional. Application logs are not fetched from the process filesystem or the API server.
- Generated files are created on the API server and are not directly downloadable through the current API.
