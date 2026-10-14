# Operations and Troubleshooting

This guide describes the current implementation. It does not certify the service as production-ready.

## Security and exposure warning

JWT authentication and scope checks are available through `internal/auth`; enable them with `REQUIRE_AUTH=true`, a high-entropy `JWT_SECRET` (at least 32 characters), and the expected issuer/audience. In production, tenant extraction also supports a fail-closed trusted-proxy contract via `FLOWRULZ_TRUSTED_PROXY_TOKEN`, `X-Proxy-Auth`, and `X-Authenticated-Tenant-ID`. A validated JWT tenant claim is authoritative; conflicting caller-supplied tenant values are rejected. Do not expose the API to untrusted multi-tenant traffic until authentication is required in the deployed configuration, every resource path has been audited, and cross-tenant negative tests plus proxy/header-stripping behavior are verified.

Native configuration defaults the HTTP server to `127.0.0.1:8080`. The local Compose file publishes API, PostgreSQL, NATS, NATS monitoring and Redis ports on host loopback only. Compose starts three worker replicas by default (`WORKER_REPLICAS=3`) using the fixed-replica shard lease allocation. It still uses demo credentials. Do not reuse those credentials or treat the development stack as a production deployment.

## Runtime topology

| Process | Entry point | Responsibility |
|---|---|---|
| HTTP API | `cmd/api/main.go` | Rule revisions/activations, workflow instances/definitions, contracts, batches, quarantine and scheduled-event endpoints |
| Worker | `cmd/worker/main.go` | NATS JetStream consumption, shard lease/fencing, bounded per-key work, event evaluation, outbox publisher and scheduler |
| Code generator | `cmd/codegen/main.go` | Generate Go, Java, Protobuf or JSON Schema artifacts from a contract already registered in storage |
| Migration CLI | `cmd/migrate/main.go` | Apply pending SQL migrations; only `up` is implemented |

The API starts HTTP only. There is no gRPC server or active gRPC listener. The client SDK includes a gRPC transport interface, but that is not a server implementation in this repository. `/health` is a liveness endpoint only; it does not prove storage or NATS readiness.

## Worker metrics for the React console

Worker processes can expose their process-local Prometheus registry on a private HTTP listener by setting:

```sh
FLOWRULZ_METRICS_ADDR=127.0.0.1:9090
FLOWRULZ_CONSOLE_ORIGIN=http://127.0.0.1:5173
```

`FLOWRULZ_METRICS_ADDR` is opt-in. Set a unique reachable address per worker host/container and keep it on a private network. Set `FLOWRULZ_CONSOLE_ORIGIN` to the exact browser origin only when the console must scrape metrics cross-origin; leave it unset when a same-origin reverse proxy is used. The metrics server exposes only `GET /metrics`, allows preflight only for the configured origin, and does not enable wildcard CORS. The metrics listener does not authenticate requests; do not expose it to untrusted networks. Put it behind an authenticated reverse proxy if browser access crosses a trust boundary. Do not expose raw metrics to the public internet. Prometheus counters and gauges are process-local; scrape each worker separately for per-worker views. This endpoint does not provide log tailing; use the approved centralized log backend for logs.

## Local development

Start the development stack from the repository root:

~~~sh
docker compose up --build -d
~~~

Published host ports are loopback-bound: console `localhost:3000`, API `localhost:8080`, NATS client `localhost:4222`, NATS monitoring `localhost:8222`, PostgreSQL `localhost:5432`, and Redis `localhost:6379`. The console reverse-proxies API requests, worker `/metrics`, and Loki queries over the private Compose network. See [the Compose deployment guide](deployment-compose.md) for details.

Stop it with:

~~~sh
docker compose down
~~~

To retain local database volumes, use `docker compose down` without `-v`. Only remove volumes when you intentionally want to destroy development data.

API/worker startup initializes the configured storage backend and applies migrations. The Go worker publishes command and tenant-service effects through NATS JetStream. There is no `EFFECT_DESTINATION_URL` setting and HTTP callback delivery is unsupported. Receivers must use durable consumers for their exact `effects.*`, `commands.*`, or assigned `services.*` subjects and deduplicate stable effect IDs.

## Configuration

The authoritative Compose variable template is `.env.example`; copy it to `.env` before starting the stack. Compose injects this file into API, worker, and codegen containers, and the Compose file only overrides container-specific addresses/paths. Values such as `DATABASE_URL`, `NATS_URL`, and `REDIS_ADDR` must use Docker DNS service names (`postgres`, `nats`, `redis`) inside containers. Parsing and validation live in `internal/config/config.go`. Never commit `.env`.

| Setting | Default / behavior |
|---|---|
| `HTTP_ADDR` | `127.0.0.1:8080` for direct local execution; Compose overrides this inside the container to listen on `:8080` |
| `DATABASE_URL` | Local PostgreSQL DSN; `STORAGE_BACKEND=postgres` is the default |
| `STORAGE_BACKEND` | `postgres` or `mongodb` |
| `MONGODB_URI`, `MONGODB_DATABASE` | Required when `STORAGE_BACKEND=mongodb`; transactions require replica-set/sharded MongoDB |
| `NATS_URL` | NATS JetStream connection URL; use Docker DNS service name `nats` inside Compose |
| `NATS_STREAM`, `NATS_CONSUMER` | Durable stream for events/effects/commands/service deliveries and optional consumer identifier |
| `NUM_SHARDS` | 4096 virtual shards by default; must match producer/SDK/scheduler settings |
| `WORKER_REPLICAS` | `3` in Compose; must match the configured worker replica count on every worker. This is fixed membership, not automatic rebalance after replica loss. |
| `BATCH_MODE` | Must remain `none` until batch execution uses a valid shard fencing token and the per-key queue; other values fail startup validation |
| `FETCH_BATCH_SIZE`, `MAX_GLOBAL_IN_FLIGHT`, `MAX_PER_KEY_QUEUE`, `KEY_QUEUE_WORKERS` | Bound fetching and queued/in-flight work |
| `NATS_ACK_WAIT_MS`, `NATS_MAX_DELIVER` | NATS delivery acknowledgement timeout and maximum delivery count |
| `EFFECT_PUBLISH_INTERVAL_MS`, `EFFECT_PUBLISH_BATCH_SIZE`, `EVENT_RETRY_DELAY_MS` | Outbox publishing cadence/batch and event retry delay |
| `SCHEDULER_POLL_MS`, `SCHEDULER_BATCH_SIZE` | Scheduled-event polling cadence and batch size |
| `LEASE_TTL_MS`, `LEASE_RENEWAL_INTERVAL_MS` | Shard lease duration and renewal cadence; renewal must remain within configured validation limits |
| `SHUTDOWN_TIMEOUT` | Bounded graceful-shutdown interval |

Some optional observability and generation settings are also accepted by config. Verify the concrete entry point uses a setting before assuming it starts a listener or service; unused configuration should not be treated as an implemented feature.

## Rule deployment and event publishing

Deploy a single JSON or YAML rule document:

~~~sh
curl --fail-with-body -sS \
  -H 'Content-Type: application/yaml' \
  -H 'X-Tenant-ID: development-tenant' \
  --data-binary @domains/banking/rules/transaction-initiated.yaml \
  http://localhost:8080/v1/rules/transaction.initiated/revisions
~~~

Use a file whose `rule_set` matches the path parameter. HTTP request decoding is bounded to 1 MiB and rejects multiple JSON values or multiple YAML documents. The sample tenant header is for local testing only, not authentication.

For event traffic, use the configured subject family `events.shard.N`. The shard is derived from the tenant/partition identity using the same `NUM_SHARDS` value as the worker. The business event type is carried in the envelope; it is **not** a replacement for the stream subject. Use the SDK publisher or a message fixture matching `api/envelope-schema.json` and the runtime's shard calculation.

The event flow is:

1. A producer publishes an `EventEnvelope` to `events.shard.N`.
2. The worker consumes a shard for which it currently holds a lease and queues work with per-key serialization.
3. The event service validates the fencing token, deduplicates through the inbox, loads the active immutable rule revision, evaluates it, then commits execution and outbox effects atomically.
4. The worker acknowledges only after a successful durable transaction.
5. The outbox publisher routes emitted child events back to `events.shard.N`; generic effects, commands, and tenant-service deliveries publish through NATS JetStream.
6. The scheduler claims due scheduled events and publishes them to the same subject family.

Delivery is at-least-once. Inbox/outbox semantics support deduplication and crash recovery, but external side effects are effectively-once only if the receiver honors the supplied idempotency key. Do not claim exactly-once delivery.

## Database and migration operations

Apply pending migrations:

~~~sh
make migrate-up
~~~

The migration CLI currently supports only `up`. There are no supported `down` or `status` commands. Do not edit an applied migration; add a new numbered migration and verify both upgrade behavior and the resulting schema. Migration files currently have a numbering gap, which should not be backfilled by renumbering applied files.

PostgreSQL is the default transactional backend. MongoDB is optional, but must be deployed as a replica set or sharded cluster for transactions. Do not assume full adapter equivalence from the current in-memory contract tests.

## Verification commands

From the repository root:

~~~sh
go build ./cmd/...
go test -race -count=1 ./...
go vet ./...
make check-fmt
~~~

Nested module boundaries mean each SDK and example module must be checked separately:

~~~sh
(cd sdk/go && go test -race -count=1 ./...)
(cd sdk/go/flowrulz && go test -race -count=1 ./...)
(cd examples/banking && go test -race -count=1 ./...)
(cd examples/ecommerce && go test -race -count=1 ./...)
(cd examples/food-delivery && go test -race -count=1 ./...)
~~~

The existing `tests/contract` suite uses in-memory repositories. `tests/integration` currently uses mocks/in-memory dependencies; it does not prove crash recovery against live PostgreSQL and NATS. Production behavior still needs live tests for transaction rollback, redelivery/ACK loss, competing/expired outbox claims, lease loss while paused, broker restart, external destination timeouts and destination idempotency.

## Failure handling and known risks

### Crash before the event transaction commits

Database writes in the transaction should roll back and the message can be redelivered. Confirm the durable state and broker behavior in a live PostgreSQL/NATS failure test; mock tests alone do not demonstrate it.

### Commit succeeds, ACK is lost

The message may be redelivered. The inbox and execution lookup are intended to avoid re-executing a committed event. Maintain the unique constraints and transaction ordering required for this invariant.

### External destination is unavailable

Effects are retried/backed off and may be quarantined after the configured attempt limit. Outbox delivery/retry/quarantine settlement now checks a unique claim token and unexpired claim in SQL and Mongo, and durable settlement errors are returned by the publisher. Quarantine records are persisted separately from outbox settlement, so a failure during cleanup can still leave an orphan record; monitor quarantine/outbox state and validate failure recovery against a live database.

### Long-running destination call / expired outbox claim

The current publisher sends effects sequentially and uses a fixed two-minute claim TTL. A slow destination may outlive the claim; stale settlement is rejected, but the remote side effect may already have occurred. Keep receiver-side idempotency enabled and add per-effect timeouts before adding concurrency.

### Scheduled-event claims and recovery

The SQL adapter uses columns introduced by migration `015_scheduled_event_claims.sql`. SQL and Mongo can reclaim expired claims and require the claimant token plus a non-expired claim when marking an event released or failed. A stale claimant cannot settle an event after its lease expires. However, the scheduler currently marks publish errors terminally failed instead of retrying transient broker failures; treat that as an open reliability limitation.

The scheduled-event list and get endpoints are tenant-filtered. Cancellation only changes events that are still `pending`; an event that is claimed or already released returns a conflict. A validated JWT claim or trusted proxy identity establishes the tenant; caller-supplied tenant headers are not identity and cannot override a JWT claim.

### Batch processing

The API can list/create/query batch records using tenant-scoped repository reads, but the worker's batch execution path is not integrated with owned-shard fencing and per-key queue ordering. The service currently cannot safely pass the fencing token required by the event processor. Configuration validation therefore rejects non-`none` `BATCH_MODE` values. Do not bypass this guard to enable batch execution.

### Mongo payload representation

Raw JSON event data, inbox/outbox payloads, scheduled payloads, workflow context and rule source/compiled rules are stored as BSON binary bytes, not as `bson.Raw`. BSON round-trip tests cover those documents. MongoDB transactions still require an appropriate replica-set or sharded deployment, and live adapter conformance testing remains outstanding.

### Shard lease lost or worker pauses

The event transaction validates the fencing token before writes, and the queue serializes work by key within a worker. Verify lease expiry, fencing and redelivery under real process pauses and network partitions before increasing worker replicas.

### API tenant isolation

This remains a release blocker until deployment authentication is mandatory and correctly configured. JWT validation and trusted-proxy tenant resolution are implemented, but configuration alone does not prove production wiring or complete resource-level authorization. Verify every read/write path and cross-tenant denial tests before exposing the service.

## Operational readiness

- `/health` is liveness only; implement a separate dependency-aware readiness probe before relying on orchestration to route traffic.
- No gRPC server is implemented.
- Observability packages exist, but instrumentation/metric coverage should be checked against operational objectives rather than assumed complete.
- Retention policies, index/query-plan reviews, throughput/latency benchmarks and live crash/failure-injection coverage remain work items.
- LeaseManager currently targets `NUM_SHARDS/3` per worker without a worker-membership/rebalancing protocol. This assumes a three-worker topology; changing replica count can leave shards unowned. Validate shard ownership coverage before scaling or changing replicas.
- Keep the HTTP API and local backing services on private interfaces until required JWT/proxy authentication, header stripping, and tenant authorization are verified in the deployment.
