# FlowRulz

FlowRulz is a Go-based, event-driven rules runtime. NATS JetStream retains the durable message log; runtime caches accelerate reads, while PostgreSQL stores transactional processing state and the outbox. Rules are compiled into immutable revisions; a worker evaluates incoming events and commits an execution record plus resulting effects; effects are delivered asynchronously through an outbox.

> Security: do not expose the HTTP API directly to untrusted networks. Production tenant identity must come from validated JWT claims or an authenticated trusted proxy that strips and replaces trusted headers. Development-only caller-supplied tenant headers are not authentication.

## Processes

| Process | Entrypoint | Responsibility |
|---|---|---|
| HTTP API (control plane) | cmd/api | Rule revisions/activation, admin-scoped workflow definitions, contracts, batch/quarantine read views, tenant domains/services, read-only execution and trace views |
| Worker | cmd/worker | NATS JetStream consumption, shard leases/fencing, per-process key serialization, event evaluation, outbox publishing |
| Code generator | cmd/codegen | Generate Go, Java, Protobuf, and JSON Schema artifacts from a contract already registered in storage |
| Migration CLI | cmd/migrate | Apply pending migrations; only the up command is implemented |

The root module uses Go 1.26.0. Both sdk/go and the newer sdk/go/flowrulz client are separate Go modules; each example application also has its own module. CI tests every module separately because nested go.mod files are not included in a parent-module ./... command.

## Control plane / data plane boundary

The HTTP API is a control-plane service. It creates and validates configuration (domains, contracts, rule revisions, workflow definitions, and registered provider/service capabilities) and exposes management/read-only operational views. It does not accept business events and does not connect to NATS to publish them. Producers publish `EventEnvelope` messages directly to NATS JetStream using the configured subject and shard contract. Only `cmd/worker` consumes business events and evaluates rules. The Go SDK's HTTP event transport is rejected; configure `FLOWRULZ_TRANSPORT=nats` for event publication. A `schedule_event` rule action is persisted through the transactional outbox and then the worker-owned scheduler. Correlated workflow `wait`/resume semantics are not yet supported.

## Event lifecycle

1. A producer publishes an EventEnvelope to the NATS JetStream subject family events.shard.N. N is computed from tenant ID and partition key using the configured shard count. File-backed, limits-retention streams keep acknowledged messages available for replay until configured retention or storage limits remove them. Configure `NATS_STREAM_MAX_AGE`, `NATS_STREAM_MAX_BYTES`, and `NATS_STREAM_MAX_MSGS`; zero age and `-1` limits mean unlimited, which requires capacity monitoring.
2. The worker consumes only shards for which it owns a lease. The key queue serializes work for each partition key within this worker process and can allow concurrency across keys. This alone does not guarantee end-to-end per-key ordering across redelivery, lease handoff, or concurrent outbox publishers.
3. Inside one storage transaction, the event service validates the fencing token, checks the inbox for duplicates, loads the active rule revision, evaluates rules, and writes the execution plus outbox effects.
4. The worker acknowledges the broker delivery only after the transaction commits.
5. The outbox publisher routes emitted child events back to events.shard.N. Command effects publish to `commands.<destination>.<name>`; domain subscribers receive matching event deliveries on a unique tenant-scoped `services.<tenant-hash>.<service-hash>` subject. Both are durable JetStream messages and at-least-once.
6. The scheduler claims due scheduled events and publishes them to the same subject family.

NATS delivery is at-least-once. JetStream is the durable append-only message log; PostgreSQL is the durable transactional state store for inbox deduplication, executions, and outbox delivery state. Runtime caches are disposable accelerators and must be rebuildable from durable stores. The inbox prevents duplicate event execution when storage constraints and transaction semantics hold. External effects are effectively-once only when the destination honors the idempotency key. Do not claim exactly-once delivery.

## Tenant domains and receiving services

Tenant-owned event domains and receiver registrations are managed through the `/v1/domains` and `/v1/services` management APIs. The management API remains HTTP; application messages between producers, workers, and receivers use NATS JetStream. A domain groups event type names; a service subscribes to one or more domains. After a matching event commits a successful active-rule execution, the worker adds one service delivery per active subscriber to the same transaction's outbox. Events that have no active rule do not trigger service deliveries.

FlowRulz assigns each service a stable, tenant-isolated `nats_subject` such as `services.<tenant-hash>.<service-hash>`. Receivers create a durable JetStream consumer for their exact subject on the configured `NATS_STREAM`; subjects are unique per service so one receiver cannot steal another receiver's fan-out copy. Command effects publish to `commands.<destination>.<name>`, where `destination` is a single lowercase subject token and `name` is a dot-separated set of lowercase tokens. Generic `emit` effects publish to `effects.<topic>`.

Each delivery is a JSON effect envelope containing the stable outbox `id`, `idempotency_key`, `tenant_id`, `execution_id`, `destination`, `name`, `effect_type`, and original JSON `payload`. Receivers must deduplicate by `idempotency_key` (or stable effect ID), commit their own state before ACKing, and assume at-least-once delivery. Transient publish errors leave the outbox eligible for retry; a successful broker publish does not prove that the receiver completed its business operation. HTTP callback URLs are no longer supported. Hashed subjects provide routing separation, not authorization: configure NATS credentials and subject permissions so each receiver can consume only its assigned subject and publish only approved response subjects. Never share an unrestricted broker credential with tenant-owned receivers.

## Run locally

Start the local development stack from the repository root:

~~~sh
cp .env.example .env
# Edit .env before using non-development credentials.
docker compose config
docker compose up --build -d
~~~

Use the default Go worker. The optional Rust worker profile is experimental. Its effect routes use NATS JetStream, but it is not yet proven equivalent to Go; keep Go selected until differential and infrastructure-backed recovery tests pass.

Open the React operations console at http://localhost:3000. Compose starts three worker replicas by default (`WORKER_REPLICAS=3`); check them with `docker compose ps worker`. It proxies the API, worker metrics, and Loki through Docker DNS so browser clients do not need to resolve container names. API is also available at localhost:8080, NATS at localhost:4222, NATS monitoring at localhost:8222, PostgreSQL at localhost:5432, and Redis at localhost:6379. Published ports bind to 127.0.0.1 only. Promtail discovers local Docker container logs and ships them to Loki for the console Logs page. The stack uses development settings and demo credentials; do not reuse those credentials in production. See [docs/deployment-compose.md](docs/deployment-compose.md) for service discovery, environment variables, log collection, and troubleshooting.

For root-module development:

~~~sh
go build ./cmd/...
go test -race -count=1 ./...
go vet ./...
~~~

Test the nested modules separately:

~~~sh
(cd sdk/go && go test -race -count=1 ./...)
(cd sdk/go/flowrulz && go test -race -count=1 ./...)
(cd examples/banking && go test -race -count=1 ./...)
(cd examples/ecommerce && go test -race -count=1 ./...)
(cd examples/food-delivery && go test -race -count=1 ./...)
~~~

## Generated Go clients

Generating a Go target emits both the contract type and a typed client alongside it. The generated client defaults to the registered contract name as its rule set; `SendToRuleSet` and `SubscribeRuleSet` allow an explicit rule-set name. Send options such as `flowrulz.WithPartitionKey`, `WithWorkflowID`, `WithCorrelationID`, and `WithHeaders` are passed through to the SDK. Consumer modules need a dependency on `github.com/flowrulz/flow-rulz-sdk/go/flowrulz`.

~~~go
client, err := flowrulz.New()
if err != nil {
    return err
}
defer client.Close()

typed := transactioninitiated.NewClient(client)
err = typed.Send(ctx, &transactioninitiated.Initiated{
    TransactionId: "txn-123",
    AccountId:     "account-456",
}, flowrulz.WithPartitionKey("account-456"))
~~~

Generated `Subscribe` methods use live core NATS subscriptions and are available only with the NATS transport. They receive matching events for the configured tenant without competing with the worker's JetStream WorkQueue consumer. They are best-effort rather than durable: events published while disconnected are not replayed, and handler failures stop the subscription without ACK/retry behavior.

Environment variable names and local defaults are documented in .env.example. API and worker startup initialize storage/migrations. The Go worker publishes all command and service effects through NATS JetStream; no HTTP destination URL is required.

## Repository map

- cmd contains the process entrypoints.
- internal/domain contains business types, validation, state transitions, IDs, and hashes.
- internal/ports defines broker, storage, transaction, and service interfaces.
- internal/rules contains rule compilation and deterministic evaluation.
- internal/services contains event, effects, tenant domain/service, rules, batch, workflow, and quarantine use cases.
- internal/application and internal/runtime contain worker orchestration, the per-key queue, shard leases, and the scheduler.
- internal/adapters contains HTTP management handlers, PostgreSQL, MongoDB, NATS, Redis, fake test destinations, and NATS effect destinations.
- internal/storage selects and wires the configured persistence backend.
- sdk/go/runtime is the original standalone producer/consumer SDK module.
- sdk/go/flowrulz is a separate client SDK module. NATS is the supported event transport; HTTP ingestion, gRPC transport and local evaluation are not available in the current runtime.
- examples contains banking, e-commerce, and food-delivery contracts, rules, workflows, and sample applications. Typed domain code lives in `domains/<domain>/builds/go` (separate Go modules) and is produced by `make generate-domain DOMAIN=<domain>` from the contracts stored in the database; example services import it from there and the SDK from `sdk/go/flowrulz`.
- migrations contains numbered PostgreSQL migrations.
- docs contains architecture diagrams, rules, operations, roadmap, and dated audit notes.
- AGENTS.md is the engineering guide and source-backed file map.
- [Architecture map](docs/architecture/map.md) describes runtime data flow, package boundaries, and known gaps.

Start with docs/README.md for the documentation index.

## Important limitations

- Production tenant resolution requires validated JWT identity or a trusted proxy token and authenticated-tenant header. The proxy boundary is not first-party authentication; do not expose the API directly to untrusted networks.
- /health is a liveness endpoint only. Use /ready for database and NATS reachability; neither endpoint proves workers are processing.
- The API starts HTTP only. No gRPC server is implemented; the SDK's gRPC transport requires a compatible server deployed separately.
- Migration rollback/status commands and live infrastructure-backed chaos tests are not implemented.
- Tests under tests/integration use mocks/in-memory repositories, not live PostgreSQL and NATS. Validate storage atomicity, redelivery, lease expiry, and crash recovery against real infrastructure before making production guarantees.
- MongoDB transaction behavior and parity must be verified for the exact deployment configuration before relying on it for the same guarantees as PostgreSQL. JSON-backed fields are stored as BSON binary bytes and covered by BSON round-trip tests; live adapter conformance tests remain outstanding.
- Scheduled-event SQL claim columns are introduced in migration 015. Expired claims can be reclaimed; list/get/cancel are tenant-filtered and cancel only pending events. Publish errors still mark scheduled events failed without retry/backoff.
- Batch reads are tenant-scoped, but batch event execution has not been integrated with owned-shard fencing and the per-key queue. Non-none BATCH_MODE values are rejected until this is fixed; do not bypass the validation guard.
- The shard lease manager currently targets NUM_SHARDS/3 per worker without membership/rebalancing, so the current implementation implicitly assumes three worker replicas. Validate all shards remain assigned before changing replica count.

## Random producer simulation

An opt-in random producer can publish 1–100 messages per simulated client through NATS JetStream using Compose service discovery. See [random producer README](examples/random-producer/README.md) for run commands, ACK semantics, shard routing, and the current limits around master election and dynamic worker membership.
