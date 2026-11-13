# Rust worker service (experimental)

## Broker mode with optional transports

Set `WORKER_MODE=broker` to run the broker and local rule evaluator in the Rust
process. It uses the same protocol v1, environment names, canonical EventEnvelope,
topic names, tenant scoping and consumer receipts as the Go broker.

```sh
cd rust-worker
WORKER_MODE=broker WORKER_RULES_DIR=../examples/direct-worker/rules \
  BROKER_TRANSPORTS=tcp NUM_SHARDS=16 cargo run --locked
```

`BROKER_TRANSPORTS` accepts any subset of `http,tcp,nats`, or `none` for in-process
use. `BROKER_OUTPUT_TRANSPORT` independently selects local topics or one HTTP,
TCP or NATS peer. Disabled adapters do not require their connection settings.
Memory storage is the default; `BROKER_STORAGE=file` enables atomic, synced local
snapshots. Rust's default data directory is `data/rust-broker`. Set
`BROKER_DATA_DIR` explicitly for deployments and give each running instance its
own directory. Local snapshots retain messages and consumer offsets, with receipts
reissued after restart. Snapshot writes are synchronous within the broker
mutation so request cancellation cannot reorder durable snapshots.

See [the broker protocol and configuration](embedded-broker.md) for endpoints,
frame limits, tokens, publication acknowledgements, consumer groups and retries.
The Go `examples/broker-client` can communicate with Rust over every supported
transport. NATS ingress uses request/reply on a broker-specific RPC subject.

Broker mode supports local JSON rules with `emit`, `command`, and `emit_event`.
Rule IDs and child events retain tenant identity. Canonical child events put
runtime identity under `meta`, and child IDs use the tenant-scoped Go formula.
Registry-bound contracts and scheduled rules fail closed. The local Rust rule
validator accepts object-path predicates and the existing Rust evaluator's
operators; full rule-language equivalence remains unverified. This broker is
single-instance and at-least-once, with receiver deduplication required for
repeated outputs. Replication and automatic failover are not implemented.

Native process checks include HTTP/TCP, SIGKILL recovery and disabled-network
startup. Add a NATS address for all nine transport pairs and a Go executable for
both cross-runtime directions:

```sh
cargo build --locked
python3 tests/broker_protocol.py
# With a local NATS server and a separately built Go worker:
python3 tests/broker_protocol.py --nats-address 127.0.0.1:4222 --go-worker /path/to/go-worker
```

The sections below describe the default `WORKER_MODE=durable` runtime, which uses
PostgreSQL and NATS JetStream. The Go-only `direct` mode is rejected explicitly by
Rust; local-rule processing with optional NATS is available through broker mode.

## Experimental status

The Rust worker publishes generic effects, commands, child events, explicit `service_event` effects, and subscription-router deliveries through NATS JetStream. It waits for JetStream publish acknowledgements before settling outbox/scheduler state. Subscription matching is isolated in `src/engine/subscriber_router.rs`; it creates durable delivery intents in the same transaction but does not evaluate rules.

It is still experimental, not a production replacement for Go. Rule/contract behavior, key ordering, lease rebalancing, observability, and crash/recovery behavior have not been proven equivalent with differential tests and live PostgreSQL/NATS integration tests. Keep Go selected until those gates pass.

## Isolation from Go

The existing Go worker source, Dockerfile, and Compose service are preserved. Rust is an independent service built by `Dockerfile.worker-rust` and defined as the opt-in `worker-rust` Compose service. It is not selected through a switch inside the Go container.

Start the Rust service only after stopping/scaling down the Go worker, because both runtimes use the same PostgreSQL shard leases and must not compete for the same shards:

```sh
docker compose stop worker
docker compose --profile rust up --build --scale worker-rust=${RUST_WORKER_REPLICAS:-3} worker-rust
```

Return to Go by stopping Rust and starting the existing worker again:

```sh
docker compose --profile rust stop worker-rust
docker compose up -d worker
```

## Module boundaries

- `src/main.rs`: process entrypoint and top-level error reporting.
- `src/config.rs`: environment parsing and validation.
- `src/broker_config.rs`: validation of selected broker dependencies and limits.
- `src/broker_runtime.rs`: broker/rule/adapter composition and bounded shutdown.
- `src/runtime/broker.rs`: retained partitions, consumer groups, receipts and capacity.
- `src/application/broker.rs`: shared broker operations, authentication and rule processing.
- `src/ports/broker.rs`: protocol v1, optional snapshot storage and output contracts.
- `src/adapters/broker_{http,tcp,nats}.rs`: optional network adapters.
- `src/adapters/broker_output.rs`: local or remote output publication.
- `src/adapters/broker_store.rs`: optional atomic local snapshots, reusing WAL directory-lock mechanics.
- `src/engine/local.rs`: bounded local rule loading through the existing evaluator.
- `src/runtime.rs`: lifecycle orchestration, migration bootstrap, initial lease acquisition, task supervision.
- `src/runtime/consumer.rs`: JetStream consumers, event validation, ACK/NAK decisions.
- `src/runtime/quarantine.rs`: durable quarantine writes.
- `src/runtime/effects.rs`: outbox claiming, NATS JetStream effect routing and retry settlement.
- `src/runtime/scheduler.rs`: scheduled-event claiming and publication.
- `src/engine.rs`: event-processing transaction orchestration and stable event/shard contracts.
- `src/engine/persistence.rs`: inbox, active revision, execution and outbox persistence.
- `src/engine/subscriber_router.rs`: transaction-scoped subscription matching and stable service-delivery intent construction.
- `src/engine/rules.rs`: compiled rule model, predicate evaluation, templates and action construction.
- `src/engine/lease.rs`: PostgreSQL shard lease acquisition, renewal and release.

Methods are split by responsibility so transport, persistence, scheduling, rule evaluation and configuration can be changed independently.

## Event and rule-set semantics

Rule-set authoring and activation are API/service operations backed by `rule_revisions` and `rule_activations`; this repository does not define a separate NATS rule-management event stream. Rust consumes business events whose `EventEnvelope.type` identifies the rule set and reads the active compiled revision from PostgreSQL.

The Rust service relies on the existing API service to apply the database migrations, then acquires/renews shard leases, verifies the event's shard, checks fencing and inbox deduplication in a PostgreSQL transaction, evaluates rules, persists execution/outbox rows and commits before ACK. Transient failures are retried; malformed/permanent events are quarantined. Separate loops publish outbox effects and due scheduled events.

## Current limits

- PostgreSQL only; MongoDB is unsupported.
- Existing compiled revisions are evaluated, but Rust does not yet implement a rule-authoring/compiler API or input-contract validation. Differential Go-vs-Rust tests are required.
- Fixed replica count and initial lease acquisition are implemented, but dynamic membership/rebalancing is not.
- The Go key-queue/backpressure model, complete observability/tracing and all shutdown/recovery behavior are not fully ported.
- Subscriber routing runs after a successful active-ruleset execution and adds stable tenant-scoped service delivery intents to the transactional outbox. Go/Rust parity tests and live PostgreSQL/NATS recovery tests are still required before production use.
- Real PostgreSQL/NATS integration tests, crash-after-commit-before-ACK tests and stale-owner failure injection remain outstanding.

Keep Go selected until the Rust service passes parity and integration tests. Do not run both runtimes against the same shard at once.

## Local checks

```sh
cd rust-worker
cargo fmt --all
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```
## Architectural boundary and contract safety

The worker is a rules execution runtime, not a subscriber registry or business router. Rule evaluation produces rule-declared effects only. A separate generic subscriber router matches the incoming event to active subscriptions in the same tenant and adds durable service delivery intents to the same transaction as execution/outbox persistence. It does not add business-specific behavior to the rules engine. Explicit `service_event` effects remain rule-declared outputs and use the generic dispatcher.

Go remains the supported runtime for contract-bound events. Rust currently fails closed when an event declares a contract or the active ruleset requires an input contract; it must not silently skip contract validation or binding-based ruleset resolution. Do not route contract-bearing production traffic to Rust until differential tests prove contract validation, binding resolution, and effect parity with Go.

Leases, inbox deduplication, execution records, the transactional outbox, retries, and broker acknowledgements are infrastructure guarantees. They may affect delivery reliability, but must not add business decisions or effects.
