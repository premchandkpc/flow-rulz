# Rust worker service (experimental)

## Experimental status

The Rust worker publishes generic effects, commands, child events, and explicitly declared `service_event` effects through NATS JetStream. It waits for JetStream publish acknowledgements before settling outbox/scheduler state. It no longer queries tenant-domain subscriptions or synthesizes subscriber deliveries during event evaluation.

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
- `src/runtime.rs`: lifecycle orchestration, migration bootstrap, initial lease acquisition, task supervision.
- `src/runtime/consumer.rs`: JetStream consumers, event validation, ACK/NAK decisions.
- `src/runtime/quarantine.rs`: durable quarantine writes.
- `src/runtime/effects.rs`: outbox claiming, NATS JetStream effect routing and retry settlement.
- `src/runtime/scheduler.rs`: scheduled-event claiming and publication.
- `src/engine.rs`: event-processing transaction orchestration and stable event/shard contracts.
- `src/engine/persistence.rs`: inbox, active revision, execution and outbox persistence.
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
- Automatic subscription-based fan-out is not implemented in this branch. Subscriber registrations alone do not cause delivery; only explicit ruleset-declared effects are dispatched.
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

The worker is a rules execution runtime, not a subscriber registry or business router. A committed execution may contain only effects emitted by the selected ruleset. It must not query tenant-domain subscriptions or synthesize service-delivery effects outside the ruleset.

The control plane still manages tenant domains and service/subscriber registrations, but subscription-based fan-out is not currently implemented by this worker branch. A separate generic router should match events to authorized subscriptions and persist delivery intents through a defined transactional boundary; do not treat a registration as an active delivery path until that component and its tests exist. Explicit `service_event` effects remain rule-declared outputs and are dispatched through the generic effect path.

Go remains the supported runtime for contract-bound events. Rust currently fails closed when an event declares a contract or the active ruleset requires an input contract; it must not silently skip contract validation or binding-based ruleset resolution. Do not route contract-bearing production traffic to Rust until differential tests prove contract validation, binding resolution, and effect parity with Go.

Leases, inbox deduplication, execution records, the transactional outbox, retries, and broker acknowledgements are infrastructure guarantees. They may affect delivery reliability, but must not add business decisions or effects.
