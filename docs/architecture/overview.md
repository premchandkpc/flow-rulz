# Current architecture

The detailed current-state topology and file map live in [map.md](map.md). This document summarizes the package boundaries and transactional invariants, and deliberately distinguishes implementation from planned work.

## Layers and responsibilities

- cmd/api, cmd/worker, cmd/codegen and cmd/migrate are composition roots. They load configuration, assemble concrete adapters/use cases, own lifecycle and return errors before process exit.
- internal/domain owns business types, validation, state transitions, and stable identifiers/hashes. It must not import infrastructure.
- internal/rules validates/compiles rule source and evaluates compiled rules. Evaluation is side-effect-free; time-dependent behavior must use an injected clock.
- internal/services contains use-case orchestration and works through internal/ports rather than concrete drivers.
- internal/application and internal/runtime coordinate the worker loop, bounded key queue, shard leases/fencing and scheduled-event release.
- internal/ports contains storage, transaction, broker, clock and service interfaces. Some current transaction interfaces are too broad and expose implementation details; see the repository audit.
- internal/adapters implements HTTP, PostgreSQL, MongoDB, NATS JetStream, Redis, and effect destinations.
- internal/storage selects the configured storage backend and composes repositories for the command entrypoints.
- sdk/go is a nested independent module; examples/banking, examples/ecommerce and examples/food-delivery are also separate modules.

## Runtime path

1. Producers publish an EventEnvelope to the configured NATS subject family, currently events.shard.N.
2. The worker obtains shard leases and uses the bounded key queue to serialize events per partition key.
3. The event service validates the fencing token, deduplicates in the inbox, loads the active rule revision, evaluates it, and commits execution plus outbox effects inside a storage transaction.
4. The worker ACKs only after durable commit.
5. The effects service claims outbox entries. The effect router sends emit-event effects back to events.shard.N using the same configured shard count as the consumer; command effects go to the configured HTTP destination.
6. The scheduler claims due scheduled events and publishes them to the same stream subject family.

Delivery is at-least-once. Inbox/outbox patterns improve recovery and handoff correctness but do not prove exactly-once external delivery. The external destination must honor the supplied idempotency key.

## HTTP API

Routes are registered in internal/adapters/http/handlers. Rule creation/activation is POST /v1/rules/{ruleSet}/revisions; rule revisions and active rules are exposed through GET routes; DELETE on a revision path deactivates a rule set. Contract routes are under /v1/contracts. Workflow definition routes are under /v1/workflows/definitions. See ../../README.md for the route matrix.

Security limitation: authentication and authorization are not implemented. TenantExtractor trusts X-Tenant-ID or tenant_id query values and falls back to default. ../../AGENTS.md had mistakenly described a trusted-proxy token mode that is not implemented; do not rely on such a setting. The API must not be exposed to untrusted networks until verified identity and tenant authorization are implemented. /health is process liveness only; the API has no gRPC listener. The default native HTTP bind is loopback.

## Transaction and failure invariants

- Fencing assertion, inbox deduplication, rule lookup/evaluation, execution/outbox writes and inbox committed state must participate in the same transaction.
- ACK must happen after successful commit, not after merely receiving or validating a message.
- A duplicate inbox row must not create a second execution. An inconsistent duplicate without its execution must fail closed.
- Outbox delivery/retry/quarantine settlement requires a unique claim token and an unexpired claim in SQL and Mongo. Durable settlement errors are returned. Quarantine records and outbox state still live in separate repositories, so cleanup after a lost claim can still fail and require repair.
- Persisting a quarantine record or delivery/retry state can fail; logging those failures and returning success is not acceptable. Current effect service still needs that hardening.
- Keep producer, scheduler, effect router and consumer aligned on base subject and shard count. Event type is part of the envelope, not an alternate stream subject.
- The scheduler can reclaim expired claims and its settlement methods are claimant/expiry fenced. Transient publish failures are still marked terminally failed instead of retried with backoff.
- Mongo stores JSON payload/source/context fields as binary bytes rather than casting JSON bytes to bson.Raw. BSON round-trip regression tests cover these fields; live Mongo/PostgreSQL conformance remains unverified.
- Batch queries are tenant-scoped, but batch event execution still lacks a valid shard lease/fencing token and key-queue integration. BATCH_MODE values other than none are rejected until that is implemented.
- LeaseManager targets NUM_SHARDS/3 per worker without dynamic membership/rebalancing; the current behavior assumes three replicas and can leave shards unowned when replica count differs.
- External destination calls require bounded timeouts and receiver idempotency. Do not imply that the broker transaction includes a remote HTTP side effect.

## Configuration and lifecycle

Configuration is parsed in internal/config. API and worker construct dependencies in their cmd package, then call internal/storage. Close storage, broker clients and observability on every shutdown/error path. Do not invoke fatal process exit from a function that still owns deferred cleanup.

The local Docker Compose file uses development settings and example credentials. Production deployments must inject real secrets, use trusted tenant identity, verify TLS, and provide an explicit effect destination. Migration CLI currently implements up only. The API starts HTTP only.

## Testing architecture

- internal/*_test.go covers focused unit/regression behavior.
- tests/contract uses in-memory repositories and provides only partial contract coverage.
- tests/integration currently relies on mocks/in-memory repositories; it is not live PostgreSQL/NATS integration.
- Root Go checks, SDK checks and example-module checks run as separate Go module steps in CI.
- Production guarantees require actual PostgreSQL/NATS tests for rollback, redelivery, ACK loss, competing claimants, worker pause/lease expiry, broker restart and destination timeout/idempotency.

See ../../AGENTS.md for the detailed package/file map and ../audit/2026-10-11-repository-review.md for the prioritized unresolved issues.
