# ADR-002: Transactional outbox persistence boundaries

- Status: Accepted
- Date: 2026-10-14

## Decision

1. NATS JetStream is the durable append-only log for accepted messages. PostgreSQL is the authoritative transactional store for processing state: inbox deduplication, executions, and outgoing outbox effects. These stores serve different roles; neither substitutes for the other.
2. For a processed event, the worker writes the inbox state, execution record, and outbox effects through repositories created from the same database transaction. The broker ACK occurs only after that transaction commits.
3. The outbox publisher claims committed rows, publishes to NATS JetStream, and records delivery/retry/quarantine state through a persistence port. Publishing is at-least-once; consumers and destinations must use stable idempotency keys.
4. Application code depends on ports, not on PostgreSQL, Redis, or NATS concrete clients. The outbox ports are split by responsibility:
   - `OutboxWriter`: insert effects inside the event transaction.
   - `OutboxDispatcher`: claim and settle delivery attempts.
   - `OutboxReader`: read pending effects and execution traces.
   - `OutboxRepository`: composite adapter contract for storage wiring and compatibility.
5. Redis is optional for cache/coordination optimizations only. It is not the source of truth for outbox state, and correctness must not depend on Redis availability.
6. NATS JetStream is both the durable message log and delivery transport. PostgreSQL remains the transactional state store. Stream retention must preserve records for the required replay and audit window.

## Failure behavior

- Crash before database commit: inbox, execution, and outbox writes roll back together; the broker delivery can be retried.
- Crash after commit but before broker ACK: redelivery is deduplicated by the inbox.
- Crash after publishing but before marking an outbox row delivered: the effect can be published again, so stable effect IDs and consumer idempotency are required.
- Redis unavailable: event processing and outbox delivery remain correct; only optional optimizations may be unavailable.\n- A consumer is recreated: retained messages may be replayed, so stable IDs and idempotent consumers are mandatory.

## Consequences

The event-processing use case needs only the write port, and trace handlers need only the read port. Adapters can be replaced without exposing driver-specific types to application services. Alternative database adapters must explicitly preserve the required transaction semantics; matching method names alone is not sufficient.
