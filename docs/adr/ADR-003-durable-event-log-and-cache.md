# ADR-003: Durable event log and disposable runtime cache

- Status: Accepted
- Date: 2026-10-14

## Decision

- Accepted application messages are stored in file-backed NATS JetStream streams with limits-based retention. Acknowledging a message does not delete it; the retained stream is replayable.
- PostgreSQL stores transactional projections and processing guarantees: inbox/deduplication records, executions, rule activation state, leases/fencing where configured, scheduled events, and the transactional outbox.
- In-memory and Redis caches are accelerators only. Cache misses fall back to the authoritative store; cache eviction or Redis loss must never lose an accepted event or change business correctness.
- Go application code depends on ports. NATS JetStream is the current log/broker adapter. A future Kafka/Redpanda or other append-only log adapter must implement the same application-facing contracts rather than leak provider types into use cases.
- Outbox rows bridge PostgreSQL transactions to outgoing JetStream messages. The worker acknowledges an incoming event only after its PostgreSQL transaction commits.

## Delivery and replay

Delivery is at-least-once. A crash after publish but before outbox settlement may duplicate an outgoing message. Recreating a durable consumer or replaying a stream can replay historical messages. Event handlers and downstream consumers must deduplicate by stable event/effect ID.

## Retention and migration

The stream uses limits-based retention and file storage rather than work-queue retention, which deletes acknowledged records. Operators must set retention/size limits to match replay, audit, and storage requirements, monitor stream capacity, and archive records if they must outlive the JetStream retention window.

JetStream does not allow changing a stream's retention policy in place. To migrate an existing work-queue stream, stop producers and consumers, let in-flight work finish, verify there are no pending deliveries, then delete/recreate the stream with limits retention before restarting services. Previously acknowledged messages cannot be recovered from the old work-queue stream because that policy already deleted them.
