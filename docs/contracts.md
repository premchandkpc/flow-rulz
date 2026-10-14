# Multi-contract model

## Identity and immutability

A contract is identified by `(name, version)`; its canonical `schema_hash` identifies the exact semantic schema. Contract versions are immutable once referenced by a rule revision or published to producers. Create a new version for a semantic change. `description`, owner, and timestamps are metadata and do not affect the schema hash.

## Message envelope and child contracts

The common envelope carries `message_id`, `tenant_id`, `message_kind`, `contract.name`, `contract.version`, `contract.schema_hash`, `correlation_id`, `causation_id`, `partition_key`, and `payload`. Event, command, and effect payloads are independently versioned child contracts. The parent envelope explicitly lists allowed child contract identities. A receiver declares accepted contract identities and versions; matching a subject alone is not authorization or schema acceptance.

## Routing

- Event ingress validates the envelope, contract identity, schema hash, tenant, and payload before broker publication.
- Rule revisions pin the exact input contract version and hash. Activation fails when that contract is missing, deprecated, or hash-mismatched.
- Child event emission is routed through NATS JetStream, never by direct invocation of another rule set. The event contract selects eligible rule sets through explicit bindings.
- Single-send, batch-send, and fan-out use the same envelope and validation rules. Batch validation is per item; invalid items must be reported individually and must not partially publish unless the API explicitly declares partial-success semantics.
- Commands and effects use distinct message kinds and contract identities. Service registrations list accepted contract identities, not only broad event domains.

## Request/response

A request has a stable message ID and correlation ID, and declares the expected response contract. Correlation state is durable and tenant-scoped. Its state machine is `PENDING -> DISPATCHED -> WAITING_FOR_RESPONSE -> COMPLETED | FAILED | TIMED_OUT | CANCELLED | QUARANTINED`. Responses must match tenant, correlation ID, and expected contract identity/hash. A timeout is durable and idempotent. A synchronous wait is only a bounded convenience over the durable asynchronous workflow.

## Delivery guarantees

NATS JetStream is the single application-message transport. PostgreSQL remains the persistence protocol; the HTTP API is control-plane management only. Broker delivery is at-least-once. Consumers deduplicate on stable message/effect IDs. Contract validation must occur before ACK-worthy durable execution, and schema hashes are not substitutes for tenant authorization.
