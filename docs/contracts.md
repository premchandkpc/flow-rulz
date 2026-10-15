# Multi-contract model and implementation status

## Identity and immutability

A contract is identified by `(name, version)`; its canonical `schema_hash` identifies the exact semantic schema. Contract versions are immutable once referenced by a rule revision or published to producers. Create a new version for a semantic change. `description`, owner, and timestamps are metadata and do not affect the schema hash.

## Message envelope and child contracts

The event envelope carries `id`, `type`, `occurred_at`, `data`, and optional `contract`, `message_kind`, `correlation_id`, and `causation_id`. Runtime identity (`tenant_id`, `partition_key`) and extensible runtime context live under the event's `meta` object. These values belong to each event, never to Domain/Service/ruleset configuration. Event, command, and effect payloads are independently versioned child contracts. The parent envelope explicitly lists allowed child contract identities. A receiver declares accepted contract identities and versions; matching a subject alone is not authorization or schema acceptance.

## Routing

- Event ingress validates the envelope, contract identity, schema hash, tenant, and payload before broker publication.
- Rule revisions pin the exact input contract version and hash. Activation fails when that contract is missing, deprecated, or hash-mismatched.
- Child event emission is routed through NATS JetStream, never by direct invocation of another rule set. The event contract selects eligible rule sets through explicit bindings.
- Single-send, batch-send, and fan-out use the same envelope and validation rules. Batch validation is per item; invalid items must be reported individually and must not partially publish unless the API explicitly declares partial-success semantics.
- Commands and effects use distinct message kinds and contract identities. Service registrations list accepted contract identities, not only broad event domains.

## Request/response

A request has a stable message ID and correlation ID, and declares the expected response contract. Correlation state is durable and scoped by runtime event identity. Its state machine is `PENDING -> DISPATCHED -> WAITING_FOR_RESPONSE -> COMPLETED | FAILED | TIMED_OUT | CANCELLED | QUARANTINED`. Responses must match the originating event identity, correlation ID, and expected contract identity/hash. A timeout is durable and idempotent. A synchronous wait is only a bounded convenience over the durable asynchronous workflow.

## Delivery guarantees

NATS JetStream is the single application-message transport. PostgreSQL remains the persistence protocol; the HTTP API is control-plane management only. Broker delivery is at-least-once. Consumers deduplicate on stable message/effect IDs. Contract validation must occur before ACK-worthy durable execution, and schema hashes are not substitutes for producer authorization.

## Current implementation boundary

Implemented in this change: contract kinds, canonical schema hashes, immutable semantic versions, explicit parent-child allowlists, payload type/required/enum validation, validation at the existing event-ingestion boundary and worker execution, and input-contract hash pinning during rule activation.

Implemented in the Go event service: contract-to-ruleset binding lookup with contract hash and pinned input-contract checks. Not yet implemented: receiver-side accepted-contract declarations, per-item contract validation for batch send, and durable request/response correlation state. Rust contract-to-ruleset parity is not yet complete. The request/response state machine above is the required design, not a claim that those workflow transitions already exist in code. The legacy HTTP event-ingestion endpoint also remains for SDK compatibility; it must not be described as a NATS-only producer ingress until that compatibility path is removed or replaced.
