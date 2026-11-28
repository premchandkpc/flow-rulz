---
title: Workers
type: reference
status: current
owner: premchandkpc
last_verified: never
---

# Workers

Workers are generic execution runtimes. They understand event envelopes, contracts, immutable ruleset revisions, execution records, and generic effects. They must not contain business-specific order, payment, or receiver logic.

## Deployment identity

The control plane can describe Domain/Service deployments and pinned ruleset revisions, but the current durable worker does not enforce those assignments or isolate service pools. `FLOW_SERVICE_ID` and a nonzero `FLOW_DEPLOYMENT_REVISION` therefore cause worker startup to fail until service-specific routing and ownership are implemented.

Events carrying `flow.service_id` or `flow.deployment_revision` headers, or `target_service_id` or `deployment_revision` metadata, are not processed by the current workers. Durable and direct workers retry those deliveries instead of acknowledging them. The HTTP ingest endpoint `POST /v1/events/{eventType}` and worker-hosted broker publish endpoints reject them with a validation error; previously retained targeted events remain retryable. Monitor pending deliveries and do not publish service-targeted events until the feature is implemented.

Blast radius (durable mode, by code reading, Unverified end-to-end): each shard's JetStream consumer allows one unacknowledged message (`MaxAckPending: 1`), so a held targeted event stalls that shard for every tenant mapped to it until it is removed.

## Quarantine limits

Contract violations (deprecated contract, schema-hash mismatch, wrong contract kind, payload failing the schema) are permanent and are quarantined after the first delivery. A quarantine entry stores the error and a `jetstream:flowrulz` reference, not the event payload. `Replay` only updates the entry's `updated_at`; it does not re-publish the event (Implemented, see `internal/adapters/postgres/quarantine.go`). Recover a quarantined event by re-publishing it from the producer.

## Processing guarantees

1. Validate event envelope and immutable contract reference.
2. Reject unsupported service-targeted events without acknowledging them.
3. Derive the shard from the event's tenant/partition pair.
4. Deduplicate by stable event identity and ruleset identity.
5. Evaluate active rulesets resolved from contract bindings or the event type, without subscriber-specific behavior.
6. Persist execution state and effects transactionally.
7. Acknowledge only after the durable transaction succeeds.
8. Retry transient failures; quarantine permanent or exhausted failures according to the runtime policy.

## Go and Rust parity

Both implementations must agree on event JSON, partition hash input, deployment routing metadata, contract identity, idempotency, and error classification. Add tests to both workers when changing a shared envelope or routing rule.

## Storage and transport status

Current runtime: NATS JetStream + PostgreSQL. The Rust worker's supported persistence/runtime capabilities must be checked against its implementation and CI; do not assume parity for an adapter that has not been implemented. The proposal under `proposals/kafka-log-first/` is reference code, not a production Kafka worker.
