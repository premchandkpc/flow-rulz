# FlowRulz log-first proposal skeleton

This directory is a **reference sketch**, not the current FlowRulz runtime and not a production-ready broker/storage adapter.

## Architecture boundary

FlowRulz owns the **log-storage contract**, event semantics, validation, rule evaluation, Domain → Service deployment assignments, subscriber routing, and application orchestration. It must not own a particular storage engine.

- `storage.go` defines the application-owned `LogStore` / `LogTransaction` ports.
- `worker.go` sketches processing: each worker is pinned to a Domain/Service deployment ID and revision, validates that routed events target that deployment, resolves only its assigned rulesets, evaluates into generic effects, appends outputs/audit records, and persists the input checkpoint atomically through the storage port.
- `scheduler.go` sketches one-shot schedule firing through a transaction boundary.
- `../../docs/proposals/kafka-log-first-data-plane.md` documents the log contract, schema, ordering, migration, and operations.

The storage implementation is an adapter behind these ports. Start with one explicitly selected implementation; a later third-party persistence service can implement the same contract without changing domain/application logic. Do not let handlers or domain packages import Kafka, NATS, SQL, filesystem, or vendor SDK types.

## Service deployment boundary

A Domain contains Services. Each Service declares the ruleset revisions and contract bindings assigned to its worker deployment. The ingress router routes an event to the relevant service work stream; each worker group consumes only its assigned service stream and must not load all tenant rulesets. Scaling one service changes only that service worker pool.

Do not confuse a Service deployment (where rules execute) with a subscriber/receiver registration (where an emitted event is delivered). They are separate configuration concepts and must have separate IDs, lifecycle, and authorization. Subscriber fan-out remains a generic post-evaluation routing stage.

## Adapter contract

Any implementation must preserve these semantics, not merely expose CRUD-shaped methods:

1. Durable append acknowledgement: a successful append means the configured durability policy has been met.
2. Atomic output + input-position commit for worker processing, or an explicitly documented equivalent protocol.
3. Stable ordering for the same tenant and partition key.
4. Idempotent append/retry behavior using stable record IDs.
5. Replay from a known position and explicit retention/compaction behavior.
6. Tenant authorization and isolation enforced by the adapter/service, not inferred from tenant IDs in keys.
7. Bounded retries, cancellation, timeouts, backpressure, and observable failure categories.

A remote storage service adapter will add network timeouts and ambiguous commit outcomes. Callers must resolve those using idempotency keys and transaction status/retry semantics; they must never assume a timeout means the append failed.

## Deliberate omissions

- No actual Kafka client or remote storage-service adapter.
- No claim that this reference sketch is production-ready.
- No real timer heap, retry/DLQ implementation, or schema-registry integration.
- No end-to-end exactly-once claim for external receivers.

The current NATS JetStream + PostgreSQL runtime remains active until a separately reviewed migration is implemented and its real-broker failure gates pass. This proposal does not switch production traffic.

## Current API and worker rollout contract

- The management API represents execution deployments as `deployments` on a tenant Domain. Each entry pins a `service_id`, deployment `revision`, and immutable `ruleset_id`/revision assignments. PostgreSQL stores this as JSONB; MongoDB stores the equivalent BSON document field. The standalone contract is `contracts/schemas/service-deployment.schema.json`.
- This execution-deployment model is intentionally separate from the existing `/v1/services` subscriber registry. Subscriber registrations continue to drive generic post-evaluation delivery only.
- Go and Rust workers accept `FLOW_SERVICE_ID` plus `FLOW_DEPLOYMENT_REVISION` as a paired deployment identity. When configured, they fail closed on events whose `flow.service_id` or `flow.deployment_revision` headers do not match. `NATS_RULESETS` / `NATS_RULESETS` (Rust) must be generated from that Domain Service's pinned assignment; do not configure a service worker with every tenant ruleset.
- The current NATS/PostgreSQL data plane does not yet dynamically publish deployment changes to worker pools. The API persists and validates assignments, while rollout tooling must currently translate the selected deployment revision into worker environment/config and routed-event headers. The Kafka router in this proposal is the future automated distribution path, not a feature claimed to exist today.
