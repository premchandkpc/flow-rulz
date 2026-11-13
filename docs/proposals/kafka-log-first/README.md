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

A Domain contains Services. Each Service declares the immutable ruleset revisions assigned to its worker deployment. Contract-to-ruleset bindings remain independent, versioned records; the router resolves the event contract against those bindings and intersects the matching rulesets with each target service's assigned revisions. Each worker group consumes only its assigned service stream and must not load rulesets outside its assigned deployment. Scaling one service changes only that service worker pool.

Do not confuse a Service deployment (where rules execute) with a subscriber/receiver registration (where an emitted event is delivered). They are separate configuration concepts and must have separate IDs, lifecycle, and authorization. Subscribers declare their own `event_types` on `/v1/subscriptions`; Domain configuration does not carry event types. Subscriber fan-out remains a generic post-evaluation routing stage.

## Adapter contract

Any implementation must preserve these semantics, not merely expose CRUD-shaped methods:

1. Durable append acknowledgement: a successful append means the configured durability policy has been met.
2. Atomic output + input-position commit for worker processing, or an explicitly documented equivalent protocol.
3. Stable ordering for the same tenant and partition key.
4. Idempotent append/retry behavior using stable record IDs.
5. Replay from a known position and explicit retention/compaction behavior.
6. Event tenant identity is validated at ingress and retained for runtime isolation; Domain and Ruleset configuration is global.
7. Bounded retries, cancellation, timeouts, backpressure, and observable failure categories.

A remote storage service adapter will add network timeouts and ambiguous commit outcomes. Callers must resolve those using idempotency keys and transaction status/retry semantics; they must never assume a timeout means the append failed.

## Deliberate omissions

- No actual Kafka client or remote storage-service adapter.
- No claim that this reference sketch is production-ready.
- No real timer heap, retry/DLQ implementation, or schema-registry integration.
- No end-to-end exactly-once claim for external receivers.

The current NATS JetStream + PostgreSQL runtime remains active until a separately reviewed migration is implemented and its real-broker failure gates pass. This proposal does not switch production traffic.

## Current API and worker rollout contract

- The management API represents execution deployments as `services` nested under a Domain. Each entry pins a `service_id`, deployment `revision`, and immutable `ruleset_id`/revision assignments. Contract-to-ruleset bindings remain independent and can fan one contract out to multiple rulesets. PostgreSQL stores deployments as JSONB; MongoDB stores the equivalent BSON document field. The public Domain API does not accept tenant identity, event types, or partition keys as configuration. These arrive on runtime events. The standalone contract is `contracts/schemas/service-deployment.schema.json`.
- This execution-deployment model is intentionally separate from the generic `/v1/subscriptions` registry. Subscriber registrations continue to drive generic post-evaluation delivery only.
- Current Go and Rust workers reject `FLOW_SERVICE_ID` and a nonzero `FLOW_DEPLOYMENT_REVISION`: isolated deployment streams and pinned-rule enforcement are not yet implemented. Targeted events are not acknowledged by the durable path.
- The current NATS/PostgreSQL data plane does not dynamically publish deployment changes to worker pools. The API persists and validates assignments; the service-specific routing and automated distribution described here remain proposed work.
