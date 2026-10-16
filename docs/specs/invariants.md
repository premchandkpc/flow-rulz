# Cross-cutting FlowRulz invariants

**Status:** Engineering constraints, not proof of complete enforcement. Confirm source and test coverage for the exact release.

## Event and durability
1. ACK an input only after durable processing commits, or after a duplicate is confirmed from durable state.
2. Inbox/deduplication, execution outcome and outbox intent must share the required transaction boundary in the PostgreSQL processing path.
3. NATS JetStream is at-least-once. Do not claim exactly-once processing or external effects.
4. ACK means FlowRulz has accepted durable responsibility, not that a downstream effect completed.
5. Outbox effects may retry. Use stable effect identity plus destination dedupe or reconciliation when duplicates are unacceptable.
6. An ambiguous database commit or downstream timeout is not proof of failure; reconcile or retry idempotently.
7. Runtime caches accelerate reads; durable state is authoritative.
8. A permanent failure may be ACKed only after quarantine is durable. If quarantine persistence fails, preserve a recoverable delivery.

## Transport and retries
1. Current runtime transport is NATS JetStream plus PostgreSQL. Kafka/Redpanda proposals are not current runtime behavior unless implemented and tested.
2. Finite JetStream MaxDeliver is prohibited until exhausted-delivery recovery is durable and restart-safe. Unlimited delivery still requires monitoring and remediation.
3. Broker retries and scheduled-event retries are separate mechanisms.
4. Transient infrastructure errors remain retryable; permanent event/data failures follow the documented quarantine path.
5. In-process key serialization alone does not prove order across redelivery, worker handoff or concurrent publishers.

## Tenant, routing and configuration
1. Event tenant and partition identity come from meta.tenant_id and meta.partition_key, not Domain configuration.
2. A tenant field is data, not authentication. Producers must be authorized to assert it.
3. Tenant-scoped reads/writes must enforce scope at service/repository boundaries, not only in UI filters.
4. Sharding uses the shared tenant-plus-partition algorithm and configured shard count.
5. Hashes, subjects, partition keys and target-service values route work; they do not authorize callers.
6. Domain → Service assignments pin immutable ruleset revisions. Persisting an assignment does not prove workers automatically reconcile it.

## Rules, effects and leases
1. Published ruleset revisions are immutable.
2. Evaluation semantics must match the current evaluator and tests.
3. Subscriber fan-out is separate from ruleset evaluation and Service assignment.
4. A scheduled-event action is not proof that correlated workflow wait/resume is supported.
5. Go/Rust parity requires equivalent fixtures and tests for capabilities both runtimes claim.
6. A worker must not commit authoritative work after losing shard ownership or failing fencing validation.
7. Retry behavior must preserve required order or document the relaxation.
8. Poison events and quarantine-storage failures must remain observable and recoverable.

For every invariant, record enforcement point, test path, exact verification commit and status: Verified, Partial, Gap, Skipped, Unknown or Not applicable. Never count skipped tests as coverage.
