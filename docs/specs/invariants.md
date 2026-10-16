# Cross-cutting FlowRulz invariants

**Status:** Engineering constraints. Confirm implementation and test coverage before claiming an invariant is fully enforced.

## Event processing and durability
1. A broker delivery is acknowledged only after the durable outcome is committed or a duplicate is positively identified.
2. Inbox deduplication, execution state, and outbox intent must share the required atomic transaction boundary for the selected backend.
3. NATS JetStream delivery is at-least-once. Do not describe broker delivery or external effects as exactly-once.
4. External effects may be retried. Destinations must honor stable idempotency keys if duplicate external actions are to be prevented.
5. Runtime caches are disposable acceleration, not authoritative durable state.
6. An unknown commit outcome must not be treated as confirmed rollback or success without reconciliation.

## Tenant and event identity
1. Runtime tenant identity and partition key are event-level values, not global Domain configuration.
2. Tenant-scoped reads and writes must enforce tenant scope at the persistence/query boundary, not only in the UI.
3. Shard selection must use the shared routing implementation and configured shard count.
4. A hash or subject name is a routing identifier, not authorization.

## Rules and configuration
1. Published rule revisions are immutable; changes create a new revision.
2. Domain → Service → ruleset assignments pin explicit revisions.
3. Rule evaluation must be deterministic for the same revision, input, and supported runtime semantics.
4. A proposal or roadmap item is not an implemented capability.
5. Subscriber routing is separate from rule evaluation and execution Service assignment.

## Concurrency and recovery
1. A worker must not continue authoritative work after losing shard ownership or failing fencing validation.
2. Per-process key serialization alone does not prove distributed ordering across redelivery, worker handoff, or concurrent publishers.
3. Retry policy must preserve required ordering or explicitly document the relaxation.
4. Poison-message and maximum-delivery exhaustion paths need an explicit observable recovery/quarantine policy.

For every invariant, record its enforcement point and regression test. Mark gaps plainly; do not infer coverage from a test name or skipped test.
