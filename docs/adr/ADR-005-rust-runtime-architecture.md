# ADR-005: Rust runtime and FlowRulZ-owned log

Status: Proposed. Date: 2026-10-14.

The Rust process is a stateless execution service. It owns no durable business truth in memory or local files. FlowRulZ owns append-only log semantics: partition order, stable record identity, progress, replay, retention and recovery. NATS JetStream can remain an ingress/transport adapter during migration, but is not automatically the authoritative processing log. The worker is stateless; the system is not.

## Invariants

1. Accept only after the input fact is durable.
2. Facts are immutable; corrections append new facts.
3. Event, execution and effect identities remain stable across retries and replay.
4. Ordering is per tenant + partition key; unrelated keys execute concurrently.
5. Execution pins immutable rule revisions, content hashes, contract versions and compiler version.
6. Input progress advances only after result and all effect intents commit durably.
7. Arbitrary external delivery is at-least-once unless the destination supports idempotency.
8. PostgreSQL and Redis become rebuildable projections/caches after migration.
9. Fencing is checked at the authoritative commit boundary.
10. Quarantine is durable before progress advances.

## Hexagonal modules

Domain: canonical events, identities, rule/contract refs, effects, execution commits and log positions. Ports: EventLog, RuleRegistry, ContractRegistry, EffectSink, ProjectionStore, Lease and Scheduler. Application: small AcceptEvent, ProcessEvent, ProcessBatch, DispatchEffect, ScheduleEvent, ReplayPartition and RebuildProjection use cases. Engine: pure compiler/evaluator/output builder. Adapters: FlowRulZ log, NATS bridge, PostgreSQL registry/projections, Redis cache and effect destinations. Runtime: dependency wiring, bounded concurrency, supervision and graceful shutdown.

Use constructor injection and narrow interfaces. Avoid global service locators, giant runtime objects and generic repositories that hide transaction semantics.

## Zero-copy

Use immutable reference-counted buffers such as Bytes, borrow payloads where practical, parse JSON once, retain canonical original bytes, and allocate only when output transforms data. Bound in-flight work and payload size. Durable storage still persists bytes: zero-copy means minimizing unnecessary hot-path copies, not copy-free disk durability.

## Atomic processing

Append event.accepted; pin immutable rule revisions; evaluate without I/O; atomically append execution result and the complete effect-intent set; advance input progress only after that commit; dispatch effects separately; append settlement facts; rebuild PostgreSQL projections from the log. If group commit is unavailable, one execution.committed record must contain both result and effects. Never expose success before effects are durable.

Rule sets declare multiple input contracts and output contracts. Outputs route to consumers, another rule set, workflows, schedules, request/reply or named internal processors. Rule-set routing creates a new durable event, not a nested evaluator call. Use deterministic child IDs, causation chains and hop limits. Single, batch, scheduled, workflow, request/reply and replay modes share the same envelope and execution engine. Batch outcomes are per item and preserve same-key order. Schedules are durable facts plus a rebuildable due-time index, not just in-memory timers.

## Rollout

1. Domain and ports with unit/property tests.
2. Segmented WAL prototype with checksums, record-ID index, recovery, replay and retention.
3. Replication/quorum, leader epochs and fencing. A local WAL is not a distributed broker.
4. Refactor Rust use cases while retaining PostgreSQL mode behind a feature flag.
5. Rule/contract parity, routing, effects, scheduling and batches.
6. Projection rebuild and shadow comparison.
7. Crash-boundary integration tests.
8. Demote PostgreSQL from correctness authority only after replay rebuilds all required state.

Fail closed when the log cannot durably commit. Do not promise exactly-once delivery to arbitrary external systems.