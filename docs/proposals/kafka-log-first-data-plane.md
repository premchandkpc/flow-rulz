# Kafka/Redpanda log-first data-plane proposal

**Status:** proposal only. The current production path remains NATS JetStream + PostgreSQL until the migration gates below pass. This document does not claim the Kafka implementation exists.

## Decision summary

For a future low-dependency deployment, evaluate **Redpanda or Kafka KRaft as the durable event log**, a single Go codebase with separate `api`, `worker`, and `scheduler` process roles, and no database on the event-processing hot path. Keep ClickHouse optional for query-heavy history. Do not run a Kafka and NATS data plane in parallel indefinitely.

This removes PostgreSQL from the worker hot path, but it does **not** make state disappear. The broker log becomes the durable source of truth for configuration and scheduling, while worker caches and scheduler heaps are disposable projections rebuilt from compacted topics. The API needs a query/read model for listing rules and tenants. That model may initially be an embedded local store, but a single local SQLite file is not automatically shared or highly available across API replicas.

### Explicit constraints

- Workers are stateless with respect to ownership: no shard leases, no replica-count assumptions, and no correctness-critical in-memory state.
- Rule evaluation is pure and domain-agnostic. Workers understand event envelopes, immutable rule revisions, contracts, and generic effects, not service-specific business behavior.
- **A Domain contains Services; each Service is a deployment boundary that declares which rulesets/revisions its worker pool runs.** A worker is configured for one service deployment and loads only that deployment's assigned rules. It must not load every tenant ruleset and decide at runtime which service owns it.
- Keep execution-service deployment separate from subscriber/receiver registration. A service deployment owns ruleset assignments and worker capacity; a subscriber registration expresses interest in delivered events. Do not overload one registry record to mean both.
- Subscriber fan-out remains a generic routing stage. It emits one deterministic effect per active tenant-scoped subscriber; it is not folded into rule evaluation.
- Same-key ordering is provided by a stable Kafka key and partition assignment. A hot key is still limited to one partition's throughput.
- Kafka transactions make input-offset commits and Kafka output records atomic within Kafka. They do not include receiver databases, HTTP calls, or ClickHouse writes.
- External receivers use idempotency keys and process deliveries at least once.
- The current NATS/PostgreSQL implementation stays the rollback path until the migration is proven with real-broker failure tests.

## Deployables and dependencies

| Deployable | Role | Correctness-critical state |
|---|---|---|
| `flowrulz --role=api` | Validate and publish contracts, rule revisions, activation records, tenant/service config, and schedule commands | Broker writes; disposable query projection |
| `flowrulz --role=router` (may share the worker binary) | Consume ingress events, resolve Domain → Service assignments, and publish one routed copy per target service deployment | Kafka transaction and compacted deployment projection |
| `flowrulz --role=worker --deployment=<domain/service>` | Consume only the configured service deployment stream; load/evaluate only rules assigned to that deployment; emit generic effects and subscriber intents | Kafka transaction only; deployment-scoped rebuildable cache |
| `flowrulz --role=scheduler` | Consume assigned schedule partitions, rebuild timer heap, publish due events and schedule tombstones transactionally | Compacted schedule topic; rebuildable heap |
| Optional sink consumer | Write audit/history to ClickHouse or object storage | Sink-owned checkpoints; not on hot path |

These are **process roles**, not necessarily separate codebases. The router is a module/loop, not a mandatory additional microservice. Deploy API, router, each service worker pool, and scheduler independently only where independent scaling is useful. Broker is the required infrastructure. ClickHouse is optional. Do not add Redis, a routing microservice, or a sidecar unless measurements establish a concrete need.

## Topic contract

Use a topic prefix configurable per environment, shown below as `flow.`. Configure replication factor, min in-sync replicas, retention, ACLs, and quotas for production. Topic creation and partition counts are managed infrastructure, not ad-hoc worker behavior.

| Topic | Key | Value / meaning | Cleanup |
|---|---|---|---|
| `flow.events` | `tenant_id + ":" + partition_key` | Versioned `EventEnvelope` | Time/size retention, sized for replay policy |
| `flow.rulesets` | `ruleset_id + ":" + revision` | Immutable compiled/source rule revision | Compacted; retain revision history through explicit policy |
| `flow.ruleset-activation` | `ruleset_id` | Active revision and optional effective-from marker | Compacted |
| `flow.contracts` | `contract_id + ":" + version` | Immutable JSON Schema / contract metadata | Compacted |
| `flow.contract-ruleset-bindings` | `contract_id + ":" + version + ":" + ruleset_id` | Independent contract-to-ruleset binding, activation policy, and priority; one contract may bind to multiple rulesets | Compacted |
| `flow.domains` | `domain_id` | Event domain configuration | Compacted |
| `flow.services` | `domain_id + ":" + service_id` | Service identity within a domain, deployment metadata, worker profile, and status | Compacted |
| `flow.service-deployments` | `domain_id + ":" + service_id` | Immutable deployment revision: assigned ruleset IDs/revisions, concurrency/resource profile, activation generation | Compacted |
| `flow.service-work.<service-token>` | `tenant_id + ":" + partition_key` | Routed event copy addressed to one service deployment; target service ID is part of the envelope | Time/size retention; separate topic/ACL boundary per deployed service or deployment group |
| `flow.subscriptions` | `subscriber_id` | Receiver subscription filters, independent of execution-service deployment | Compacted |
| `flow.schedules` | `tenant_id + ":" + schedule_id` | Schedule upsert; tombstone on cancel/fire | Compacted |
| `flow.commands` | Destination-specific key | Generic command envelope | Time/size retention |
| `flow.service-deliveries` | `tenant_id + ":" + service_id + ":" + event_id` | One delivery intent per matching subscriber | Time/size retention |
| `flow.audit` | `tenant_id + ":" + execution_id` | Execution, selected revision, outcomes, correlation IDs | Time/size retention; sink for long-term history |
| `flow.retry` | Original partition key | Retry envelope, attempt, not-before timestamp | Time/size retention |
| `flow.dlq` | Original partition key | Poison event plus failure metadata | Operational retention and replay policy |

### Partitioning and ordering

1. `flow.events` uses a stable key derived from tenant ID and the declared partition key. Keep tenant identity in the key to avoid cross-tenant key collisions.
2. Choose the partition count before production and document how it changes. Increasing a topic's partition count can remap keys and break ordering across the transition; use a versioned topic/migration protocol if strict ordering must survive repartitioning.
3. A consumer group gives one active group member ownership of a partition at a time, but processing must stop and abort the transaction when the consumer loses its assignment. Do not continue side effects after a rebalance.
4. Ordering is per key/partition, not global. A single hot key cannot be parallelized without changing its ordering contract.
5. Retry topics can break original order if later records continue. For strict per-key order, pause/hold the affected partition/key until retry resolution or use a retry design that preserves sequence. Do not promise ordering if delayed retry records are allowed to overtake.

### Important limitation: rule activation versus events

A compacted activation topic and an event topic do not by themselves establish a total order between a rule activation and an event. Workers must expose the revision they actually used. Define activation semantics before shipping:
- **Eventual activation (simpler):** workers apply activation records as they consume them; events can temporarily use the prior revision.
- **Strict cut-over:** activation records include a versioned barrier/effective offset protocol, and producers/workers enforce the barrier. This is a separate feature and needs dedicated tests.

Do not claim that merely writing to two topics makes activation atomic.

## Versioned schemas

Use a schema registry or a checked-in schema compatibility pipeline. Every envelope has `schema_version`; reject unsupported major versions to a quarantine/DLQ path. The examples below are normative field intent, not a complete JSON Schema document.

### EventEnvelope v1

```json
{
  "schema_version": 1,
  "id": "stable-producer-event-id",
  "tenant_id": "tenant-123",
  "event_type": "orders.created",
  "partition_key": "order-42",
  "occurred_at": "2026-10-15T05:00:00Z",
  "payload": {"order_id": "order-42"},
  "metadata": {
    "correlation_id": "corr-1",
    "causation_id": "event-parent",
    "batch_id": null,
    "hop": 0,
    "requested_ruleset_id": null
  }
}
```

Requirements:
- `id` is stable across producer retries. If the producer cannot provide a stable ID, the API must issue one before publishing and return it.
- `tenant_id`, `event_type`, and `partition_key` are required and validated before enqueue.
- `payload` is a JSON object validated against the contract selected for this event.
- Increment `metadata.hop` on child events and quarantine at the configured maximum to prevent cycles.
- Batch is a producer-side splitter that publishes independent envelopes with stable IDs and a shared `batch_id`; it is not an alternate in-worker execution path.
- Scheduled events use the same envelope shape and deterministic IDs derived from schedule ID plus scheduled occurrence.

### RulesetRevision v1

```json
{
  "schema_version": 1,
  "tenant_id": "tenant-123",
  "ruleset_id": "orders-risk",
  "revision": 7,
  "contract_id": "orders.created",
  "contract_version": 2,
  "compiler_version": "flowrulz-rules-v1",
  "content_hash": "sha256:...",
  "source": {},
  "compiled_artifact": {},
  "created_at": "2026-10-15T05:00:00Z"
}
```

Revisions are immutable. Key the cache by tenant, ruleset ID, revision, and compiler/runtime compatibility version. Verify the content hash before caching. Activation points to a revision; it must never mutate an existing revision. Record the exact revision and content hash in every execution audit record.

### DomainServiceDeployment v1

A domain service is the deployment target for a set of rulesets. The deployment revision is immutable; activating a new assignment publishes a new deployment revision rather than mutating a running worker's interpretation in place.

```json
{
  "schema_version": 1,
  "tenant_id": "tenant-123",
  "domain_id": "orders",
  "service_id": "risk-engine",
  "deployment_revision": 4,
  "worker_profile": {"min_replicas": 2, "max_replicas": 8},
  "rulesets": [
    {"ruleset_id": "orders-risk", "revision": 7},
    {"ruleset_id": "fraud-check", "revision": 3}
  ],
  "status": "active",
  "updated_at": "2026-10-15T05:01:00Z"
}
```

The deployment resolver validates that every referenced immutable ruleset revision exists before publishing the deployment. Contract-to-ruleset bindings remain independent, versioned control-plane records. At event routing time, resolve the event contract against active bindings, then intersect the matching rulesets with the target service deployment’s assigned immutable revisions. This preserves multi-contract fan-out without duplicating contract identity inside deployment assignments. Workers are started/configured with a stable `deployment_id` (`tenant/domain/service`) and consume only that deployment's work stream. On rollout, a new deployment revision warms its cache and reports readiness before traffic shifts; the old revision drains before it is retired. Do not let every worker subscribe to every service's rulesets.

### Routed event metadata

The producer-facing event envelope does not choose an internal worker deployment. After validating the event, the router resolves the global Domain → Service deployment mapping and writes a routed copy to the target service work stream. Tenant identity and partition key are taken from the event envelope and are not part of Domain configuration. The routed copy adds trusted internal metadata:

```json
{
  "target_service_id": "risk-engine",
  "deployment_revision": 4
}
```

Workers reject a routed record if these fields do not match the deployment identity configured at process startup. These fields are routing metadata, not authorization; broker ACLs and event identity validation are still required.

### RulesetActivation v1

```json
{
  "schema_version": 1,
  "tenant_id": "tenant-123",
  "ruleset_id": "orders-risk",
  "active_revision": 7,
  "activation_id": "activation-uuid",
  "effective_from": null,
  "updated_at": "2026-10-15T05:01:00Z"
}
```

### Generic EffectEnvelope v1

```json
{
  "schema_version": 1,
  "effect_id": "stable-hash-of-execution-and-effect-index",
  "idempotency_key": "stable-effect-id",
  "tenant_id": "tenant-123",
  "execution_id": "execution-uuid",
  "effect_type": "emit_event",
  "destination": "flow.events",
  "name": "risk.review_requested",
  "partition_key": "order-42",
  "causation_id": "source-event-id",
  "hop": 1,
  "payload": {"order_id": "order-42"}
}
```

Effect IDs must be deterministic for the same input event, ruleset revision, and effect index. Subscriber deliveries use the same envelope and a destination service ID. Keep destination routing generic; receivers own their domain logic.

### ScheduleRecord v1

```json
{
  "schema_version": 1,
  "tenant_id": "tenant-123",
  "schedule_id": "schedule-42",
  "revision": 3,
  "due_at": "2026-10-15T06:00:00Z",
  "event": {},
  "status": "pending"
}
```

A key's latest record represents its current schedule state; a tombstone removes it. Due-event publication and tombstone must be in the same Kafka transaction. If the transaction aborts, neither change is visible to read-committed consumers. Use deterministic occurrence IDs so downstream idempotency is still required.

## Domain → Service → worker deployment routing

1. A tenant owns a Domain; a Domain contains one or more Services.
2. Each Service declares the immutable ruleset revisions assigned to that worker deployment. Contract-to-ruleset bindings are independent records; the router intersects active contract bindings with the target deployment’s assignments. Never rely on a global "all active rules" list.
3. A generic ingress router resolves the domain/service mapping and writes a routed copy to that service deployment's work stream. This router is an application module/role, not necessarily another microservice. If an event is explicitly bound to multiple services, it creates one deterministic routed copy per target service.
4. Each worker process is configured for exactly one deployment identity and consumes only that deployment's work stream. It validates the target service/deployment ID and resolves rulesets only from that deployment's manifest.
5. Scaling a service scales only its worker group. It does not change rule ownership for other services. Deployment revisions are rolled out with readiness, traffic switch, and drain; cache contents are rebuildable from the compacted deployment/ruleset topics.
6. The separate subscriber router handles downstream event subscriptions after successful processing. A subscriber is not automatically an execution service, and subscriber fan-out must not be used as the rule-deployment mechanism.

This introduces a routing boundary but not a mandatory extra deployable. The ingress/router loop can share the Go binary and deployment artifact with workers, while still being independently scalable as a process role if throughput requires it.

## Worker transaction algorithm

For each consumed batch:
1. Poll records and validate envelope version, tenant, contract, hop limit, and partition-key invariants.
2. Resolve activation and immutable ruleset revision from the local projection; fail closed if required state is missing or its hash/version is invalid. Do not silently fall back to an arbitrary older revision.
3. Begin Kafka transaction.
4. Verify the routed envelope targets this worker's configured service deployment. Resolve only the immutable ruleset revisions assigned to that service deployment and compatible with the event contract. Evaluate rules as a pure function. Subscriber matching is a separate generic component over the subscriber projection, not the execution-service registry.
5. Produce all output records and the consumed offsets as part of the same transaction.
6. Commit. On any processing/produce/commit error, abort and allow redelivery. Never ACK/commit offsets outside the transaction for successfully processed records.

A real implementation must use a maintained Go Kafka client with explicit transactional producer and consumer-group offset APIs. The code sketch in `worker.go` is deliberately an interface-level skeleton; it is not a drop-in implementation and does not compile against a Kafka client until an adapter is supplied.

Kafka's exactly-once guarantee stops at the Kafka transaction boundary. A receiver or ClickHouse sink can still repeat work after its own crash, so it must use stable IDs and idempotent writes.

## Scheduler algorithm

- Scheduler consumer group owns partitions of `flow.schedules`; each instance rebuilds an in-memory min-heap from the compacted state for its assigned partitions.
- The scheduler must not emit due events until it has finished replaying the assigned partition to its high-water mark.
- For a due record, begin a Kafka transaction, publish its deterministic event envelope to `flow.events`, publish a tombstone for the schedule key, and commit both atomically.
- On assignment loss or transaction error, stop processing that partition and discard its local heap entries. The next owner rebuilds from the topic.
- A heap is a cache, not the source of truth. Scheduler restart can cause late firing while replay completes; it must not cause lost events.
- Decide explicitly how recurring schedules, cancellation races, clock skew, and catch-up after downtime work. The v1 proposal supports one-shot schedules first.

## API read model without PostgreSQL

The compacted topics are durable configuration logs, not a query API. The API may maintain a disposable projection for list/get operations, rebuilt from topic replay and checkpointed offsets. For multiple API replicas, the projection must be shared or each instance must be able to rebuild a correct local copy. Do not treat a local SQLite file as shared state, and do not accept writes until the durable broker append succeeds.

If operational needs require relational joins, billing, uniqueness across multiple entities, or strong multi-record transactions, keep PostgreSQL for the control plane only. The goal is to remove PostgreSQL from the **hot path**, not to win a contest for the fewest logos in a diagram.

## Observability and operations

Required metrics: consumer lag by group/partition, transaction aborts, rebalance count/duration, processing latency, rule-cache misses, missing revisions, DLQ/retry count, scheduler lateness, output production failures, per-key skew, and broker under-replicated partitions. Correlate logs with tenant ID, event ID, execution ID, causation ID, ruleset revision, topic, partition, and offset. Never log raw sensitive payloads by default.

Set bounded retry attempts and a DLQ policy. Alert on sustained consumer lag, scheduler lateness, missing rule revisions, transaction abort storms, and partition skew. Secure topics with least-privilege ACLs; tenant IDs in keys and subjects are isolation metadata, not authorization.

## Migration plan and gates

### Phase 0: preserve the current system
- Keep NATS + PostgreSQL runtime unchanged.
- Add this proposal and schema contract tests only. No shadow publish that could trigger business effects twice.

### Phase 1: broker spike
- Choose Kafka KRaft versus Redpanda after reviewing operational requirements, compatibility, and licensing.
- Create topics, ACLs, schema compatibility checks, and a transactional producer/consumer adapter.
- Prove consume-produce-offset atomicity with a test that kills a worker before commit and after broker commit.

### Phase 2: control-plane projections
- Publish immutable rule revisions, activation records, contracts, domains, service deployment manifests, subscriber registrations, and schedule records.
- Rebuild projections from topic replay and verify hashes/counts against the current store.
- Define activation cut-over semantics and service-subscription matching before routing any real traffic.

### Phase 3: worker in isolation
- Implement pure rule evaluation plus generic subscriber routing.
- Test duplicates, crash/rebalance, poison records, missing revisions, hop limit, deterministic effect IDs, and same-key order against a real broker.
- Use an isolated test tenant and non-production destinations.

### Phase 4: scheduler and sinks
- Implement one-shot scheduling with transactionally published event + tombstone.
- Kill scheduler before and during commit; assert no lost occurrence and stable event ID.
- Add ClickHouse only if history-query requirements justify another service.

### Phase 5: controlled migration
- Replay a fixed event corpus against both implementations and compare rule decisions, effect IDs, subscriber fan-out, and revision selection.
- Do not dual-publish effects to real receivers. Use isolated output topics or a no-side-effect comparison mode.
- Canary with explicit rollback and reconciliation procedures.

### Exit criteria

- Real-broker integration tests prove transaction/offset atomicity, rebalance safety, scheduler recovery, and DLQ handling.
- Contract compatibility, per-service ruleset assignment, deployment rollout, and rule-revision activation behavior are documented and tested.
- Same-key ordering and partition-count migration behavior are tested.
- Subscriber routing retains tenant isolation and deterministic idempotency.
- No claim of end-to-end exactly-once; receivers and sinks are idempotent.
- Operational dashboards, alerts, capacity/retention estimates, and recovery runbooks exist.

## Alternatives and recommendation

- **Kafka KRaft:** broad ecosystem, mature transactions, more operational concepts to manage.
- **Redpanda:** Kafka protocol compatibility and potentially simpler operations; verify current feature parity and licensing for the intended product/distribution.
- **Keep NATS + PostgreSQL:** lowest migration risk and already implemented in this repository. Fix the known shard membership/ordering/test gaps before a wholesale rewrite.
- **Remove PostgreSQL but keep NATS:** not recommended as a shortcut. JetStream does not provide Kafka-style transactional consume-transform-produce plus consumer offset commit semantics, so the application would need to rebuild dedupe, ownership, and atomic publication behavior.

**Recommendation:** treat Kafka/Redpanda as a deliberate v2 architecture proposal, not an immediate replacement. First decide whether eliminating the PostgreSQL hot-path round trip is worth the broker migration and new activation/read-model complexity. Prove it with a narrow vertical slice before rewriting the repository.


## Application-owned log storage port and future extraction

The log is an application capability, not a vendor dependency scattered through FlowRulz. The service owns the interface and its semantics; an adapter owns the storage technology. This is the seam that allows the persistence implementation to move into a separately deployed third-party storage service later.

### Layering

```text
API / Worker / Scheduler
          |
          v
Application use cases
          |
          +---- Rule evaluator / subscriber router (domain logic)
          |
          v
       LogStore                 <- owned by FlowRulz application
          |
          +---- EmbeddedLogAdapter       (initial implementation, if selected)
          +---- KafkaLogAdapter           (future/optional)
          +---- RemoteStorageService     (future adapter over HTTP/gRPC)
```

The domain and application layers must not import storage SDKs, SQL drivers, Kafka/NATS clients, filesystem details, or remote-service generated clients. Composition at the process boundary chooses the adapter. Avoid a generic repository abstraction that hides ordering, replay, checkpoints, or transaction semantics: those are part of this log's contract.

### Required port semantics

- Append records with stable IDs and partition keys.
- Replay/read records from a position with explicit ordering and retention behavior.
- Start a transaction that appends output records and commits the consumed input position atomically, where the chosen backend supports it.
- Return typed errors for transient failure, permanent rejection, conflict, unavailable history, and ambiguous commit outcome.
- Support context cancellation, bounded request sizes, and backpressure.
- Enforce tenant authorization independently of the tenant value in a record.
- Expose health and metrics without leaking payloads.

A local file/WAL adapter is **not automatically highly available**: it is durable only according to its fsync/replication policy and can be lost with its host or volume. A remote adapter adds network partitions and ambiguous commit results. Stable idempotency keys and a status/retry protocol are mandatory for those cases.

### Extraction path

1. Keep the port in this repository and test the application against a fake implementation.
2. Implement one concrete adapter and run contract tests against it, including crash/replay, duplicate append, ordering, and ambiguous timeout cases.
3. If storage becomes a separate service, move only the adapter implementation and storage-owned schema/operations. Keep the FlowRulz port and domain behavior stable.
4. Add a remote adapter implementing the same contract; do not make domain packages call the third-party service directly.
5. Run the same contract suite against both adapters before cutover. Use explicit versioning and a migration/reconciliation plan; do not dual-write business effects.
6. Keep vendor-specific features behind optional capability interfaces only when their semantics are explicitly needed. Do not pretend all backends support identical transactions.

The `LogStore` interface in `proposals/kafka-log-first/storage.go` is a contract sketch. It does not imply that a concrete embedded WAL or remote service adapter exists yet. The worker/scheduler transaction sketches must be wired through this port in the implementation phase; they must not independently create vendor clients.
