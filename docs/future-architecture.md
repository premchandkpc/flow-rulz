# Target Business Event Runtime

FlowRulz is intentionally evolving as **one deployable application with clear internal modules**, rather than a collection of microservices. This is a target design: it labels proposed capabilities rather than presenting them as current production guarantees.

## Current baseline

- `EventEnvelope` has stable top-level `id`, `type`, `occurred_at`, optional `contract`, and `data`; runtime identity and extensible routing/tracing context live under `meta` (`meta.tenant_id`, `meta.partition_key`, and future fields).
- `event.type` selects the active immutable rule revision.
- Inbox deduplication, execution, and durable outbox effects commit in one SQL transaction.
- A broker fetch batch retrieves up to ten deliveries, but each delivery is evaluated, acknowledged, or retried independently.
- An effect-publisher batch claims up to ten outbox effects. It is a publication batch, not a rule-evaluation batch.
- `emit_event` creates a durable child event with an explicit literal partition key and publishes it to `events.{type}` from the outbox.
- Virtual-shard and lease types exist, but worker-side shard ownership and strict per-key sequencing are not enforced yet.

## Target event model

```text
event_id       idempotency identity
tenant_id      tenant isolation
type           selects a rule set
partition_key  entity whose events need an ordering relationship
workflow_id    optional long-running business-process identity
scheduled_at   optional future eligibility time
```

These fields are independent. An order event can retain `workflow_id = order-123` while a notification emitted from it uses `partition_key = customer-55`.

## Rule chaining: Phase 2 baseline

`emit_event` creates a complete child event envelope, including a new event ID, type, partition key, and payload. It is persisted as a durable outbox record in the same transaction as the parent execution.

The parent completes when that transaction commits, not when a child finishes. The worker then ACKs the parent. An outbox publisher makes the child visible to NATS, where it follows ordinary inbox deduplication and rule-set resolution.

The child action currently requires a literal partition key. Future routing policies may add `inherit` and `from_data`; missing or invalid derived keys must reject the action instead of silently picking a key.

## Ordering and concurrency: proposed Phase 3–4

Rule chaining and partition routing are separate decisions. Same-key children need a serial executor; retries must block later same-key work to preserve strict order. Different keys may run concurrently, possibly on different workers, and a parent does not wait for them.

Kafka can route a message key to a partition. NATS needs equivalent application-level routing: virtual shards, leases with fencing tokens, and bounded queues/executors. Until worker enforcement exists, FlowRulz provides deduplication but does not promise strict per-key order under concurrency.

## Execution modes

| Mode | Meaning | Status |
|---|---|---|
| Immediate | One event → one rule evaluation | Implemented |
| Broker fetch batch | Retrieve several deliveries, process each independently | Implemented |
| Partition-key accumulation | Collect one key until a count/window threshold, then synthesize a batch event | Proposed; domain and migration groundwork exists, scheduler is not wired |
| Scheduled batch/event | Persist work until a specific time, then release it | Proposed |
| Outbox publish batch | Claim and send several pending effects | Implemented |

## Delivery order

1. Explicit partition-key inheritance and data-path derivation.
2. Shard ownership, fencing, bounded per-key queues, and backpressure.
3. Scheduled events and partition-key accumulation.
4. Workflow instances, timers, and compensation.
