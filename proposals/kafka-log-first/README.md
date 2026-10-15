# Kafka log-first proposal skeleton

This directory is a **reference sketch**, not the current FlowRulz runtime and not a production-ready Kafka adapter.

- `worker.go` sketches the worker transaction boundary: evaluate into generic effects, produce outputs/audit records, then commit consumed offsets in the same Kafka transaction.
- `scheduler.go` sketches one-shot schedule firing: publish the deterministic event and tombstone the compacted schedule key in the same transaction.
- `../../docs/proposals/kafka-log-first-data-plane.md` is the topic, schema, ordering, activation, migration, and operations specification.

The interfaces intentionally avoid binding the domain logic to one Go Kafka client. Before implementation, choose a maintained client and implement/test adapters for:
1. Consumer group assignment, cooperative/sticky rebalance behavior, and assignment-loss cancellation.
2. Transactional producer initialization, fencing/transactional IDs, abort/commit error classification, and sending group offsets to the transaction.
3. Read-committed consumption for transactional output topics.
4. Compacted-topic replay through a known high-water mark before a scheduler partition becomes active.
5. Permanent versus transient failures, retry ordering, DLQ writes, and operational recovery.
6. Schema validation and deterministic effect/occurrence ID generation.

## Deliberate omissions

- No actual Kafka client dependency or runnable command.
- No real timer heap or compacted-topic snapshot loader.
- No retry/DLQ implementation.
- No schema registry integration.
- No ClickHouse sink.
- No end-to-end exactly-once claim.

The worker currently aborts a whole consumed batch on a processing error. A production implementation must classify permanent failures and atomically route poison records to a DLQ while committing offsets, without allowing a poison record to block a partition forever. If strict same-key order is required, retry behavior must not allow later records for that key to overtake.

The current NATS JetStream + PostgreSQL runtime remains the active implementation until the migration plan's real-broker gates pass. Do not switch production traffic based on this proposal alone.
