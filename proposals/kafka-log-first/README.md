# FlowRulz log-first proposal skeleton

This directory is a **reference sketch**, not the current FlowRulz runtime and not a production-ready broker/storage adapter.

## Architecture boundary

FlowRulz owns the **log-storage contract**, event semantics, validation, rule evaluation, subscriber routing, and application orchestration. It must not own a particular storage engine.

- `storage.go` defines the application-owned `LogStore` / `LogTransaction` ports.
- `worker.go` sketches processing: evaluate into generic effects, append outputs/audit records, and persist the input checkpoint atomically through the storage port.
- `scheduler.go` sketches one-shot schedule firing through a transaction boundary.
- `../../docs/proposals/kafka-log-first-data-plane.md` documents the log contract, schema, ordering, migration, and operations.

The storage implementation is an adapter behind these ports. Start with one explicitly selected implementation; a later third-party persistence service can implement the same contract without changing domain/application logic. Do not let handlers or domain packages import Kafka, NATS, SQL, filesystem, or vendor SDK types.

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
