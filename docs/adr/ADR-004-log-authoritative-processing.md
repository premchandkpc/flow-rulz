# ADR-004: Log-authoritative event processing

- Status: Proposed
- Date: 2026-10-14

## Goal

The durable append-only log is the recovery authority for accepted events and processing facts. PostgreSQL and Redis may accelerate queries, but losing either must not lose an accepted event or make an event execute twice after replay.

## Record protocol

The application-facing port is `ports.EventLog`. Records are immutable facts with deterministic IDs and tenant/partition/event identity. The log adapter assigns a monotonically increasing cursor.

- `event.accepted`: the accepted input event, immutable payload, input contract reference and source identity.
- `execution.committed`: the deterministic outcome, pinned rule revisions, decision hash, result, fencing epoch and the **complete set of effect intents** produced by that execution. This is the atomic correctness boundary. Persist it as one durable record before acknowledging processing progress.
- `effect.requested`: an optional normalized/materialized view of an effect intent from `execution.committed`. It must be deterministically reconstructable and is not the sole evidence that an effect exists.
- `effect.settled`: delivery outcome for an effect, keyed by deterministic effect ID and settlement identity.
- `event.quarantined`: terminal failure plus reason and source event identity.

The reason effects are embedded in `execution.committed` is important: a multi-record append is not automatically atomic, and JetStream does not provide a general transaction that atomically appends an execution result and several effect messages. If those were separate correctness facts, a crash between them could persist a result while losing an effect. Derive or materialize individual effect records only after the complete commit exists, and make that derivation idempotent.

Append retries must be idempotent for the complete recovery horizon. A repeated record ID with the same immutable content returns the original position; the same ID with different content is an identity conflict. JetStream's message-ID deduplication window alone is not sufficient for permanent deduplication. An adapter must reconcile retries against retained records or maintain a rebuildable deduplication index.

## Processing protocol

1. Validate the envelope and immutable input contract, then append `event.accepted` before acknowledging the producer.
2. Consume in partition order, preserving tenant + partition-key order. Only one active owner may advance a key; ownership changes use monotonic fencing.
3. Resolve immutable rule revisions and compiler versions, then evaluate deterministically without external I/O in the decision step.
4. Append one `execution.committed` fact containing the result and every effect intent. Replaying the same input and pinned revisions must yield the same logical execution/effect IDs and payload hashes.
5. Derive effect dispatch work from committed facts. Use each effect ID as the destination idempotency key. If a destination does not support idempotency, delivery is at-least-once and duplicate business actions remain possible.
6. Record effect settlement separately. A crash after publishing but before settlement may lead to redelivery; stable IDs and idempotent consumers are required.
7. Advance consumer progress only after the required facts are durable. A crash between commit and acknowledgement causes replay, which must find the existing logical commit rather than create a second execution.
8. Build PostgreSQL query tables and dispatch indexes from log records. Projection checkpoints are hints only; deleting a projection must permit rebuilding it from the retained log.

## Ordering and recovery

- Preserve order for a given tenant + partition key. Different keys may run concurrently.
- A failed transient append or lease operation is retried without advancing the key's terminal progress.
- Permanent validation/business errors become explicit quarantine facts; they must not silently disappear at maximum delivery count.
- Keep retention longer than the recovery/rebuild window. If indefinite rebuild is required, archive records before transport limits can evict them.
- The authoritative append must reject stale fencing epochs. A local append-only file is not a replicated log, and a transport's ordering guarantees are only as strong as its documented scope.

## Rollout constraints

This ADR does not claim the runtime has migrated yet. The current worker still uses PostgreSQL transactions for inbox, execution and outbox correctness. The domain/port contract is only a compatibility seam. Next, implement a durable log adapter and deterministic projection/dispatch rebuild, then route the worker through it behind a disabled-by-default feature flag. PostgreSQL must not be demoted from correctness authority until crash-boundary integration tests pass against the real adapter and the runtime can rebuild all required state from the log.
