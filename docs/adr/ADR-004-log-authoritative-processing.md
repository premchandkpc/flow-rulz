# ADR-004: Log-authoritative event processing

- Status: Proposed
- Date: 2026-10-14

## Goal

The durable append-only log is the recovery authority for accepted events and processing facts. PostgreSQL and Redis may accelerate queries, but losing either must not lose an accepted event or make an event execute twice after replay.

## Record protocol

The application-facing port is `ports.EventLog`. Records are immutable facts with deterministic IDs and tenant/partition/event identity. The log adapter assigns a monotonically increasing cursor. The initial record vocabulary is:

- `event.accepted`: accepted input event and immutable payload.
- `execution.recorded`: deterministic execution result and decision metadata.
- `effect.requested`: durable intent to emit an effect, keyed by deterministic effect ID.
- `effect.settled`: delivery outcome for an effect.
- `event.quarantined`: terminal failure plus reason and source event identity.

Append retries must be idempotent for the complete recovery horizon. JetStream's message-ID deduplication window alone is not sufficient for permanent deduplication. An adapter must reconcile a retry against retained records or maintain a rebuildable deduplication index.

## Processing protocol

1. Append the accepted event before acknowledging the producer.
2. Consume by shard and partition key. Only one active owner may advance a key; ownership changes must use monotonic fencing and must not allow a stale owner to append a conflicting terminal fact.
3. Resolve an immutable rule revision and evaluate deterministically. The event record must carry or resolve a stable revision identity, including content hash and compiler version.
4. Append execution and effect-intent facts with deterministic IDs. Replaying the same input and rule revision must produce identical IDs and payload hashes.
5. Dispatch effects from the log. Use the effect ID as the destination idempotency key. If a destination does not support idempotency, delivery is at-least-once and duplicate business actions remain possible.
6. Advance consumer progress only after the required facts are durable. A crash between append and acknowledgement causes replay; replay must find the existing logical fact rather than create a second execution/effect.
7. Build PostgreSQL query tables from log records. Projection checkpoints are hints only; deleting a projection must permit rebuilding it from the retained log.

## Ordering and recovery

- Preserve order for a given tenant + partition key. Different keys may run concurrently.
- A failed transient append or lease operation is retried without advancing the key's terminal progress.
- Permanent validation/business errors become explicit quarantine facts; they must not silently disappear at maximum delivery count.
- Effect publish followed by crash before settlement may duplicate a publish. Stable effect IDs and idempotent consumers are mandatory.
- The log retention window must exceed the recovery/rebuild window. If indefinite rebuild is required, archive records before JetStream limits can evict them.

## Rollout constraints

This ADR does not claim the runtime has migrated yet. The current worker still uses PostgreSQL transactions for inbox, execution, and outbox correctness. The port and record contract are the first compatibility seam; the next change must implement the JetStream adapter and replayable projection, then route the worker through it behind a disabled-by-default feature flag. PostgreSQL must not be demoted from correctness authority until crash-boundary integration tests pass against real JetStream and the runtime can rebuild all required state from the log.
