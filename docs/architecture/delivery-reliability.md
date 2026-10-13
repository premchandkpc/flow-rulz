# Delivery Reliability Contract

## Current guarantee

FlowRulz uses PostgreSQL for durable business state and NATS JetStream for transport delivery. The event service commits inbox, execution, and outbox state in one transaction. The worker ACKs only after processing commits or after permanent failure is durably quarantined.

JetStream `MaxDeliver` must be `-1` (unlimited) until an exhausted-delivery recovery workflow exists. Finite delivery limits are rejected by both configuration validation and the NATS adapter. This avoids silently stranding an unacknowledged work-queue message after the broker stops redelivering it. Unlimited broker delivery is a safety measure, not a dead-letter queue.

## Settlement rules

| Outcome | Action |
|---|---|
| Processing transaction committed | ACK |
| Duplicate with committed inbox/execution | ACK after the committed result is verified |
| Transient processing error | Delayed NAK; use bounded exponential backoff with jitter |
| Permanent event error | Persist quarantine, then ACK |
| Quarantine persistence fails | Delayed NAK; never ACK the only recoverable copy |
| External effect fails | Retry the outbox effect independently; do not re-run the event decision |
| External call has an ambiguous outcome | Retry with the same durable idempotency key or reconcile with the destination |

A broker ACK means the application has durably accepted responsibility for the event. It does not mean an external effect has completed.

## Retry behavior

The worker uses JetStream delivery metadata when available to select an exponential delay, capped at five minutes with equal jitter. If an adapter cannot report the attempt count, the worker safely falls back to the configured base delay.

Permanent failures should be classified and quarantined. Infrastructure and unknown failures remain retryable. Because the current event retry path relies on durable JetStream redelivery, finite `MaxDeliver` values are prohibited until the exhausted-delivery handoff is durable and restart-safe.

The scheduled-event subsystem has its own persisted retry count, next retry time, and terminal failed state. Scheduled-event retries and broker-delivery retries are different workflows and must not be conflated.

## Remaining production work

- Implement a durable exhausted-delivery recovery path before allowing finite broker delivery limits.
- Add live NATS + PostgreSQL tests for crash-before-commit, crash-after-commit-before-ACK, quarantine-store outage, worker restart, and redelivery after lease transfer.
- Verify per-key ordering when an earlier event is retrying.
- Add operator-visible alerts for retry age, repeated failures, terminal scheduled events, and quarantine persistence failures.
- Test destination-side idempotency and ambiguous external outcomes.

Do not claim production readiness based on unit tests alone. State the exact commit and checks that were run.

## Batch completion

Batch processing uses the same transport-neutral application contracts as ordinary
events. The application worker depends on `ports.ShardedConsumer`; NATS and JetStream
client types remain inside the NATS adapter.

A completed batch run and its inbox member assignments are finalized in one repository
transaction. Replaying the same deterministic batch ID is idempotent. A member already
assigned to a different batch, a missing member, a non-committed member, or a mismatched
member count rejects the entire finalization rather than partially assigning rows.

This transaction protects batch metadata and membership. It does not combine the
separate event-processing transaction with batch finalization. If processing commits
but finalization fails, the deterministic synthetic batch event must be replayed so
the event processor can return its idempotent result before finalization is retried.
Live PostgreSQL and MongoDB transaction tests, plus crash-boundary tests, remain
necessary before claiming end-to-end crash recovery.
