undefined
## Non-blocking worker queue admission
Durable workers now use `TrySubmit` when supported; it bounds each hot key to half the global slots and fails fast with delayed broker redelivery, rather than blocking the fetcher. Legacy `Submit` remains available for existing callers. This fixes admission starvation, **not** strict event ordering on retry.

## Strict per-shard broker ordering
NATS durable consumers now specify `MaxAckPending: 1`, so an unacknowledged or delayed-retry message blocks later deliveries within its shard consumer. This prevents overtaking at the broker input boundary, at the cost of head-of-line blocking and one in-flight delivery per shard. It does not establish ordering across different ruleset-specific consumers or independent outbox publication; those require stronger routing and state semantics.

## Bounded concurrency defaults
Durable worker defaults now start 8 independent key-queue processors; the outbox scans every 250 ms with a batch of 64 and bounded drain of up to 16 batches. These are starting configurations only. Tune against measured database latency, NATS consumer overhead and downstream effect throughput.
