# Durable event transport decoupling

The durable effect router and scheduler publish domain envelopes through `ports.EnvelopePublisher`. The NATS adapter validates, serializes, and selects the virtual shard. The composition root still owns connection lifecycle.

This reconciliation intentionally preserves `ports.EventPublisher`, `NewRouterWithConfig`, and `NewScheduler` for direct and embedded broker modes introduced after this PR was created. Those paths still select subjects and shards and can be migrated separately.

The scheduler clones persisted headers before adding an idempotency key. The change does not alter transaction boundaries, ACKs, fencing or state ownership. Delivery remains at-least-once.

CI on the reconciled tree must validate the durable event publisher, scheduled header immutability, and compatibility with the embedded and direct workers.
