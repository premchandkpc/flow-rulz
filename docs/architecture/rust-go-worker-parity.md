# Rust/Go worker feature-parity status

This is a focused first parity increment, not a declaration of complete equivalence.

## Aligned in this PR
- Rust predicate `neq` and `not_in` return true on missing field, matching Go's evaluator.
- Rust local `first_match` rejects `otherwise` on a higher-priority rule, matching Go compiler behavior.
- Rust local rule compilation rejects duplicate priorities and rules without a nonempty `then` action list, matching Go's compiler validation.
- Rust JetStream shard consumers set `max_ack_pending = 1` for ordered redelivery per consumer, trading throughput for correctness.
- Rust uses bounded exponential retry backoff (half-to-full jitter, capped at 5 minutes) like Go.
- Rust uses application-side `EVENT_MAX_DELIVERY_ATTEMPTS` (default 24): persist quarantine before ACK when a transient failure reaches its limit. Missing attempt metadata is retried; server max_deliver stays unlimited.

## Not yet equivalent
- Contract-bound multi-ruleset discovery and ruleset-specific deduplication.
- Service assignment and pinned deployment execution ownership.
- Go's per-tenant key queues, nonblocking fair admission and hot-key alarms.
- Go's expired-shard recovery and multi-replica live reconciliation.
- Full effect/outbox ordering, direct mode, scheduler parity and deployment modes.
- End-to-end Go/Rust differential fixtures and broker/database failure injection.

CI and integration tests are required before production rollout.
