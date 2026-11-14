# Worker review remediation

This narrowly scoped PR builds on `refactor/fifteen-line-functions` and does not bring its broad changes into main independently.

## Fixed
- Scope queue serialization by both tenant and partition key; avoid noisy-neighbour contention.
- Set a reachable hot-key threshold default (50), bounded by per-key queue capacity.
- Regression tests cover unambiguous queue identities and hot-key threshold bounds.

## Follow-up work, not claimed fixed
- Live lease membership and takeover after worker death.
- Strict ordering across retry and outbox publication.
- Exhausted-delivery quarantine and shutdown settlement.
- Throughput benchmarking, broker consumer optimizations and outbox drain policy.

The review's proposed non-blocking enqueue example is not copied because enqueueing before reserving the global slot introduces accounting races.
