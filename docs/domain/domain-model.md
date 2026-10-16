# FlowRulz domain model

This is the conceptual model. Field-level API truth belongs in linked schemas and implementation guides.

| Concept | Responsibility | Important boundary |
|---|---|---|
| Domain | Root of global execution configuration | Does not carry per-event tenant or partition identity |
| Service | Deployment/execution boundary within a Domain | Pins explicit ruleset revisions; not a receiver subscription |
| Ruleset / revision | Ordered rule definitions and immutable version | Publish a new revision for changes |
| Contract | Payload schema identity and version/hash | Compatibility is validated at the relevant boundary |
| Event envelope | Runtime message plus event metadata | Carries tenant and partition key |
| Inbox | Durable duplicate-detection record | Participates in the required transaction |
| Execution | Durable record of processing outcome | Tied to event and selected revision |
| Outbox effect | Durable intent to publish or perform an effect | Delivery is at-least-once |
| Subscription | Generic subscriber delivery configuration | Separate from evaluation and Service deployment |
| Shard lease / fencing token | Temporary ownership of a shard | Stale holders must be rejected at the durable write boundary |

## Key relationships
- A Domain contains Services.
- A Service assigns one or more explicit ruleset revisions.
- An event has tenant identity and a partition key used for routing, deduplication, and ordering scope.
- A processing transaction coordinates inbox, execution and outbox state where the selected backend supports the required semantics.
- Outbox dispatch happens after the durable intent is committed and may retry.
- Runtime caches are rebuildable accelerators, not authoritative state.

This is not a database schema or API contract. Use migrations, JSON schemas, and endpoint tests to establish exact field names, constraints and deployed behavior.
