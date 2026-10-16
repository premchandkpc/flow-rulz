# FlowRulz domain model

This is the conceptual model. Exact fields and constraints are defined by source, SQL migrations, JSON Schemas and [implementation guides](../README.md).

## Main concepts
| Concept | Responsibility | Lifecycle / boundary |
|---|---|---|
| Domain | Root of global execution configuration | Contains Services; no per-event tenant or partition identity |
| Service | Execution/deployment boundary | Assigns explicit ruleset revisions and has a deployment revision |
| Ruleset | Named collection of rules and mode | Changes produce immutable revisions |
| Ruleset revision | Stable snapshot of evaluation logic | Referenced explicitly by deployment assignment |
| Contract | Named payload shape with version and canonical schema hash | Event/ruleset binding must agree where validation applies |
| EventEnvelope | Runtime event identity, payload and metadata | meta.tenant_id and meta.partition_key are required context |
| Inbox | Durable record for duplicate detection | Part of the idempotency protocol |
| Execution | Durable processing decision | Does not prove external effect completion |
| Outbox effect | Durable intent for asynchronous delivery | Can retry; destination dedupe may be required |
| Subscription | Generic receiver fan-out | Separate from execution assignment |
| Shard lease/fencing | Temporary worker ownership | Stale ownership rejected at durable write boundary |

## Relationships and flow
1. A Domain contains Services.
2. A Service pins ruleset ID plus immutable revision.
3. An event carries stable ID, type, timestamp, data, contract, headers and metadata.
4. Routing uses tenant plus partition key; tenant value is not authentication.
5. The worker validates event/contract and resolves applicable pinned rulesets.
6. Processing persists duplicate-detection and execution state plus outbox intent under the durable protocol.
7. Input settlement follows durable processing or durable quarantine.
8. Outbox effects and subscriber deliveries are separate paths and may fail independently.

## Important distinctions
- Persisting a Domain assignment does not prove all worker deployments have reconciled; automatic reconciliation is currently documented as incomplete.
- A committed execution is not proof an external API or subscriber succeeded.
- Hashes, subjects and target-service values route work but do not authorize callers.
- At-least-once delivery requires dedupe and idempotent effects; do not claim exactly-once side effects.
- This conceptual page is not a database schema or API contract.
