# Event contract and identity

The event envelope is the runtime boundary between producers and workers. The canonical definition is the shared EventEnvelope type, event schema and validation code. This page explains semantics but does not replace the schema.

## Envelope responsibilities
- id: stable event identity used by deduplication.
- type: event type used by the current rule-resolution model.
- occurred_at: source occurrence time, distinct from processing time.
- data: payload validated against the declared contract where configured.
- contract: contract name, version and schema hash.
- headers: transport/tracing metadata such as trace context.
- meta: runtime context containing tenant_id and partition_key; additional supported metadata belongs here rather than as new top-level fields.

See [event guide](../event.md), [contract guide](../contract.md), [event example](../../data/examples/event.json) and [event schema](../../contracts/schemas/event-envelope.schema.json).

## Identity, routing and trust
- tenant_id scopes runtime data and processing. It is not authentication; producers must be authorized to assert it.
- partition_key identifies the logical key used with tenant identity for shard selection and intended same-key serialization.
- Use the shared shard algorithm. Different hash or normalization logic creates routing divergence.
- Internal routing metadata such as target Service and deployment revision must be set or validated by trusted routing policy, not blindly accepted from producers.
- Broker subjects, hashes, event types and partition keys are not authorization boundaries.
- Do not place tenant or partition identity in Domain, Service, ruleset or contract configuration.

## Contract compatibility
Event contract name, version and schema hash must match the registered contract and any ruleset-pinned contract where validation applies. A name/version match alone does not prove identical schema semantics.

For a change, record compatible producer/consumer versions, additive versus breaking fields, missing/null/default behavior, canonical hash generation, dependent rulesets, generated SDKs, replay/backfill implications and rollback safety. Publish a new contract version for semantic changes.

## Failure cases to test
Malformed envelope; missing tenant/partition metadata; unknown top-level fields; invalid schema hash; unsupported payload type; unauthorized tenant claim; mismatched routing metadata; duplicate event ID. Assert the observable error, quarantine or retry result, not just parser failure.

## Source-of-truth order
Current type and validation code → JSON Schema and registry/migrations → contract tests and examples → this explanation. Update this page when the canonical contract changes.
