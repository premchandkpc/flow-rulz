# Contracts

Contracts version and validate event payloads independently from Domain, Service, and ruleset configuration.

## Message envelope and child contracts

The event envelope contains top-level `id`, `type`, `occurred_at`, optional `contract`, `data`, and a required extensible `meta` object. `meta.tenant_id` and `meta.partition_key` identify the runtime event scope and ordering key. Correlation, causation, tracing, routing, and future runtime attributes also belong inside `meta`. New metadata keys must be preserved without requiring a new envelope version. Unknown top-level envelope keys remain invalid.

Event, command, and effect payloads are independently versioned child contracts. A receiver declares accepted contract identities and versions; matching a subject alone is not authorization or schema acceptance.

## Routing

The runtime derives its shard from `meta.tenant_id` and `meta.partition_key`. Domain configuration contains Services and their pinned ruleset revisions only; it does not contain event metadata.

See [contract.md](contract.md), [event.md](event.md), and [the schema](../contracts/schemas/event-envelope.schema.json).
