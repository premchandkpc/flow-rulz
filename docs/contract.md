# Contracts

A **contract** defines the payload shape for a named message version. Its identity is `name + version`; `schema_hash` identifies the exact semantic schema.

## Rules

- Publish a new version for a semantic schema change.
- Never change the meaning of an existing name/version.
- A ruleset may pin an input contract identity and hash.
- An event declares the contract it conforms to.
- Validate the event payload against the registered contract before rule evaluation.
- Contract configuration is global configuration and does not contain `tenant_id` or `partition_key`. Runtime event identity is provided under `event.meta`.

## Contract document

The API accepts a schema with `name`, `version`, `kind`, and a non-empty `fields` map. Supported kinds are `event`, `command`, `effect`, and `envelope`. The registry computes the canonical schema hash; callers should not invent one.

See [contract.json](../data/examples/contract.json) and the [contract JSON Schema](../contracts/schemas/contract.schema.json).

## Event reference

The event's `contract` object contains `name`, `version`, and `schema_hash`. A name/version match without the expected hash is not sufficient to establish that the payload matches the ruleset's pinned contract.

## Compatibility

Prefer additive optional fields when evolving a schema. If a field's meaning or type changes, publish a new contract version. Deprecate old versions explicitly and migrate producers and Service ruleset assignments deliberately.
