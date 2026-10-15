# ADR-006: Global Domain and ruleset configuration

- **Status:** Accepted for the configuration model; deployment reconciliation remains incomplete.
- **Date:** 2026-10-15

## Context

Domain configuration was being described with tenant-oriented names even though runtime tenant identity and partition ordering come from each event. Mixing event identity into configuration makes the ownership model unclear and encourages workers to depend on request-specific configuration scopes.

## Decision

The public configuration model is:

\`Domain → Services → Ruleset assignments (ruleset ID + immutable revision)\`.

A Domain request contains \`name\` and \`services[]\`. A Service contains its local ID/name, deployment revision, activation state, optional worker profile, and one or more ruleset revision assignments. Domain, Service, ruleset, and contract configuration do not contain \`tenant_id\` or \`partition_key\`.

Each event carries \`tenant_id\` and \`partition_key\`. Runtime processing uses this pair for stable shard routing and uses event tenant identity for event-level deduplication, execution records, and effect isolation. It does not use the event tenant to choose a separate copy of global ruleset configuration.

The existing SQL column named \`tenant_scope\` is retained as a storage compatibility detail and uses the fixed internal value \`__domain_configuration__\` for global ruleset configuration. The value is hidden from public rule revision and activation responses.

Generic subscriber registrations remain separate from execution Services nested under a Domain. Do not merge subscriber fan-out into rule evaluation.

## Consequences

- Configuration APIs are tenant-free; event envelopes remain tenant-aware.
- Rule revisions and activation records are managed in the global configuration scope.
- Service assignments pin immutable ruleset revisions and require a higher deployment revision when assignments or worker profile change.
- Worker deployment identity is Domain/Service plus deployment revision; event tenant and partition key are read from each event.
- Existing runtime tenant columns remain in event-processing tables because they represent event data, not configuration ownership.
- The Go and Rust worker deployment controller/projection is not yet complete. Deployment automation must keep worker settings aligned with Domain assignments until reconciliation is implemented.

## Validation

The implementation must have tests proving that:
- Domain configuration rejects \`tenant_id\`, \`partition_key\`, and legacy \`event_types\` fields.
- Rule-management routes use global configuration scope and do not return internal scope fields.
- Rule lookup uses the global configuration scope regardless of event tenant.
- Shard identity still varies with event tenant + partition key.
- Subscriber fan-out remains separate from rule evaluation.
- Service assignments reject missing, wrong-scope, wrong-ID, or wrong-revision ruleset references.
