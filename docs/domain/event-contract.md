# Event contract and identity

## Event identity
Runtime events must follow the canonical envelope definition in source and schema. Documented identity fields include meta.tenant_id and meta.partition_key. A tenant value must be asserted only by an authorized producer; a field in an event is not authentication.

- tenant_id scopes processing and tenant-owned data.
- partition_key identifies the logical key whose events need stable routing/serialization.
- Contract name, version and schema hash identify expected payload shape where contract validation is configured.
- Event type and explicit routing configuration must not be treated as interchangeable without checking the current routing implementation.

## Compatibility checklist
For a contract change, document:
1. producer and consumer versions that may coexist;
2. additive vs breaking field changes;
3. required/missing/null behavior;
4. schema version/hash generation and validation;
5. replay/backfill implications for retained JetStream messages;
6. rule revisions that depend on the old contract;
7. migration and rollback behavior.

## Trust and privacy
Authenticate producers independently from envelope validation. Validate tenant authorization before accepting the event. Do not log credentials or unrestricted sensitive payloads. Routing subjects and hashes are not authorization boundaries.

The current EventEnvelope type, registered contract schema, generated artifacts, validation code and contract tests take precedence over this summary. Update this document when those sources change.
