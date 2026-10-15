# Sample data

These fixtures explain the core FlowRulz concepts and are intended to be read alongside the documentation.

- `examples/domain.json`: Domain containing Services and their pinned ruleset revisions.
- `examples/domain-response.json`: server-generated Domain ID and timestamps.
- `examples/ruleset.json`: immutable ruleset revision.
- `examples/ruleset-order-notification.json`, `ruleset-payment-notification.json`, and `ruleset-payment-risk.json`: additional ruleset examples.
- `examples/contract.json` and `contract-payment-authorized.json`: versioned payload contracts.
- `examples/event.json`: event envelope with extensible `meta`, including `tenant_id` and `partition_key`, plus the business payload in `data`.
- `examples/worker-config.json`: illustrative worker settings.

Domain configuration contains only Domain and Service/ruleset assignments. Runtime identity, partitioning, tracing, and producer context belong to each event's `meta` object. Unknown `meta` fields are allowed and must be preserved; unknown top-level envelope fields are not.

See [the documentation index](../docs/README.md), [event schema](../contracts/schemas/event-envelope.schema.json), and [Domain schema](../contracts/schemas/domain.schema.json).
