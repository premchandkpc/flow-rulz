# Data examples

These files are small, human-readable fixtures for understanding the configuration/runtime boundary. They are examples, not production secrets or generated state.

- `examples/domain.json`: global Domain create/update request.
- `examples/domain-response.json`: server-generated Domain ID and timestamps with nested Services/ruleset revisions.
- `examples/ruleset.json`: immutable ruleset revision.
- `examples/contract.json`: event payload contract definition.
- `examples/event.json`: runtime event; `meta` contains `tenant_id`, `partition_key`, and extensible runtime context.
- `examples/worker-config.json`: worker deployment/runtime settings.

Domain, Service, ruleset, and contract configuration do not contain tenant identity or partition keys. Those are runtime event metadata and must live under the event's `meta` object. New runtime metadata belongs there too; unknown top-level envelope fields are not allowed.

The domain deployment examples under [domains/](../domains/README.md) show how multiple rulesets are assigned to named Services.
