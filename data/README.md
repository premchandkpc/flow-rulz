# Data examples

These files are small, human-readable fixtures for understanding the configuration/runtime boundary. They are examples, not production secrets or generated state.

- `examples/domain.json`: global Domain create/update request.
- `examples/domain-response.json`: server-generated Domain ID and timestamps with nested Services/ruleset revisions.
- `examples/ruleset.json`: immutable ruleset revision.
- `examples/contract.json`: event payload contract definition.
- `examples/event.json`: runtime event with event-supplied tenant identity and partition key.
- `examples/worker-config.json`: worker deployment/runtime settings.

Configuration examples intentionally omit `tenant_id` and `partition_key`. Only event data carries those values.


The domain deployment examples under [domains/](../domains/README.md) also contain a `domain.json` file per sample application. These files show how multiple rulesets are assigned to named Services. Keep runtime identity (`tenant_id` and `partition_key`) exclusively in event envelopes.
