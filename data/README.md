# Data examples

These files are small, human-readable fixtures for understanding the configuration/runtime boundary. They are examples, not production secrets or generated state.

- `examples/domain.json`: global Domain → Services → ruleset revisions configuration.
- `examples/ruleset.json`: immutable ruleset revision.
- `examples/contract.json`: event payload contract definition.
- `examples/event.json`: runtime event with event-supplied tenant identity and partition key.
- `examples/worker-config.json`: worker deployment/runtime settings.

Configuration examples intentionally omit `tenant_id` and `partition_key`. Only event data carries those values.
