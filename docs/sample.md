# Sample data walkthrough

The fixtures under `data/examples/` show the public configuration and runtime wire formats.

```text
contract.json ──validates──> event.json
ruleset.json ──pinned by──> domain.json / services[].rulesets[]
domain.json ──selects execution configuration──> worker-config.json
event.json ──provides runtime identity and extension fields──> meta
```

- `domain.json`: a Domain containing Services, each with explicit ruleset revision assignments.
- `ruleset.json`: one immutable ruleset revision.
- `contract.json`: the event payload contract.
- `event.json`: stable top-level envelope plus `meta` and business `data`.
- `worker-config.json`: illustrative worker runtime settings.

In `event.json`, `meta.tenant_id` and `meta.partition_key` are required runtime identity values. Fields such as `traceparent`, `producer`, `region`, and future custom metadata belong in the same object. Domain configuration does not contain these values.

The samples are illustrative and must be checked against the current schemas and API handlers before production use. The Kafka proposal is not a production Kafka adapter.
