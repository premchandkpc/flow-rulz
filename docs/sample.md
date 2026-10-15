# End-to-end sample

Use the JSON files in [data/examples](../data/examples/) in this order:

1. **[contract.json](../data/examples/contract.json)** defines the immutable `OrderCreated` event payload contract.
2. **[ruleset.json](../data/examples/ruleset.json)** defines a versioned ruleset that consumes that contract.
3. **[domain.json](../data/examples/domain.json)** assigns multiple rulesets to a Service inside a Domain.
4. **[event.json](../data/examples/event.json)** is a runtime event. It carries `tenant_id` and `partition_key`; neither appears in Domain or ruleset configuration.
5. **[worker-config.json](../data/examples/worker-config.json)** shows deployment identity, mirrored ruleset assignments, and runtime connection settings. It does not configure a tenant or partition key.

## Mental model

```text
contract.json ──validates──> event.json
ruleset.json ──pinned by──> domain.json / services[].rulesets[]
domain.json ──selects deployment──> worker-config.json
event.json ──provides runtime identity──> tenant_id + partition_key
```

The worker configuration's `domain_id` is illustrative; in a live deployment, use the `id` returned by `POST /v1/domains`, not the Domain name. The files are illustrative and should be validated against the schemas and API handlers before being used in a live environment. The examples do not imply that the Kafka proposal is implemented.
