# End-to-end sample

Use the JSON files in [data/examples](../data/examples/) in this order:

1. **[contract.json](../data/examples/contract.json)** defines the immutable `OrderCreated` event payload contract.
2. **[ruleset.json](../data/examples/ruleset.json)** defines a versioned ruleset that consumes that contract.
3. **[domain.json](../data/examples/domain.json)** is the Domain create request assigning multiple rulesets to Services. **[domain-response.json](../data/examples/domain-response.json)** shows the server-generated Domain ID.
4. **[event.json](../data/examples/event.json)** is a runtime event. `meta.tenant_id`, `meta.partition_key`, and future runtime context live under `meta`; they do not appear in Domain or ruleset configuration. Unknown metadata keys and nested objects are preserved; top-level identity fields are rejected.
5. **[worker-config.json](../data/examples/worker-config.json)** shows worker deployment/runtime settings. It does not configure a tenant or partition key.

## Mental model

```text
contract.json ──validates──> event.json.data
ruleset.json ──pinned by──> domain.json / services[].rulesets[]
domain.json ──selects deployment──> worker-config.json
event.json.meta ──provides runtime identity + extensible context──> worker
```

The worker configuration's `domain_id` is illustrative; in a live deployment, use the `id` returned by `POST /v1/domains`, not the Domain name. Validate examples against the schemas before using them in a live environment. The Kafka proposal is not a production Kafka adapter.
