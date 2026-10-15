# Sample data walkthrough

The files under [`data/examples/`](../data/examples/) are executable reference examples for the public API and event contract.

1. [`contract.json`](../data/examples/contract.json) defines the immutable `OrderCreated` payload contract.
2. [`ruleset.json`](../data/examples/ruleset.json) defines a versioned ruleset that consumes that contract.
3. [`domain.json`](../data/examples/domain.json) is the Domain create request. A Domain contains Services, and each Service pins one or more ruleset revisions. It has no tenant or partition-key configuration.
4. [`domain-response.json`](../data/examples/domain-response.json) shows the server-generated Domain ID.
5. [`event.json`](../data/examples/event.json) is a runtime event. Its `meta` object contains `tenant_id`, `partition_key`, tracing and optional producer-specific metadata. Unknown `meta` fields are allowed and preserved.
6. [`worker-config.json`](../data/examples/worker-config.json) shows worker deployment identity and runtime connection settings. It does not configure a tenant or partition key.

## Mental model

```text
Domain
└── Service
    ├── Ruleset A @ revision N
    └── Ruleset B @ revision M

Event
├── id / type / occurred_at
├── contract
├── data                 business payload
└── meta                 tenant, partition, trace, correlation, extra context
```

Configuration answers **which rulesets belong to each Service**. Event metadata answers **which tenant and partition this runtime event belongs to**. Keep those responsibilities separate.
