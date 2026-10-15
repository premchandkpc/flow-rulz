# Example data

These JSON files show the public configuration and runtime event shapes:

- `examples/domain.json`: Domain with Services and explicit immutable ruleset revisions.
- `examples/domain-response.json`: response with server-generated Domain identity.
- `examples/ruleset.json`: ruleset configuration and payload contract binding.
- `examples/ruleset-*.json`: additional rule-set examples.
- `examples/contract.json` and `examples/contract-payment-authorized.json`: versioned payload contracts.
- `examples/event.json`: event envelope. Runtime context belongs in the extensible `meta` object; `meta.tenant_id` and `meta.partition_key` are required, and unknown metadata keys are allowed.
- `examples/worker-config.json`: deployment-specific runtime configuration. Tenant and partition keys are never configured on a worker.

Validate examples against schemas under `contracts/schemas/`. Keep examples synchronized with the API and event-envelope docs when schemas change.
