# Workers

Workers are generic execution runtimes. They understand event envelopes, contracts, immutable ruleset revisions, execution records, and generic effects. They must not contain business-specific order, payment, or receiver logic.

## Deployment identity

The control plane can describe Domain/Service deployments:

- `FLOW_SERVICE_ID`: service identity, currently expected as `<domain-id>/<service-id>`.
- `FLOW_DEPLOYMENT_REVISION`: positive deployment revision.
- Ruleset assignments are stored in the Domain's Service configuration. Enforcing those pinned revisions in the durable worker remains future work.

The deployment identity is configuration. `meta.tenant_id` and `meta.partition_key` are read from each event. The current worker checks `headers["flow.service_id"]` and `headers["flow.deployment_revision"]` when configured with a deployment identity; it does not create those headers or route events to an isolated service stream.

**Current implementation note:** the Domain API stores Service assignments, but the worker does not intersect those assignments with matching rulesets or isolate service pools. Shards are leased globally; a worker can quarantine and acknowledge an event targeted at a different deployment. Do not use deployment headers for multi-service routing until routing and ownership are redesigned and tested. The experimental Rust runtime still reads its `RULESETS` list from environment configuration.

## Processing guarantees

1. Validate event envelope and immutable contract reference.
2. When configured, compare the event's deployment headers with the worker's configured identity. This check is not a routing mechanism.
3. Derive the shard from the event's tenant/partition pair.
4. Deduplicate by stable event identity and ruleset identity.
5. Evaluate active rulesets resolved from contract bindings or the event type, without subscriber-specific behavior.
6. Persist execution state and effects transactionally.
7. Acknowledge only after the durable transaction succeeds.
8. Retry transient failures; quarantine permanent or exhausted failures according to the runtime policy.

## Go and Rust parity

Both implementations must agree on event JSON, partition hash input, deployment routing metadata, contract identity, idempotency, and error classification. Add tests to both workers when changing a shared envelope or routing rule.

## Storage and transport status

Current runtime: NATS JetStream + PostgreSQL. The Rust worker's supported persistence/runtime capabilities must be checked against its implementation and CI; do not assume parity for an adapter that has not been implemented. The proposal under `proposals/kafka-log-first/` is reference code, not a production Kafka worker.
