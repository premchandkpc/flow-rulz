# Workers

Workers are generic execution runtimes. They understand event envelopes, contracts, immutable ruleset revisions, execution records, and generic effects. They must not contain business-specific order, payment, or receiver logic.

## Deployment identity

Each worker pool belongs to one configured Domain/Service deployment:

- `FLOW_SERVICE_ID`: service identity, currently expected as `<domain-id>/<service-id>`.
- `FLOW_DEPLOYMENT_REVISION`: positive deployment revision.
- Ruleset assignments come from the Domain's Service configuration; a worker should execute only those pinned revisions.

The deployment identity is configuration. `tenant_id` and `partition_key` are read from each event and are never worker environment settings.

**Current implementation note:** the Domain API stores Service assignments, but a complete deployment controller that continuously projects those assignments into both Go and Rust worker pools is not yet present. The current Rust runtime still reads its `RULESETS` list from environment configuration. Until that controller is implemented, deployment automation must keep worker settings aligned with the Domain revision; do not claim that workers dynamically reload the Domain API.

## Processing guarantees

1. Validate event envelope and immutable contract reference.
2. Verify the event is routed to the configured Service and deployment revision.
3. Derive the shard from the event's tenant/partition pair.
4. Deduplicate by stable event identity and ruleset identity.
5. Evaluate assigned rulesets without subscriber-specific behavior.
6. Persist execution state and effects transactionally.
7. Acknowledge only after the durable transaction succeeds.
8. Retry transient failures; quarantine permanent or exhausted failures according to the runtime policy.

## Go and Rust parity

Both implementations must agree on event JSON, partition hash input, deployment routing metadata, contract identity, idempotency, and error classification. Add tests to both workers when changing a shared envelope or routing rule.

## Storage and transport status

Current runtime: NATS JetStream + PostgreSQL. The Rust worker's supported persistence/runtime capabilities must be checked against its implementation and CI; do not assume parity for an adapter that has not been implemented. The proposal under `proposals/kafka-log-first/` is reference code, not a production Kafka worker.
