# Events

An event separates its stable envelope from runtime metadata and business payload.

- Top-level `id`, `type`, `occurred_at`, `contract`, and `data` define the event identity, kind, timestamp, payload contract, and business payload.
- `meta` contains runtime identity and extensible context. It is intentionally open to unknown fields so new tracing, routing, producer, and execution metadata does not require a breaking envelope change.
- `meta.tenant_id` and `meta.partition_key` are required runtime values. They come with each event and are never configured on a Domain, Service, ruleset, or worker.
- `meta.flow.service_id` and `meta.flow.deployment_revision` are reserved internal routing metadata. Public ingress rejects producer-supplied values; trusted routing infrastructure alone sets them after resolving the Domain → Service assignment.
- Unknown keys inside `meta` must be preserved when the event is decoded and re-encoded. Unknown top-level envelope keys remain rejected to keep the envelope stable.

## Partitioning and ordering

The current Go and Rust runtimes compute the virtual shard from `meta.tenant_id + ":" + meta.partition_key`. Events with the same pair are routed to the same virtual shard. The tenant value is runtime event identity, not configuration identity.

## Example

See [event.json](../data/examples/event.json) and [the event schema](../contracts/schemas/event-envelope.schema.json).

## Processing outline

```text
Event
  -> validate stable envelope and required meta fields
  -> validate the contract against data
  -> derive shard from meta.tenant_id + meta.partition_key
  -> resolve Domain -> Service -> assigned rulesets
  -> deduplicate and evaluate rules
  -> persist execution, audit, and effects atomically
  -> acknowledge input after durable commit
  -> deliver effects and subscriber deliveries separately
```

The current runtime transport is NATS JetStream with PostgreSQL-backed transactional state. The Kafka/Redpanda log-first path remains a proposal until its adapters and integration tests are implemented.
