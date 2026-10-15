# Events

An event is a runtime fact, not configuration. The event envelope separates stable envelope fields from extensible runtime metadata.

## Envelope fields

- `id`: stable event ID used for deduplication.
- `type`: event type, such as `orders.created`.
- `occurred_at`: event occurrence timestamp.
- `data`: payload validated against the declared contract when one is present.
- `contract`: immutable contract name, version, and schema hash.
- `headers`: transport/tracing headers such as `traceparent`.
- `meta`: extensible runtime metadata. It must contain `tenant_id` and `partition_key`; additional metadata is allowed.

## Runtime metadata

Example:

```json
{
  "meta": {
    "tenant_id": "merchant-42",
    "partition_key": "order-1007",
    "source_system": "checkout-api",
    "region": "ap-south-1"
  }
}
```

`tenant_id` identifies the runtime tenant and `partition_key` is the business key used to preserve same-key processing order. Future runtime context belongs inside `meta`; the envelope schema intentionally permits additional keys there. Do not add those runtime fields to Domain, Service, ruleset, or contract configuration. Unknown top-level envelope fields remain invalid, so extensibility stays scoped to `meta` and the event payload `data`.

## Partitioning and ordering

The current Go and Rust runtimes compute the virtual shard from `meta.tenant_id + ":" + meta.partition_key` on the wire. Internally, the Go and Rust execution models may expose these values as typed fields for efficient routing. Events with the same pair are routed to the same virtual shard; identical business keys from different tenants remain isolated.

## Example and schema

See [event.json](../data/examples/event.json) and [the event schema](../contracts/schemas/event-envelope.schema.json).

## Processing outline

```text
Event
  -> validate envelope + contract
  -> derive tenant/partition shard key from meta
  -> resolve Domain/Service deployment and assigned rulesets
  -> deduplicate and evaluate rules
  -> persist execution, audit, and effects atomically
  -> acknowledge input after durable commit
  -> deliver effects/subscriber notifications separately
```

The current runtime transport is NATS JetStream with PostgreSQL-backed transactional state. The Kafka/Redpanda log-first path remains a proposal until its adapters and integration tests are implemented.
