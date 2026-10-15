# Events

An event is a runtime fact, not configuration. The envelope keeps stable event identity and payload at the top level. Runtime context belongs inside the extensible `meta` object.

## Envelope shape

- `id`: stable event ID used for deduplication.
- `type`: event type, such as `orders.created`.
- `occurred_at`: event occurrence timestamp.
- `contract`: optional immutable contract reference.
- `data`: business payload validated against the contract when one is present.
- `meta`: required extensible object containing `tenant_id`, `partition_key`, and optional context such as workflow, correlation, causation, headers, trace context, producer sequence, or producer-specific fields.

The schema requires `meta.tenant_id` and `meta.partition_key`, while allowing unknown metadata fields. Consumers must preserve metadata they do not understand when forwarding an event. Do not add runtime metadata fields to Domain or Service configuration.

## Partitioning and ordering

The Go and Rust runtimes compute the virtual shard from `meta.tenant_id + ":" + meta.partition_key`. Events with the same pair route to the same virtual shard; the tenant identifier keeps equal business keys from different tenants isolated.

The tenant value comes from the runtime event (and should be checked against the authenticated producer identity at ingress). It is not a Domain configuration field. A worker's configured identity is the Domain/Service deployment identity and revision, not a tenant.

## Example

See [event.json](../data/examples/event.json) and the [event envelope schema](../contracts/schemas/event-envelope.schema.json).

## Processing outline

```text
Event
  -> validate envelope + contract
  -> read tenant + partition key from meta
  -> derive virtual shard
  -> resolve Domain -> Service -> assigned rulesets
  -> deduplicate and evaluate rules
  -> persist execution, audit, and effects atomically
  -> acknowledge input after durable commit
  -> deliver effects/subscriber deliveries separately
```

The current runtime transport is NATS JetStream with PostgreSQL-backed transactional state. The Kafka/Redpanda log-first path remains a proposal until its adapters and integration tests are implemented.
