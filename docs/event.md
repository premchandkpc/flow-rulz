# Events

An event is a runtime fact, not configuration. It carries its own tenant identity and partition key.

## Required envelope fields

- `id`: stable event ID used for deduplication.
- `type`: event type, such as `orders.created`.
- `tenant_id`: runtime identity supplied with the event. It is not a Domain or Service configuration field.
- `partition_key`: business key used to preserve same-key processing order.
- `occurred_at`: event occurrence timestamp.
- `data`: payload validated against the declared contract when one is present.

Optional metadata includes `contract`, `workflow_id`, `message_kind`, `correlation_id`, `causation_id`, and `headers`.

## Partitioning and ordering

The current Go and Rust runtimes compute the virtual shard from `tenant_id + ":" + partition_key`. Therefore, events with the same pair are routed to the same virtual shard. Different tenants using the same business key remain isolated in the shard hash input.

Keep `tenant_id` and `partition_key` on the event envelope. Do not configure either one on the Domain, Service, worker deployment, or ruleset. A worker's configured identity is the Domain/Service deployment identity and revision, not a tenant.

## Example

See [event.json](../data/examples/event.json) and [the event schema](../contracts/schemas/event-envelope.schema.json).

## Processing outline

```text
Event
  -> validate envelope + contract
  -> derive tenant/partition shard key
  -> resolve applicable Service deployment and assigned rulesets
  -> deduplicate and evaluate rules
  -> persist execution, audit, and effects atomically
  -> acknowledge input after durable commit
  -> deliver effects/receptor subscriptions separately
```

The current runtime transport is NATS JetStream with PostgreSQL-backed transactional state. The Kafka/Redpanda log-first path remains a proposal until its adapters and integration tests are implemented.
