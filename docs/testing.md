# Testing and PR readiness

Run the repository CI workflows for Go and Rust before merging. For event-envelope changes, include the following coverage.

## Event metadata boundary

- [ ] `tenant_id` and `partition_key` are required inside `meta`, not at the top level.
- [ ] Unknown fields inside `meta` are accepted and survive decode/encode round trips.
- [ ] Unknown top-level envelope fields and legacy top-level runtime identity fields are rejected.
- [ ] Go API ingestion reads runtime identity from `meta` and preserves extension metadata.
- [ ] Go SDK emits and reads the same wire shape.
- [ ] Rust worker reads the same wire shape and preserves unknown metadata.
- [ ] Shard identity is derived from `meta.tenant_id + ":" + meta.partition_key`.
- [ ] Deployment routing metadata is read from `meta` and must be set/validated by trusted routing infrastructure.
- [ ] Contract name/version/hash are validated before rule evaluation.

## Domain configuration boundary

- [ ] Domain request contains only Domain fields and `services[]`; each Service pins immutable `ruleset_id + revision` assignments.
- [ ] Domain configuration does not accept or expose event metadata.
- [ ] Runtime metadata is not copied into worker environment configuration.
- [ ] Subscriber registrations remain separate from Domain execution Services.

## Runtime reliability

- [ ] Duplicate input events do not duplicate durable effects.
- [ ] Failed processing does not acknowledge the input or advance a durable checkpoint.
- [ ] Effects and execution state commit atomically.
- [ ] Permanent invalid events are quarantined; transient failures retry.
- [ ] Integration tests cover real PostgreSQL and NATS behavior for transaction, lease, retry, and recovery paths.
