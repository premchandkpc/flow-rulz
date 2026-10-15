# Kafka log-first proposal test matrix

This directory is a client-neutral design/reference implementation. It is not a production Kafka adapter. Tests here prove the reference contracts and failure behavior; they do not prove broker-level transactions, consumer-group rebalance behavior, or exactly-once delivery to external systems.

## Covered by unit tests

| Area | Test coverage |
|---|---|
| Domain Service deployment API | Valid immutable ruleset pinning; duplicate service IDs/names; invalid identifiers and revisions; empty/oversized assignments; invalid worker profiles; missing or cross-tenant revisions; lookup errors; monotonic revision changes; explicit deactivation before removal |
| HTTP control plane | Deployment JSON decode/response; tenant context required; malformed request rejected before calling the use case |
| PostgreSQL adapter | JSONB deployment serialization; empty deployments encoded as an array; JSONB readback; malformed persisted JSON; database error wrapping |
| MongoDB adapter | BSON deployment round-trip and tenant-scoped index definitions
| Contract bindings | Multiple rulesets may independently bind to the same immutable contract; exact contract version/hash matching |
| Go worker configuration | Service ID and deployment revision must be configured together |
| Go worker routing | Wrong service/revision rejected before evaluation; effects/audits must stay within the input event's tenant and identity |
| Rule evaluator | Multiple pinned rulesets; deterministic IDs; audit metadata; resolver and evaluation errors; unsupported schema; hop limit; cross-tenant resolver results; missing compiled revision; malformed effects; subscriber router failures; subscriber routing once and separately from rule evaluation |
| Transactional worker | Outputs and input checkpoint committed together; no checkpoint advance after decode, evaluation, encoding, append, checkpoint, or commit failure; exact per-partition next offset; no unrelated partition checkpoints; no cross-tenant outputs |
| Scheduler | Event and tombstone in one transaction; deterministic occurrence ID; invalid schedule metadata rejected; encode, append, and commit failures do not advance checkpoints |
| Rust worker | Paired deployment configuration; invalid service/revision configuration; duplicate/oversized rulesets; lease/delivery config validation; matching route accepted; missing, malformed, stale, or cross-service routing rejected; wrong shard rejected; legacy routing remains compatible when identity is unset |

## CI commands

The repository CI runs formatting checks, schema validation, Go build/unit/race tests, integration and end-to-end suites, Go vet/lint, SDK/example tests, Rust formatting/tests/Clippy, generated-code compilation, console build, and Docker/Compose validation.

Local targeted commands:

```sh
make check-fmt
go test -race -count=1 ./internal/...
go test -race -count=1 ./proposals/kafka-log-first/...
cargo fmt --manifest-path rust-worker/Cargo.toml -- --check
cargo test --manifest-path rust-worker/Cargo.toml
cargo clippy --manifest-path rust-worker/Cargo.toml --all-targets -- -D warnings
```

## Required before claiming production Kafka readiness

The production Kafka adapter does not exist in this PR, so these cannot be validated by the in-memory reference store:

- Kafka transactional producer behavior and read-committed visibility after process crash.
- Consumer-group rebalance during a transaction and fencing of a stale owner.
- Ambiguous commit response followed by replay.
- Concurrent consumers racing the same partition/checkpoint.
- Broker retention, compaction, tombstone replay, and offset restoration.
- Real scheduler crash between event append, tombstone, and checkpoint.
- End-to-end tenant isolation through the actual router and broker ACLs.
- External receiver idempotency and duplicate handling.

Do not treat passing reference tests as evidence that the absent adapter provides these guarantees.
