# Roadmap

## Implementation status

### Phase 0 — Contracts and proof (DONE)

- [x] Event envelope and rule JSON schema
- [x] Compiler with validation limits
- [x] Pure evaluator with first_match and all_matches
- [x] Deterministic effect IDs
- [x] Decision hash

### Phase 1 — Single-process vertical slice (DONE)

- [x] PostgreSQL schema (7 tables, migrations)
- [x] Transactional inbox/outbox in one Postgres transaction
- [x] Broker ACK only after commit
- [x] NATS JetStream consumer and publisher
- [x] Worker fetch/evaluate/ack loop
- [x] API for rule deployment
- [x] Outbox claim with FOR UPDATE SKIP LOCKED
- [x] HTTP destination adapter with Idempotency-Key support; destination-side durable deduplication remains a deployment contract
- [x] Graceful shutdown with in-flight drain
- [x] JetStream delivery limit set to unlimited (`NATS_MAX_DELIVER=-1`) until exhausted-delivery recovery is implemented

### Phase 1.5 — Architecture and correctness (DONE)

- [x] Service layer split (rules, events, effects)
- [x] Port interfaces (EventProcessor, EffectPublisher, RuleCompiler, RuleEvaluator)
- [x] Domain state machines (Execution, OutboxEffect, InboxEntry)
- [x] Contract-aware rule revisions
- [x] Rule explanations (explainability)
- [x] Rule metadata (name, description, tags)
- [x] SystemClock in domain package
- [x] Adapter-local Querier, pure ports.Tx
- [x] Contract test suites wired
- [x] Dead code cleanup

### Phase 2 — Production-shaped distribution (IN PROGRESS)

- [x] Shard ownership via ShardLease with fencing-token validation in the event-processing transaction
- [x] Worker subject routing by virtual shard
- [ ] Bounded concurrency per shard
- [ ] Metrics and structured logging
- [ ] Replay API
- [ ] Rule-list/read endpoint
- [ ] Authentication and authorization

### Phase 3 — Operational hardening (TODO)

- [ ] Load, failure, and security test suites
- [ ] Retention policies and payload archival
- [ ] Hot-key detection and mitigation
- [ ] Canary revision activation with rollback
- [ ] Monitoring dashboards and alerts

### Phase 4 — Kafka adapter (TODO)

- [ ] Kafka consumer adapter
- [ ] Kafka publisher adapter
- [ ] Transport abstraction validation

## Known gaps

| Gap | Status | Fix order |
|-----|--------|-----------|
| Lease expiry/contention and stale-worker behavior need live-infrastructure failure-injection coverage | Open | 1 |
| Dynamic worker membership and lease rebalancing are not implemented; assignment currently assumes three worker replicas | Open | 2 |
| Destination-side idempotency and duplicate-send behavior need production-like receiver tests | Open | 3 |
| Verify every quarantine path is durable and quarantine-write failures remain safely retryable | Open | 3 |
| Durable exhausted-delivery recovery / DLQ workflow is not implemented; finite JetStream MaxDeliver is therefore rejected and unlimited delivery is required | Open | 4 |
| Worker defaults to one key-queue worker; tune bounded concurrency and test per-key ordering under redelivery | Open | 5 |
| No authentication/authorization | Open | 6 |
| No replay API endpoint | Open | 7 |
| No metrics/observability | Open | 8 |
| No rule list/read endpoints | Open | 9 |
| No retention/archival for executions/effects | Open | 10 |

## Target success criteria

1. Duplicate effect rate = 0 (idempotent delivery verified)
2. Target: per-key ordering across worker restarts and redelivery, verified under concurrent workers; no global ordering guarantee
3. Decision p99 < 10ms for 1KB events
4. Backlog recovery within 60s after worker restart
5. Replay produces identical decision and effect content; stable execution identity across replay is not yet guaranteed
6. Operational surface: start, stop, deploy, rollback, quarantine inspect

## What we will not build

- Raft/gossip consensus (Postgres and NATS handle this)
- Arbitrary loops, timers, or compensation in rules
- Global event ordering
- Cross-database 2PC
- Sync request/reply as a rule primitive

## Correctness contracts and open verification

- **Broker/inbox boundary:** event processing commits the inbox, execution, and outbox in one database transaction; the broker ACK follows commit. A crash after commit but before ACK is expected to cause redelivery, which the retained inbox record must deduplicate.
- **Outbox/destination boundary:** publishing and marking an effect delivered are separate operations. A crash after send can cause a repeated request. Idempotency-Key must be honored durably by the destination for effectively-once external behavior.
- **Fencing:** the event service validates shard ownership and the fencing token in its transaction before mutating inbox/execution/outbox state. This needs integration/failure-injection coverage for lease expiry, contention, and stale workers.
- **Replay identity:** execution IDs are deterministically derived from tenant, event ID, rule set, and selected revision. The replay API is not implemented; define whether replay uses the original or a selected revision and add end-to-end replay tests before marking the criterion complete.
- **Poison events:** permanent event errors are quarantined before ACK. If quarantine persistence fails, the worker retries the delivery. Until a durable exhausted-delivery recovery workflow is implemented and tested, configuration requires JetStream `MaxDeliver=-1`; finite delivery limits could strand an unacknowledged work-queue message.
- **Ordering:** key-queue serialization is local to a worker process. Broker redelivery, lease transitions, and multiple publishers can affect ordering. FOR UPDATE SKIP LOCKED improves concurrent claiming but does not itself guarantee per-key outbox order.
- **Retention:** inbox retention defines the deduplication horizon. Retention must exceed broker redelivery and supported replay horizons. Producers must use stable event IDs; JetStream publish deduplication is not a substitute for durable inbox deduplication.
- **Delivery throughput:** the default publisher attempts one batch of 10 every five seconds. This is a nominal ceiling of about 2 effects/second per publisher before considering processing time. Measure before tuning; any drain loop must remain bounded and cancellation-aware.
- **Security:** the API is not safe to expose to untrusted networks until authentication, authorization, and a trusted tenant identity resolver are implemented.
