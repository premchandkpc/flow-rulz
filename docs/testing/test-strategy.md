# FlowRulz test strategy

## Layers and evidence boundaries
| Layer | Primary purpose | Does not prove by itself |
|---|---|---|
| Unit | Domain logic, validation, evaluation and retry classification | Adapter semantics, atomicity, distributed ordering |
| Service/use-case | Orchestration and error mapping | Real SQL or broker behavior when fakes are used |
| Contract/schema | JSON Schema, API/event compatibility, generated artifacts | Runtime durability |
| Adapter | SQL mapping, broker settlement, serialization | Multi-process recovery unless tested |
| Integration | Real PostgreSQL/NATS and migrations | Untested production topology or long-duration capacity |
| E2E/recovery | Full event lifecycle, redelivery, restart, lease transfer and outbox | Scenarios not included |
| Static/security | Formatting, lint, vet and code/dependency scanning | Runtime correctness |
| Build/deployment | Images, Compose, config and generated clients | Production readiness by itself |

## Critical scenarios
1. Duplicate delivery yields one durable outcome.
2. Inbox, execution and outbox intent are atomic.
3. ACK follows durable commit or durable quarantine.
4. Crash after commit but before ACK safely redelivers.
5. Quarantine persistence failure preserves a recoverable delivery.
6. Redelivery remains safe across worker restart and lease transfer.
7. Stale lease holder cannot commit after ownership changes.
8. Same tenant/partition-key order is tested across retries/handoff.
9. Effect retries reuse stable identity and reconcile ambiguous destination outcomes.
10. Tenant-scoped reads/writes and global control-plane routes enforce auth.
11. Contract name/version/hash and immutable revision selection are enforced.
12. Go/Rust parity is tested only for capabilities both implementations claim.

## Meaningful integration tests
State initial database, broker, ruleset and lease state; worker count/config; event ID/tenant/partition/contract; injected failure boundary; expected database rows, broker settlement, quarantine state and external calls; cleanup. Assert durable outcomes, not only return values. Prefer bounded polling for concrete conditions over arbitrary sleeps.

## Verification record
Record exact SHA, commands, tool versions, dependency services, exit codes, test counts, skipped tests and logs/artifacts. A green subset is not “all tests pass.” Use the [verification matrix](verification-matrix.md) to expose gaps.
