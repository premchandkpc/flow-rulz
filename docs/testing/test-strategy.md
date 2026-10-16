# Test strategy

## Layers and evidence boundaries
| Layer | Best for | Does not prove by itself |
|---|---|---|
| Unit | Pure domain logic, validation, rule evaluation, retry classification | Adapter semantics, distributed ordering, crash recovery |
| Service/use-case | Orchestration and error mapping with fakes | Real database atomicity or broker behavior |
| Contract | JSON schemas, API/event compatibility, generated code | Runtime durability |
| Adapter | SQL mapping, broker acknowledgement, serialization | Multi-process failure behavior unless exercised |
| Integration | Real PostgreSQL/NATS and migrations | Production topology or untested long-duration behavior |
| E2E/chaos | Full event lifecycle, redelivery, restart, lease handoff, outbox recovery | Failure modes not included in the test |
| Static/security | Formatting, lint, vet, dependency and code scanning | Runtime correctness |

## FlowRulz critical paths
Prioritize evidence for:
- inbox deduplication and atomic execution/outbox persistence;
- ACK only after a committed durable result;
- crash after commit but before ACK;
- duplicate and maximum-delivery behavior;
- stale lease holder rejection and fencing;
- ordering for a tenant/partition key across retries and worker handoff;
- stable effect IDs and duplicate-safe downstream delivery;
- tenant-scoped reads/writes and global control-plane authorization;
- contract/version compatibility and immutable ruleset revisions;
- Go/Rust parity only where the same behavior is explicitly claimed.

## Test design
For each scenario, define initial durable state, message/worker setup, injected failure, expected broker state, database state, externally observable effects, and cleanup. Assert persisted outcomes, not only return values.

## Verification record
Include exact commit SHA, command, dependency versions, exit code, targeted test name/count, and skipped tests. Never report all tests pass when only a subset was run.
