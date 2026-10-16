# Testing and verification

This area explains what each test layer proves. A passing unit test does not prove PostgreSQL transaction semantics, NATS redelivery or distributed lease correctness unless those systems are exercised.

## Documents
- [Test strategy](test-strategy.md): layer boundaries and critical runtime paths.
- [BDD scenario template](bdd-scenario-template.md): observable acceptance scenarios.
- [Verification matrix](verification-matrix.md): requirement-to-test traceability.

For exact repository commands and affected test paths, use the [change/test checklist](../testing.md) and [AGENTS.md](../../AGENTS.md).

## Minimum evidence by change
| Change | Evidence to consider |
|---|---|
| Predicate/action semantics | Unit tests for match, non-match, boundary and errors; contract fixtures |
| API/Domain/auth | Handler/service tests and cross-tenant negative cases |
| Event/schema | Go/Rust fixtures as applicable, schema validation, SDK/generated-code checks |
| SQL/migration | Repository tests plus tagged PostgreSQL integration tests |
| ACK/retry/inbox/outbox | Real PostgreSQL + NATS tests including crash/redelivery windows |
| Lease/fencing/order | Multi-worker tests with shared durable state and lease handoff |
| Deployment/Compose | Config validation, image build, Compose validation and startup/readiness checks |
| Documentation only | Link/factual review; do not claim runtime tests were run |

## Evidence rules
Record exact command, commit SHA, environment/dependency versions, exit status and test path. Mark skipped tests Skipped. Mocks prove their own contract, not live infrastructure. Keep failures visible until diagnosed. Run nested Go modules, Rust, generated code and console checks separately where the root module does not cover them.
