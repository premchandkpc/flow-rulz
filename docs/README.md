# FlowRulz documentation

FlowRulz is a Go event-driven rules runtime. Start with the [root README](../README.md) for development status, event lifecycle, module commands, and known security limits. [AGENTS.md](../AGENTS.md) is the engineering guide and file-by-file package map; the [architecture map](architecture/map.md) follows runtime wiring and dependency direction.

## Documentation index

| Document | Purpose |
|---|---|
| [architecture.md](architecture/overview.md) | Current package structure, ports, adapter boundaries, process wiring and tests |
| [design.md](design.md) | Data model, transaction boundaries, consistency assumptions, state machines and failure handling |
| [operations.md](operations.md) | Local deployment, process behavior, troubleshooting and operational checklist |
| [rules.md](rules.md) | Rule JSON/YAML shape, operators, compilation, evaluation and deployment lifecycle |
| [roadmap.md](roadmap.md) | Planned work; not a list of shipped capabilities |
| [future-architecture.md](future-architecture.md) | Target-state proposal; keep separate from current behavior |
| [audit/2026-10-10-audit.md](audit/2026-10-10-audit.md) | Historical audit snapshot; revalidate items against current source |
| [audit/2026-10-11-repository-review.md](audit/2026-10-11-repository-review.md) | Current repository review, changes made and unresolved risks |

## Focused diagrams

- [01-system-design.mmd](diagrams/01-system-design.mmd): system components and layers
- [02-dependency-direction.mmd](diagrams/02-dependency-direction.mmd): allowed package dependency direction
- [03-api-surface.mmd](diagrams/03-api-surface.mmd): HTTP routing and use-case structure
- [04-rule-structure.mmd](diagrams/04-rule-structure.mmd): rule source model
- [05-rule-lifecycle.mmd](diagrams/05-rule-lifecycle.mmd): compile and activate a revision
- [06-rule-evaluation-modes.mmd](diagrams/06-rule-evaluation-modes.mmd): first-match vs all-matches behavior
- [07-evaluation-algorithm.mmd](diagrams/07-evaluation-algorithm.mmd): evaluator control flow
- [08-event-processing.mmd](diagrams/08-event-processing.mmd): consume, deduplicate, commit and acknowledge
- [09-worker-runtime.mmd](diagrams/09-worker-runtime.mmd): worker and background loops
- [10-effect-delivery.mmd](diagrams/10-effect-delivery.mmd): outbox dispatch and retry
- [11-outbox-claim-protocol.mmd](diagrams/11-outbox-claim-protocol.mmd): concurrent claim behavior
- [12-execution-lifecycle.mmd](diagrams/12-execution-lifecycle.mmd): execution states
- [13-outbox-lifecycle.mmd](diagrams/13-outbox-lifecycle.mmd): effect states and claim expiry
- [14-state-transitions.mmd](diagrams/14-state-transitions.mmd): related persistence state machines
- [15-error-classification.mmd](diagrams/15-error-classification.mmd): retryable vs permanent failures
- [16-exactly-once-recovery.mmd](diagrams/16-exactly-once-recovery.mmd): historical design of duplicate/recovery paths; do not read its title as a guarantee
- [17-sharding-ordering.mmd](diagrams/17-sharding-ordering.mmd): virtual shards and lease ownership
- [18-scaling-and-ordering.mmd](diagrams/18-scaling-and-ordering.mmd): scaling and ordering considerations
- [19-data-model.mmd](diagrams/19-data-model.mmd): persistence schema
- [20-deployment-topology.mmd](diagrams/20-deployment-topology.mmd): local/deployment topology
- [21-testing-architecture.mmd](diagrams/21-testing-architecture.mmd): test layers and coverage gaps
- [22-business-order-review.mmd](diagrams/22-business-order-review.mmd): order review example
- [23-operations-troubleshooting.mmd](diagrams/23-operations-troubleshooting.mmd): operational diagnosis
- [24-single-service-target.mmd](diagrams/24-single-service-target.mmd): proposed modular runtime target
- [25-rule-chain-routing.mmd](diagrams/25-rule-chain-routing.mmd): proposed rule-chain routing example
- [26-execution-modes.mmd](diagrams/26-execution-modes.mmd): planned processing modes
- [27-order-flow-example.mmd](diagrams/27-order-flow-example.mmd): order flow example

## Historical implementation plan

[Legacy architecture redesign plan](plans/legacy-architecture-redesign.md) is retained for context only. Its checkboxes and paths are stale and must not be treated as current implementation status.

## Current HTTP routes

The authoritative route registrations are in internal/adapters/http/handlers and internal/adapters/http/router.go. Current routes include:

| Method | Path | Behavior |
|---|---|---|
| POST | /v1/rules/{ruleSet}/revisions | Decode one JSON/YAML rule set, compile, and activate a revision |
| GET | /v1/rules/{ruleSet}/revisions | List revisions in the current tenant scope |
| GET | /v1/rules/{ruleSet}/revisions/{revision} | Read a revision |
| DELETE | /v1/rules/{ruleSet}/revisions/{revision} | Deactivate the rule set |
| GET | /v1/rules/active | List active rule pointers |
| POST | /v1/contracts | Register a contract schema |
| GET | /v1/contracts | List contract schemas |
| GET | /v1/contracts/{name}/{version} | Read a contract version |
| POST | /v1/contracts/{name}/{version}/generate | Generate code/schema artifacts |
| POST | /v1/workflows | Create workflow instance |
| GET | /v1/workflows | List workflow instances |
| GET | /v1/workflows/{workflowID} | Read a workflow instance |
| POST | /v1/workflows/{workflowID}/transition | Transition an instance |
| GET / POST | /v1/workflows/definitions | List/create definitions |
| GET | /v1/workflows/definitions/{workflowType} | Read a workflow definition |
| DELETE | /v1/workflows/definitions/{workflowType}/{version} | Delete a workflow definition |
| POST | /v1/batches | Create batch |
| GET | /v1/batches and /v1/batches/{batchID} | List/read batches |
| GET | /v1/quarantine and /v1/quarantine/{id} | List/read quarantined items |
| POST | /v1/quarantine/{id}/replay | Replay a quarantined item |
| DELETE | /v1/quarantine/{id} | Delete a quarantined item |
| GET | /health | Process liveness only |

Authentication/authorization and trusted tenant identity are not implemented. The current tenant middleware trusts caller-supplied header/query values. Do not expose the API to untrusted networks. The health endpoint does not check database or broker readiness.

Scheduled/wait instructions belong to the durable worker runtime. The control-plane HTTP API does not create or mutate runtime scheduled events.\n\nScheduled/wait instructions belong to the durable worker runtime. The control-plane HTTP API does not create or mutate runtime scheduled events.

## Configuration and development

- Environment names/defaults: [.env.example](../.env.example)
- Root build/test/check targets: [Makefile](../Makefile)
- Process and package guide: [AGENTS.md](../AGENTS.md)
- Local Compose topology: [docker-compose.yml](../docker-compose.yml)

Root module checks:

~~~sh
go build ./cmd/...
go test -race -count=1 ./...
go vet ./...
~~~

Run SDK and each example module separately because they have their own go.mod files. Tests under tests/contract and tests/integration currently use in-memory repositories/mocks; they do not constitute live PostgreSQL/NATS crash-recovery tests.

The message delivery model is at-least-once. Inbox/outbox transactions do not prove exactly-once external delivery; the receiving destination must honor the idempotency key.
