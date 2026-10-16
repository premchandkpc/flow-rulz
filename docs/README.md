# FlowRulz engineering documentation

Canonical entry point for documentation about the event-driven rules runtime. Root-level guides remain implementation references; this structure adds clear ownership without duplicating field-level contracts.

## System at a glance
- **Control plane:** Go HTTP API manages Domains, Service assignments, ruleset revisions, contracts, workflow definitions and operational views.
- **Transport:** NATS JetStream currently carries business events. Producers publish EventEnvelope messages to shard subjects.
- **Durable state:** PostgreSQL stores processing state, inbox/deduplication, executions and outbox intent.
- **Execution:** Go workers own shard leases, validate routing context, resolve pinned rulesets, evaluate events and commit durable outcomes before ACK.
- **Effects:** outbox delivery is separate from the event decision and can be retried.
- **Other runtimes:** verify Rust capability parity against its implementation and tests. Proposal code is not a shipped adapter.

## Where documents belong
| Folder | Owns | Use it for |
|---|---|---|
| [specs/](specs/README.md) | Required behavior and acceptance criteria | Defining behavior before implementation |
| [plans/](plans/README.md) | Work sequence, dependencies and exit criteria | Coordinating multi-package or risky changes |
| [domain/](domain/README.md) | Concepts, identities and contracts | Changing event shape or ownership boundaries |
| [rules/](rules/README.md) | Rule structure, semantics and lifecycle | Changing predicates, actions or activation |
| [testing/](testing/README.md) | Test layers and evidence | Proving or assessing guarantees |
| [reports/](reports/README.md) | Dated findings and risks | Audits, investigations and readiness reviews |
| [architecture/](architecture/overview.md) | Runtime boundaries and reliability | Changing broker, storage or process ownership |
| [diagrams/](diagrams/01-system-design.mmd) | Editable Mermaid sources | Updating system-flow diagrams |
| [adr/](adr/) | Accepted decisions and consequences | Recording durable architecture choices |

## Current implementation guides
- API and configuration: [API](api.md), [Domain](domain.md)
- Event and schema: [Events](event.md), [Contracts](contract.md), [Contracts overview](contracts.md)
- Rules and assignment: [Rules](rules.md), [Rulesets](ruleset.md)
- Runtime: [Workers](worker.md), [Delivery reliability](architecture/delivery-reliability.md)
- Operations: [Operations](operations.md), [Compose deployment](deployment-compose.md)
- Verification: [Testing checklist](testing.md), [AGENTS.md](../AGENTS.md)
- Direction/history: [Roadmap](roadmap.md), [Future architecture](future-architecture.md), [Implementation plan](IMPLEMENTATION_PLAN.md), [Audits](audit/), [Security findings](security/)

## Documentation contract
1. Current code, schemas, migrations, configuration, tests and CI define behavior.
2. Specs define required behavior; plans define intended work; reports record evidence at a specific commit.
3. NATS delivery is at-least-once. ACK means durable responsibility, not completion of an external side effect.
4. Every substantive acceptance criterion links to a test or an explicit gap.
5. Preserve dated audits as historical snapshots; add follow-up reports instead of rewriting history.
6. Record commit SHA, exact commands, environment, exit status and skipped tests.
7. Keep canonical field definitions in schemas and implementation guides; link to them instead of creating competing copies.

## Change workflow
1. Read AGENTS.md, affected code, schemas, migrations, tests and CI.
2. Write/update the spec, including failure and compatibility behavior.
3. Create a plan for cross-package, persistence, concurrency, security or operational changes.
4. Convert acceptance criteria into BDD scenarios and map them to tests.
5. Implement a cohesive change with regression tests.
6. Run repository checks and record exact evidence.
7. Update domain, operations, diagrams and risk records where relevant.
8. Check relative links and label planned or unsupported behavior honestly.
