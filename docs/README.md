# FlowRulz documentation index

This directory is the canonical home for human-maintained project documentation. Use the categories below to find the source of truth instead of scattering new Markdown files across the repository.

## Runtime at a glance

- **Control plane:** Go HTTP API manages Domains, Service assignments, ruleset revisions, contracts, workflow definitions, and operational views.
- **Transport:** NATS JetStream carries business events. Producers publish EventEnvelope messages to shard subjects.
- **Durable state:** PostgreSQL stores processing state, inbox/deduplication, executions, and outbox intent.
- **Execution:** Go workers own shard leases, validate routing context, resolve pinned rulesets, evaluate events, and commit durable outcomes before ACK.
- **Effects:** outbox delivery is separate from the event decision and can be retried.
- **Other runtimes:** verify Rust capability parity against implementation and tests. Proposal code is not a shipped adapter.

## Documentation map

| Folder | Purpose | Start here |
|---|---|---|
| [specs/](specs/README.md) | Required behavior, acceptance criteria, invariants | [Feature spec template](specs/feature-spec-template.md) |
| [plans/](plans/README.md) | Work sequence, dependencies, exit criteria | [Change plan template](plans/change-plan-template.md) |
| [domain/](domain/README.md) | Domain model, identities, event contracts, package boundaries | [Domain model](domain/domain-model.md) |
| [rules/](rules/README.md) | Rule structure, semantics, authoring, lifecycle | [Authoring standard](rules/rule-authoring-standard.md) |
| [testing/](testing/README.md) | Test strategy, BDD scenarios, verification evidence | [Test strategy](testing/test-strategy.md) |
| [reports/](reports/README.md) | Dated findings, risks, audit reports | [Risk register](reports/risk-register.md) |
| [architecture/](architecture/overview.md) | Runtime boundaries, data flow, reliability | [Architecture map](architecture/map.md) |
| [adr/](adr/) | Accepted architecture decisions | [ADR directory](adr/) |
| [audit/](audit/) | Historical repository and architecture reviews | [Audit reports](audit/) |
| [security/](security/) | Security findings and remediation context | [CodeQL alert 24](security/codeql-alert-24.md) |
| [examples/](examples/README.md) | Data fixtures and sample applications | [Examples index](examples/README.md) |
| [proposals/](proposals/) | Proposed, non-shipped architecture and experiments | [Kafka log-first proposal](proposals/kafka-log-first-data-plane.md) |
| [sdk/](sdk/) | SDK and load-generator guides | [Random load generator](sdk/go/random-loadgen.md) |
| [console/](console/README.md) | Web operations console | [Console guide](console/README.md) |

## Current implementation guides

- API and configuration: [API](api.md), [Domain](domain.md), [Domain packages](domain/packages.md)
- Event and schema: [Events](event.md), [Contracts](contract.md), [Contracts overview](contracts.md)
- Rules and assignment: [Rules](rules.md), [Rulesets](ruleset.md)
- Runtime: [Workers](worker.md), [Delivery reliability](architecture/delivery-reliability.md)
- Operations: [Operations](operations.md), [Compose deployment](deployment-compose.md)
- Verification: [Testing checklist](testing.md), [Test strategy](testing/test-strategy.md), [Verification matrix](testing/verification-matrix.md)
- Planning: [Implementation plan](IMPLEMENTATION_PLAN.md), [Refactoring plan](plans/refactoring-plan.md), [Legacy architecture redesign](plans/legacy-architecture-redesign.md), [Roadmap](roadmap.md)
- Examples: [Data fixtures](examples/data-fixtures.md), [Banking](examples/banking.md), [E-commerce](examples/ecommerce.md), [Food delivery](examples/food-delivery.md), [Random producer](examples/random-producer.md)
- Other tools: [Random load generator](sdk/go/random-loadgen.md), [Web console](console/README.md)
- History and direction: [Future architecture](future-architecture.md), [Audit reports](audit/), [Security policy](../SECURITY.md)
- [Kafka log-first proposal](proposals/kafka-log-first-data-plane.md) is a design proposal, not current runtime transport. Supporting experiments and their test limitations are documented under [proposals/kafka-log-first/](proposals/kafka-log-first/).

## Documentation contract

1. Current code, schemas, migrations, configuration, tests, and CI define actual behavior.
2. Specs define required behavior; plans define intended work; reports record evidence at a specific commit.
3. NATS delivery is at-least-once. ACK means durable responsibility, not completion of an external side effect.
4. Every substantive acceptance criterion links to a test or an explicit gap.
5. Preserve dated audits as historical snapshots; add follow-up reports instead of rewriting history.
6. Record commit SHA, exact commands, environment, exit status, and skipped tests.
7. Keep canonical field definitions in schemas and implementation guides; link to them instead of creating competing copies.
8. Keep new human-maintained documentation in `docs/`. The root `README.md`, `AGENTS.md`, and `SECURITY.md` are intentional discovery/tooling exceptions. GitHub templates and agent skill files stay in their required `.github/` and `.opencode/` locations.

## Change workflow

1. Read `AGENTS.md`, affected code, schemas, migrations, tests, and CI.
2. Write/update the spec, including failure and compatibility behavior.
3. Create a plan for cross-package, persistence, concurrency, security, or operational changes.
4. Convert acceptance criteria into BDD scenarios and map them to tests.
5. Implement a cohesive change with regression tests.
6. Run repository checks and record exact evidence.
7. Update domain, operations, diagrams, and risk records where relevant.
8. Check relative links and label planned or unsupported behavior honestly.
