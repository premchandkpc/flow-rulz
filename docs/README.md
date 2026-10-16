# FlowRulz documentation hub

This is the canonical entry point for engineering documentation. Start with the root [README](../README.md) for current runtime behavior and setup, and [AGENTS.md](../AGENTS.md) for mandatory engineering rules.

## Documentation rules
- **Source of truth:** implementation, schemas, migrations, tests, configuration and CI. Documentation must not turn a proposal into a shipped guarantee.
- **One purpose per document:** specs define required behavior; plans sequence changes; architecture explains boundaries; domain docs define concepts and contracts; tests record evidence; reports record dated findings.
- **Status is explicit:** label important statements as implemented, verified, partial, planned, or unknown when the distinction matters.
- **Traceability:** every non-trivial spec links acceptance criteria to tests. Every report includes the commit/ref and commands or evidence used.
- **No silent history edits:** preserve dated audit reports as historical snapshots. Add a new report when the state changes.

## Canonical document structure
| Area | Use it for | Start here |
|---|---|---|
| [Specs](specs/README.md) | Requirements, invariants and acceptance criteria | [Feature spec template](specs/feature-spec-template.md) |
| [Plans](plans/README.md) | Ordered implementation plans, dependencies and exit criteria | [Change plan template](plans/change-plan-template.md) |
| [Architecture](architecture/overview.md) | System boundaries, runtime flow, ports/adapters and decisions | [Architecture map](architecture/map.md) |
| [Domain](domain/README.md) | Domain concepts, event identity and contracts | [Domain model](domain/domain-model.md) |
| [Rules](rules/README.md) | Rule authoring, revision and activation expectations | [Rule authoring standard](rules/rule-authoring-standard.md) |
| [Testing](testing/README.md) | Test layers, BDD scenarios and verification evidence | [Test strategy](testing/test-strategy.md) |
| [Operations](operations.md) | Current deployment and troubleshooting | [Compose deployment guide](deployment-compose.md) |
| [Reports](reports/README.md) | Dated audits, review findings and risks | [Report template](reports/report-template.md) |
| [ADRs](adr/) | Architecture decisions and consequences | [Architecture overview](architecture/overview.md) |
| [Diagrams](diagrams/) | Mermaid source diagrams | [System design](diagrams/01-system-design.mmd) |

## Existing implementation guides
- [API](api.md) · [Domain configuration](domain.md) · [Events](event.md) · [Contracts](contract.md) / [Contracts overview](contracts.md)
- [Rules reference](rules.md) · [Rulesets](ruleset.md) · [Workers](worker.md) · [Rust worker status](rust-worker.md)
- [Design notes](design.md) · [Delivery reliability](architecture/delivery-reliability.md) · [Operations](operations.md) · [Testing checklist](testing.md)
- [Roadmap](roadmap.md) · [Future architecture proposal](future-architecture.md)
- [Historical implementation plan](IMPLEMENTATION_PLAN.md) · [Historical architecture redesign plan](plans/legacy-architecture-redesign.md)
- [Historical audit reports](audit/) · [Security findings](security/)

## Before changing documentation
1. Read relevant implementation, schema, migration, tests and CI.
2. Update the canonical document and its index.
3. Link requirements to observable acceptance criteria and test evidence.
4. Mark unsupported or unverified behavior as a gap.
5. Check relative Markdown links in changed files.
