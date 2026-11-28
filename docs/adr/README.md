---
title: Architecture decision records
type: index
status: current
owner: premchandkpc
last_verified: 2026-11-27 @ 285a8c0
---

# Architecture decision records

One decision per file; numbers are permanent. Rules: [STANDARDS.md](../STANDARDS.md#adr-rules). Template: [adr.md](../templates/adr.md).

| ADR | Decision | Status |
|---|---|---|
| [ADR-001](ADR-001-control-plane-data-plane.md) | Separate control plane from event data plane | Accepted |
| [ADR-002](ADR-002-transactional-outbox-boundaries.md) | Transactional outbox persistence boundaries | Accepted |
| [ADR-003](ADR-003-durable-event-log-and-cache.md) | Durable event log and disposable runtime cache | Accepted |
| [ADR-004](ADR-004-log-authoritative-processing.md) | Log-authoritative event processing | Proposed |
| [ADR-005](ADR-005-rust-runtime-architecture.md) | Rust runtime and FlowRulZ-owned log | Proposed |
| [ADR-006](ADR-006-global-domain-service-config.md) | Global Domain and ruleset configuration | Accepted (deployment reconciliation incomplete) |
