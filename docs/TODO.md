# FlowRulz prioritized engineering and product TODO

**Updated:** 2026-11-22. **Baseline:** `3d706259`. This is the single actionable queue for the [holistic review](reports/2026-11-22-holistic-review.md). Items are **open**, not implemented. Link PR, test evidence and commit before checking a box. P0 blocks production claims; P1 blocks broad adoption; P2 improves usability. Do not split every checkbox into a service or abstraction.

## P0: safety and correctness

- [ ] **SEC-01** Audit actual API identity middleware and every global/tenant route; reconcile stale `SECURITY.md` against code. Acceptance: spoofed proxy headers, unauthenticated direct access and cross-tenant read/write/replay denied in automated tests.
- [ ] **SEC-02** Enforce authenticated producer tenant and partition identity at broker boundary with NATS subject ACLs. Acceptance: tenant A cannot publish/consume tenant B subjects or forge envelope identity.
- [ ] **REL-01** Add real PostgreSQL+NATS crash/ACK/commit/outbox recovery tests (not mocks). Acceptance: all ten scenarios in review correctness matrix recorded with reproducible commands and logs.
- [ ] **ROUTE-01** Implement service-scoped runtime routing and pinned rule revisions or reject unsupported deployment configurations consistently. Acceptance: targeted events only evaluated by assigned service/revision; upgrade compatibility tests.
- [ ] **ORD-01** Specify ordering guarantee per shard, ruleset and effect destination; enforce same-key outbox ordering where required. Acceptance: redelivery, handoff and concurrent publisher tests prove declared guarantee.
- [ ] **REL-04** Validate legacy inbox upgrade, historical inconsistent rows and concurrent migration locks on live PG. Acceptance: documented stop/backup/migrate/verify/restart procedure and non-destructive reconciliation.

## P1: operational and enterprise readiness

- [ ] **REL-02** Design coordinated membership/rebalancing and exercise scale up/down/rolling restarts; until then document fixed replica count as supported limit.
- [ ] **REL-03** Run live Mongo conformance and Rust-vs-Go differential/recovery tests; otherwise mark experimental and block prod flags.
- [ ] **OPS-01** Define SLOs for event age, processing latency, outbox backlog, lease coverage, quarantine rate; instrument and alert with dashboards/runbooks.
- [ ] **OPS-02** Verify readiness and dependency degradation, graceful shutdown, bounded queues, retry budgets and load shedding under NATS/PG outages.
- [ ] **OPS-03** Document backup/PITR/restore drills, migrations, forward fixes and disaster recovery RPO/RTO.
- [ ] **SEC-03** Threat model all trust boundaries, test tenant isolation, enforce TLS/secrets, audit sensitive actions, validate outbound destinations and payload size.
- [ ] **SEC-04** Review OpenSSF OSPS baseline and Scorecard checks, branch protection, CodeQL, dependency scanning, SBOM, provenance and signed releases; record evidence rather than assuming compliance.
- [ ] **DATA-01** Specify PII classification, retention, replay authorization, encryption and tenant deletion behavior.
- [ ] **TEST-01** Capture exact CI run status for all Go modules, Rust, web, real E2E; add meaningful coverage/critical-path gates, fuzz/property tests and performance regression thresholds.
- [ ] **TEST-02** Replace blanket 15-line function limit with responsibility/complexity-driven review; record baseline debt and exceptions rather than splitting coherent code mechanically.
- [ ] **PERF-01** Publish reproducible benchmark with throughput, p50/p95/p99, CPU, heap, allocations, hot-key fairness, backlog recovery, storage growth and cost/event.
- [ ] **PROD-01** Build one complete rule author→validate→simulate→approve→activate→audit→rollback journey; ensure immutable revisions and explainable traces.
- [ ] **PROD-02** Establish SDK/API compatibility policy, versioned schemas, migration strategy and clean-room onboarding test.
- [ ] **COM-01** Interview five target teams, recruit two design partners, document incumbent comparison, pricing hypothesis and integration time.

## P2: usability and focus

- [ ] **COM-02** Publish supported production mode and experimental-mode table, explicit non-goals, license/support model and deployment packaging.
- [ ] **DX-01** Improve SDK examples, error taxonomy, quickstart, local demo and 30-minute integration acceptance test.
- [ ] **DX-02** Evaluate whether direct/embedded/Rust modes justify maintenance cost; defer expansion without adoption evidence.
- [ ] **OPS-04** Publish release checklist, changelog discipline, security reporting SLA and operator upgrade guide.

## Execution order and evidence

**Phase 1:** SEC-01/02, REL-01/04, ROUTE-01, ORD-01. **Phase 2:** OPS/SEC/DATA/TEST and production-mode validation. **Phase 3:** product workflow, benchmarks and customer pilots. Work in small PRs; each PR must identify affected invariants, negative tests, rollback, docs and evidence.

| ID | Owner | PR | Test evidence | Status |
|---|---|---|---|---|
| All above | Unassigned | Not started | Not collected | Open |

Update the individual issue or PR with owner and evidence; the table above is deliberately not a fabricated progress report.
