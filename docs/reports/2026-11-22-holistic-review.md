---
title: FlowRulz holistic product and engineering review (2026-11-22)
type: report
status: historical
owner: premchandkpc
last_verified: never
---

# FlowRulz holistic product and engineering review (2026-11-22)

**Baseline:** main at `3d7062590e9fa7ad228dc0f6cd619786d6a6f727`. **Method:** documentation, CI definition, merged PR #39 and architecture review inspection; **not** a full code audit, running test report, penetration test, or customer validation. Any finding without a reproduction is a **verification gap**, not a confirmed bug. Refer to [tracked actions](../TODO.md).

## Executive decision

Continue the project, but prioritize the durable Go path and one narrow customer workflow. Commercial potential **7/10 (hypothesis)**; implementation maturity **6/10 (provisional)**. Neither is a benchmark, market survey, or release certification. Do not market exactly-once delivery, global per-key ordering, multi-tenant isolation, dynamic scaling, workflow wait/resume, or Rust parity until proved.

## Scope and supported modes

| Mode | Intended guarantee | Explicit limitation |
|---|---|---|
| durable Go | JetStream at-least-once, PostgreSQL inbox/execution/outbox transaction, ACK after commit, lease fencing | external effects at-least-once; end-to-end order, dynamic rebalance, live chaos and service pinning incomplete |
| direct | NATS local rules, publish before ACK | no DB/inbox/outbox/leases; duplicate effects possible |
| embedded broker | single-process topic/group/ACK/retry with memory/local snapshot | no distributed durability/failover guarantee |
| Rust | experimental runtime and broker transport | do not assume durable parity with Go |

## Evidence and risk register

| ID | Priority | Area | Evidence / uncertainty | Required proof |
|---|---|---|---|---|
| SEC-01 | P0 | Identity | `SECURITY.md` says unauthenticated caller-controlled tenant identity; `docs/architecture/map.md` describes non-dev trusted-proxy middleware. Documentation conflicts; current code not audited. | Inspect middleware/routes, reconcile docs; negative cross-tenant tests; direct-bypass and header-spoof tests |
| SEC-02 | P0 | Broker security | README requires producer authorization for `meta.tenant_id` and partition key; hashed subjects are not authorization. | NATS credentials, subject ACLs, tenant impersonation denial and secret rotation tests |
| REL-01 | P0 | Durable atomicity | README explicitly notes in-memory integration tests cannot prove live PG/NATS failures. | Real PG/NATS test matrix: commit/ACK gap, duplicate, stale fencing, crash after claim, publish/settle gap, replay |
| ROUTE-01 | P0 | Service isolation | README states stored assignments do not enforce isolated service streams/pinned revisions. | Service-scoped routing, revision pinning and isolation tests, including upgrade compatibility |
| ORD-01 | P0 | Ordering | PR #39 and architecture map explicitly limit ordering to shard consumers; independent ruleset consumers and outbox publishers may reorder. | Define order scope, implement ordered outbox where required; cross-worker/handoff adversarial tests |
| REL-02 | P1 | Membership | Fixed `WORKER_REPLICAS`; no coordinated dynamic rebalancing. | Scale 1→3→1, rolling upgrade, expired lease and split-brain tests |
| REL-03 | P1 | Backend parity | Mongo live transactional conformance and Rust durable parity not established. | Backend/runtime conformance suite or explicitly remove from supported production matrix |
| OPS-01 | P1 | Operations | API readiness checks DB, not NATS/worker progress; migration CLI only supports up. | Dependency and worker progress SLOs, alerts, migration recovery/rollback runbook |
| TEST-01 | P1 | CI | Go CI includes race, vet, lint, e2e, coverage; no coverage threshold. PR #39 reports 713 violations of 15-line checker across mixed code. | Verify current CI results, gate critical-path tests, replace arbitrary length rule with complexity/maintainability metrics |
| PROD-01 | P1 | Product | Approval, rollback, audit workflow and correlated wait/resume not established. | One complete user journey with rule version approval, dry run, replay and audit |
| PERF-01 | P1 | Performance | No independently reproduced capacity, p95/p99 or cost envelope in inspected evidence. | Reproducible load harness, hot-key, backlog, failure recovery and cost curves |
| SUP-01 | P1 | Supply chain | Existing CI and security policy; no complete OpenSSF baseline assessment performed. | Scorecard/OSPS baseline mapping, SAST, SBOM, signing, dependency and branch protection checks |
| DX-01 | P2 | Developer experience | Separate Go modules, SDK codegen and multiple runtime modes increase onboarding cost. | Clean-room 30-minute quickstart and generated SDK compatibility matrix |
| DATA-01 | P1 | Data governance | Tenant metadata and replay imply retention, deletion and PII handling requirements. | Data classification, retention, deletion, encryption, backups and restore tests |
| COM-01 | P1 | Commercial | No customer interviews or willingness-to-pay evidence reviewed. | 5 discovery interviews, 2 pilot workflows, buyer comparison and measured time-to-value |
| COM-02 | P2 | Packaging | Multiple modes compete for maintenance effort. | Publish one supported production SKU and explicit non-goals |

## Threat model to validate

Trust boundaries: producer→NATS; NATS→worker; API caller→trusted proxy→API; worker→PG; outbox→consumer; tenant→tenant. Threats: forged tenant IDs, cross-tenant reads, malicious event size/shape, subject ACL bypass, replay poisoning, stale owner settlement, unbounded retry storms, unsafe codegen inputs, dependency compromise, admin audit tampering. For each, require a negative test and an operational mitigation. No security compliance claim follows from this list.

## Correctness acceptance matrix

1. Duplicate event delivered concurrently: at most one committed execution per tenant/event/ruleset; effects use stable IDs.
2. DB commit succeeds, broker ACK lost: redelivery does not create a second committed execution.
3. Outbox publish succeeds, settlement fails: repeated delivery is documented; receiver deduplicates by stable key.
4. Worker lease expires during evaluation: stale fencing token cannot commit or settle newer owner's work.
5. Worker killed during outbox claim: claim eventually recovers without infinite retries; terminal quarantine is inspectable.
6. Two rulesets match same event: inbox associations remain distinct and trace shows both.
7. Hot key under high load: other keys make progress; queue memory and admission bounded.
8. Scale and rebalance: no lost shard, stale owner, or unexplained reordering relative to declared guarantee.
9. Tenant A attempts tenant B config, trace, quarantine, publish: denied at all trust boundaries.
10. Migration on two concurrent startups: serialized and recoverable; rollback/run-forward instructions documented.

**Evidence format:** commit SHA; environment/tool versions; exact command; exit status; logs/metrics/artifacts; failures/skips; reviewer. A test listed in CI is not a passing test until its run is inspected.

## Commercial review

**Buyer hypothesis:** platform/backend teams implementing event-driven policy decisions (fraud screening, order eligibility, operational alerts). **Competitors/substitutes:** custom consumers, NATS/Kafka + application logic, workflow engines and stream processors. Do not assert superiority without comparative benchmarks. **Differentiation to test:** versioned rules + transactional decision/outbox + explainable execution + faster integration. **Initial product boundary:** managed or self-hosted durable Go engine with Go SDK, audit and operations; other modes experimental.

Validation gates: (a) five problem interviews, (b) two design partners willing to deploy, (c) one real integration completed without maintainer intervention, (d) measurable time saved or correctness improvement, (e) security and recovery evidence. Price only after willingness-to-pay interviews; do not invent ARR or customer counts.

## Release gate

No external production claim until all P0 tasks in [TODO](../TODO.md) have linked passing evidence, threat-model negatives pass, backup/restore and on-call runbooks exist, and the exact supported-mode matrix is published. P1 items may be waived only with documented owner, mitigation and expiry.

## References

- [README](../../README.md), [architecture map](../architecture/map.md), [historical architecture review](2026-11-13-architecture-review.md), [CI](../../.github/workflows/go-ci.yml), [security policy](../../SECURITY.md), [PR #39](https://github.com/premchandkpc/flow-rulz/pull/39)
- [OpenSSF OSPS Baseline](https://baseline.openssf.org/), [OpenSSF Scorecard](https://github.com/ossf/scorecard)
