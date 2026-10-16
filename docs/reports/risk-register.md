# FlowRulz engineering risk register

**Purpose:** track risk areas needing evidence. These rows are not assertions that every item is a confirmed defect. Assign severity only after examining source, tests and deployment configuration.

| ID | Risk area | Failure mode to verify | Required evidence | Initial status | Owner / issue |
|---|---|---|---|---|---|
| R-01 | Inbox/execution/outbox atomicity | Partial durable state after rollback or crash | PostgreSQL tests inspecting persisted rows | Unknown | |
| R-02 | Commit-before-ACK window | Redelivery duplicates or loses a decision | Kill/restart E2E with JetStream and shared DB | Unknown | |
| R-03 | Quarantine persistence | ACK occurs despite failed quarantine write | Inject DB outage at quarantine boundary | Unknown | |
| R-04 | Lease/fencing | Stale worker commits after ownership transfer | Multi-worker lease expiry and stale-write test | Unknown | |
| R-05 | Per-key ordering | Retry/handoff reorders same tenant/partition key | Multi-worker delayed retry and handoff | Unknown | |
| R-06 | MaxDeliver exhaustion | Broker stops redelivery while work unresolved | Config validation and durable recovery test | Mitigation documented: finite MaxDeliver prohibited | |
| R-07 | External effect idempotency | Timeout/retry duplicates destination action | Stable effect ID plus destination dedupe/reconciliation | Unknown | |
| R-08 | Tenant isolation | Cross-tenant access via API or persistence | Authenticated negative read/write tests | Unknown | |
| R-09 | Global control-plane authorization | Tenant-only caller mutates global config | Scope/middleware tests for global routes | Unknown | |
| R-10 | Contract/revision consistency | Event selects incompatible schema or ruleset | Registry/schema/runtime-selection tests | Unknown | |
| R-11 | Deployment reconciliation | Domain assignment differs from worker configuration | Change assignment and inspect deployed worker state | Known limitation documented | |
| R-12 | Go/Rust parity | Routing/evaluation/recovery differs across runtimes | Shared fixtures and equivalent tests | Unknown | |
| R-13 | Retention/capacity | Stream removes data before recovery/replay | Capacity budget, alert tests and runbook | Unknown | |
| R-14 | Readiness/observability | Health says ready despite critical dependency failure | Dependency-failure and alert validation | Unknown | |

## Risk management rules
1. Link each risk to a reproducible scenario, source path, test and issue.
2. Record likelihood, impact, deployment exposure and mitigation before assigning severity.
3. Give actionable risks an owner and target date.
4. Close only after fix and required verification pass on a named commit.
5. Accepted risks need an approver, rationale, review/expiry date and mitigation.
6. Preserve historical reports and link updated evidence from this register.
