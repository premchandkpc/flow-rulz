# FlowRulz engineering risk register

**Status:** Initial register structure. These are risk areas to track, not claims that each is a confirmed defect. Attach current source/test evidence before assigning severity or closure.

| ID | Risk area | Failure mode to verify | Required evidence | Status | Owner / issue |
|---|---|---|---|---|---|
| R-01 | Inbox + execution + outbox atomicity | Partial durable state after failure | Real database transaction tests | Unknown | |
| R-02 | Commit-before-ACK crash window | Redelivery duplicates or loses processing | E2E process crash test with JetStream + DB | Unknown | |
| R-03 | Shard lease/fencing | Stale worker writes after ownership loss | Multi-worker lease-expiry test | Unknown | |
| R-04 | Per-key ordering | Retry or handoff reorders same-key events | Multi-worker retry/replay test | Unknown | |
| R-05 | Poison / max delivery | Event becomes stranded or invisible | Exhaustion, quarantine and alert test | Unknown | |
| R-06 | External effect idempotency | Duplicate side effect on outbox retry | Stable effect ID plus destination dedupe test | Unknown | |
| R-07 | Tenant isolation | Cross-tenant read/write via API or storage | Authorization and repository negative tests | Unknown | |
| R-08 | Go/Rust parity | Different results or recovery semantics | Differential tests against same fixtures/infrastructure | Unknown | |
| R-09 | Retention / capacity | Durable log fills or expires before replay | Capacity budget, alert and retention tests | Unknown | |
| R-10 | Global control-plane auth | Unauthorized global configuration mutation | Middleware/route tests with auth claims | Unknown | |

## Updating this register
1. Reproduce or inspect the risk against a specific commit.
2. Record concrete evidence and impact.
3. Assign severity only after likelihood and blast radius are understood.
4. Link a regression test, issue/PR, owner, mitigation and due date.
5. Close only when the fix and required verification pass; otherwise use Accepted or Unknown with rationale.
