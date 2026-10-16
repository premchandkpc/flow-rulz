# FlowRulz verification matrix

This is a checklist, not a claim every row is covered. Replace status only after inspecting the actual test and recording evidence for the exact commit.

**Statuses:** Verified, Partial, Gap, Skipped, Not applicable, Unknown. A skipped test is never Verified. A mock-backed test is not real database/broker evidence.

| Requirement | Unit/service | Contract/adapter | Real PostgreSQL + NATS | E2E/recovery | Initial status | Evidence / next action |
|---|---|---|---|---|---|---|
| Duplicate event has one durable outcome | Dedupe service test | Inbox adapter test | Redeliver same event ID | Restart/redelivery | Unknown | Record path and commit |
| Inbox/execution/outbox atomic | Failure unit test | Transaction adapter tests | Force rollback at each write | Inspect durable state | Unknown | Prove no partial rows |
| ACK follows durable commit | Settlement decision test | Consumer adapter test | Observe DB/ACK timing | Crash around commit/ACK | Unknown | ACK is not effect completion |
| Permanent failure quarantined before ACK | Classification test | Quarantine adapter test | Fail quarantine write | Confirm recoverable message | Unknown | Test quarantine outage |
| Crash after commit before ACK safe | Dedupe test | Adapter tests | Shared DB and broker | Kill/restart in failure window | Unknown | No duplicate decision |
| Stale lease holder fenced | Lease unit test | SQL fencing test | Two workers share DB | Expire lease and stale write | Unknown | Stale transaction rejected |
| Per-key order across retry/handoff | Queue test | Retry adapter test | Shared DB/broker | Multi-worker handoff | Unknown | Local queue alone insufficient |
| MaxDeliver cannot strand work | Retry policy test | Config validation | Finite values rejected | Durable exhaustion handoff | Partial | Current docs prohibit finite MaxDeliver pending recovery |
| Effect retries use stable identity | Effect ID test | Outbox persistence | Retry same effect | Destination dedupe/reconcile | Unknown | Include ambiguous timeout |
| Tenant isolation | Service/auth tests | Query scoping tests | Cross-tenant negatives | Authenticated API flow | Unknown | Tenant field is not auth |
| Contract identity enforced | Validator unit test | Schema/registry test | Registered contract + event | Compatibility scenario | Unknown | Verify name/version/hash |
| Immutable revision selected | Revision test | Persistence constraints | Pinned assignment lookup | Change assignment and process | Unknown | Check worker sync |
| Go/Rust parity where claimed | Shared fixtures | Runtime-specific tests | As needed | Same scenario per runtime | Unknown | Do not infer parity |
| Compose/config readiness | Config validation | Image/schema/Compose checks | Stack smoke test | Restart/readiness | Unknown | Liveness differs from readiness |

## Recording evidence
For each row, add test path/name, exact command, commit SHA, CI URL and result. For a gap, add the next test, owner and issue. Never mark Verified based on a skipped test, mock-only assertion or earlier commit.
