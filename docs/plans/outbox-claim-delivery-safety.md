# Change plan: outbox claim and delivery safety

- **Status:** Complete
- **Owner:** FlowRulz runtime
- **Issue / PR / branch:** PR 39 / `refactor/fifteen-line-functions`
- **Spec:** [Outbox claim delivery safety](../specs/outbox-claim-delivery-safety.md)
- **Baseline SHA:** `6d454dd`
- **Target SHA:** PR head containing this plan
- **Last updated:** 2026-11-14

## 1. Outcome and success measure

Meet AC-01 through AC-04. Crashes consume attempts, sends time out, and claim fencing remains intact.

## 2. Current behavior

The service claims a sequential batch for two minutes. Adapters preserve the prior attempt count. Only a returned send error increments attempts.

## 3. Proposed design

PostgreSQL, MongoDB, and the Rust PostgreSQL worker increment attempts in the atomic claim operation. Both runtimes treat returned attempts as authoritative, apply a per-send timeout, and derive claim TTL from batch size.

## 4. Scope and ownership

| Area | Files/packages | Owner | Notes |
|---|---|---|---|
| Service | `internal/services/effects` | runtime | timeout, exhaustion, TTL |
| Adapters | `internal/adapters/{postgres,mongo}` | storage | atomic attempt increment |
| Rust runtime | `rust-worker/src/runtime/effects.rs` | runtime | PostgreSQL parity |
| Tests/docs | adjacent tests and this spec | runtime | contract and failure paths |

Outbox ordering, concurrency, and quarantine atomicity are out of scope.

## 5. Ordered work breakdown

| Step | Deliverable | Acceptance evidence | Status |
|---|---|---|---|
| 1 | Specification and failing tests | AC-01 through AC-04 | Complete |
| 2 | Adapter claim increment | Adapter tests pass | Complete |
| 3 | Timeout and exhausted-claim handling | Service tests pass | Complete |
| 4 | Race and root verification | Verification record | Complete |

## 6. Data and compatibility

No migration. Existing attempt values retain meaning as completed failures until first new-version claim.

## 7. Failure and operations

Claim tokens still fence every settlement. A timed-out destination may have completed; receiver idempotency remains required.

## 8. Verification record

| Check | Actual | Commit / CI |
|---|---|---|
| Focused race tests | Passed | candidate PR head |
| Root race tests | Passed: `go test -race -count=1 ./...` | candidate PR head |
| Go build and vet | Passed | candidate PR head |
| Rust tests | Passed: 65 | candidate PR head |
| Rust clippy and repository formatting | Passed | candidate PR head |
| 15-line function policy | Failed: 713 repository-wide violations | candidate PR head |

## 9. Risks and decisions

- Longer batch claims delay crash recovery but prevent live claims expiring mid-batch.
- Counting the claim is the only durable observation before external I/O.

## 10. Exit criteria

- [x] AC-01 through AC-04 pass.
- [x] PostgreSQL, MongoDB, and Rust parity pass.
- [x] Root race tests pass.
- [x] PR describes residual limits.
