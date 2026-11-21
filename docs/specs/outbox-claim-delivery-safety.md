# Feature specification: outbox claim and delivery safety

- **Status:** Implemented
- **Owner:** FlowRulz runtime
- **Created / reviewed:** 2026-11-14 / 2026-11-14
- **Issue / PR:** https://github.com/premchandkpc/flow-rulz/pull/39
- **Related plan:** [Outbox claim delivery plan](../plans/outbox-claim-delivery-safety.md)
- **Current-behavior source and SHA:** `internal/services/effects`, `internal/adapters/{postgres,mongo}/outbox.go`, `6d454dd`

## 1. Problem and evidence

Outbox attempts incremented only after a sender returned an error. A publisher crash after claiming could repeat forever without exhausting the effect. Sends inherited an unbounded caller context while a whole sequential batch shared a fixed two-minute claim.

## 2. Outcome and scope

### Goals

- Count each durable claim as one delivery attempt.
- Quarantine an expired claim after its final allowed attempt without sending again.
- Bound each destination call.
- Keep the claim valid for the configured sequential batch worst case.

### Non-goals

- Parallel destination calls.
- Atomic quarantine and outbox settlement.
- Same-key ordering across independent outbox records.

## 3. Actors and trust boundaries

The worker claims effects from PostgreSQL or MongoDB. The effect sender calls an external destination. Destinations remain responsible for deduplicating the stable idempotency key.

## 4. Current flow and source of truth

`application.Worker` calls `effects.Service`, which claims through `ports.OutboxDispatcher`, sends through `ports.EffectSender`, then settles through the claim token. PostgreSQL and MongoDB adapters own atomic claim updates. The Rust durable worker follows the same PostgreSQL contract.

## 5. Contract and compatibility

`OutboxEffect.Attempts` now means durable claims made, including a claim interrupted by process death. No API or event schema changes. Existing rows remain compatible. Rollback restores old counting semantics without changing stored data.

## 6. Normative requirements

- **OBX-01:** A successful claim MUST atomically increment `attempts` before returning the effect.
- **OBX-02:** A claimed effect with `attempts > max_attempts` MUST be quarantined without another destination call.
- **OBX-03:** A failed final allowed attempt MUST be quarantined.
- **OBX-04:** Every destination call MUST use a bounded context.
- **OBX-05:** Claim TTL MUST cover all sequential send timeouts plus settlement margin.
- **OBX-06:** Claim-token fencing and stable idempotency keys MUST remain unchanged.

## 7. Invariants

Duplicate claims remain possible after ambiguous downstream outcomes. Stale claimants cannot settle newer claims. Attempt count advances across worker crashes. A timeout is a failed attempt; the receiver may still have acted and must deduplicate.

## 8. Acceptance criteria

| ID | Given | When | Then | Test/evidence |
|---|---|---|---|---|
| AC-01 | Pending or expired effect | Adapter claims it | Returned and stored attempts increase by one | Adapter claim tests |
| AC-02 | Expired final claim | Another worker reclaims it | Effect is quarantined without send | Service exhaustion test |
| AC-03 | Blocking sender | Send timeout elapses | Sender context ends and retry is scheduled | Service timeout test |
| AC-04 | Sequential batch | Service calculates claim | TTL covers batch timeouts and margin | Service TTL test |

## 9. Failure and recovery

Crash after claim consumes an attempt. Reclaim consumes the next attempt. A claim beyond the maximum exists only long enough to quarantine it. Failed quarantine remains claim-fenced and follows existing cleanup behavior.

## 10. Security and privacy

No identity or payload logging changes. Tenant identity remains mandatory in adapters. Timeout errors must not include payloads.

## 11. Operations and rollout

No migration is required. Operators should watch retry and quarantine counts. Mixed versions may increment attempts at different stages, so deploy Go and Rust workers together where practical.

## 12. Verification plan

| Layer | Exact test/command | Expected evidence | Status |
|---|---|---|---|
| Unit | `go test -race -count=1 ./internal/services/effects` | timeout and exhaustion pass | Passed |
| Adapter | `go test -race -count=1 ./internal/adapters/postgres ./internal/adapters/mongo` | claim contract passes | Passed |
| Root | `go test -race -count=1 ./...` | no regression | Passed |
| Rust | `cargo test --manifest-path rust-worker/Cargo.toml` | PostgreSQL parity and no regression | Passed: 65 tests |
| Static | `go vet ./...`; `cargo clippy --manifest-path rust-worker/Cargo.toml --all-targets -- -D warnings` | no findings | Passed |
| Function policy | `python scripts/quality/check_functions.py` | repository-wide 15-line limit | Failed: 713 pre-existing violations remain |

## 13. Risks and decisions

- Risk: timeouts can produce ambiguous external success. Stable idempotency keys remain mandatory.
- Decision: count claims instead of reported failures because claims survive process crashes.
- Accepted gap: outbox ordering and atomic quarantine need separate specifications.

## 14. Definition of done

- [x] Acceptance criteria map to tests.
- [x] PostgreSQL, MongoDB, and the Rust PostgreSQL runtime implement the same claim contract.
- [x] Feature checks pass; repository-wide function-policy debt is recorded.
- [x] Residual gaps remain explicit.
