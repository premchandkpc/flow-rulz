---
title: Functional review of main (2026-11-28)
type: report
status: historical
owner: premchandkpc
last_verified: 2026-11-28 @ 41164c5
baseline: 41164c54c061faf249da9fdb09038ff902ce9b65
---

# Functional review of main (2026-11-28)

What needs fixing in `main` in terms of behavior, ranked by impact. Scope is the durable worker's event-processing, failure-settlement and quarantine path. Every claim was read from source at the baseline commit; none was reproduced by running the code (see [Evidence](#evidence)).

All findings apply to **durable** worker mode. Direct and broker modes were not reviewed.

## Summary

| # | Severity | Finding | Kind | Fix size |
|---|---|---|---|---|
| 1 | High | Contract-validation failures are retried as if transient | Defect | Small |
| 2 | High | Quarantine cannot restore anything: no payload stored, "replay" re-drives nothing | Defect | Medium |
| 3 | Medium-High | Events held by design (no matching ruleset, service-targeted) each stall their shard, across all tenants | Design gap | Medium |

Findings 1 and 3 share a mechanism. Each virtual shard has a JetStream consumer with `MaxAckPending: 1` (`internal/adapters/nats/jetstream.go:274-283`), so one message being retried blocks every later event on that shard, for every tenant that hashes to it. The repository states this trade-off itself ([worker-review-remediation.md](../architecture/worker-review-remediation.md), line 16). The problem is that events which cannot succeed are allowed to hold the head of the line.

The retry schedule that sets the cost: 5 s base (`internal/config/config.go:275`), doubling to a 5 minute ceiling (`internal/application/worker_delivery.go:15`, `186-195`), 24 attempts (`config.go:293`). Delays after attempts 1 to 23 sum to 315 s + 17 x 300 s = 5415 s, about 90 minutes at the ceiling. Equal jitter halves the lower bound, so a held event occupies its shard for roughly **45 to 90 minutes** before quarantine. This is arithmetic from the constants, not a measurement.

## Finding 1: contract-validation failures are retried as transient

**Status: Implemented behavior. No durable-worker test covers it.**

The worker quarantines immediately only when `domain.IsPermanent(err)` is true, which means the Validation, Rule, Evaluation or Authorization classes (`internal/domain/errors.go:314-320`, used at `worker_delivery.go:66`). Event-contract failures do not reach those classes:

- `resolveEventContractSchema`, `validateResolvedContract` and `validateEventPayload` return plain `fmt.Errorf` values: registry not configured, contract not found, deprecated, schema hash mismatch, not an event contract (`internal/services/events/service.go:68-107`).
- Payload errors come from `ContractSchema.ValidatePayload` (`internal/domain/contracts.go:147-172`), which defines and uses no sentinel errors.
- `ClassifyError` matches only sentinels, so these fall through to `ErrorClassInternal` (`errors.go:182-209`), which `IsPermanent` rejects. A sentinel-based `isContractError` exists (`errors.go:322-326`) but the event path never wraps those sentinels.

**Impact:** a single event that violates its declared contract is retried 23 times over roughly 45-90 minutes, stalling its shard for all tenants, then quarantined (Finding 2).

**Fix:** wrap each failure with a sentinel (`domain.ErrContractMismatch` exists; add ones for not-found, deprecated and payload-invalid), using `%w`, and classify them as Validation so `IsPermanent` is true. Operational failures such as a registry lookup error should stay retryable; keep them distinct.

**Acceptance tests:**
- Table test that `IsPermanent` is true for each contract failure path and false for a registry outage.
- Worker test: a contract-violating event is quarantined and ACKed on first delivery, and a following event on the same shard produces an execution.

## Finding 2: quarantine cannot restore anything

**Status: Implemented behavior. The endpoint name promises more than it does.**

- `QuarantineEntry` has no payload field, only `PayloadRef` (`internal/domain/types.go:577-587`). For events it is always the constant `"jetstream:flowrulz"` (`internal/services/events/service.go:118`), so the entry holds no stream sequence or subject either.
- `POST /v1/quarantine/{id}/replay` exists (exercised in `internal/adapters/http/handlers/quarantine_tenant_test.go:75`). The service calls the repository, which runs only `UPDATE quarantine SET updated_at = NOW() ...` (`internal/services/quarantine/service.go:49-55`, `internal/adapters/postgres/quarantine.go:53-65`). Nothing is re-published.
- `ReplayAndDelete` does that no-op and then deletes the entry (`service.go:65-72`). It has no non-test callers today, so it is a latent hazard, not a live one.
- The failing event is ACKed after quarantine (`worker_delivery.go:85-88`), so the work queue no longer holds it.

**Impact:** an operator who calls replay sees success and the event is not reprocessed. An event quarantined for a transient cause, such as a ruleset not yet active, has no supported recovery path.

**Fix:** store the raw envelope (or stream name plus sequence) on the entry with a new migration, and make replay re-publish to the event's shard subject under the existing at-least-once and inbox rules. Until that lands, make the endpoint return 501 or rename it, and make `ReplayAndDelete` delete only after a confirmed re-publish.

**Acceptance tests:**
- Quarantine an event, replay it, and observe an execution for the original event ID.
- Replay it twice and assert there is still one execution (inbox deduplication).

## Finding 3: events held by design stall their shard

**Status: Documented and tested behavior. The cost is not addressed.**

Two kinds of event are deliberately held instead of rejected:

1. **No matching ruleset.** `processFencedEvent` returns `ErrRuleNotReady` ("rule not ready, retry later"; `internal/services/events/transaction.go:55-57`, `errors.go:51`), classified as Broker and therefore retried (`errors.go:188`). Intent: the ruleset may be activated shortly. A test asserts the error is returned (`internal/services/events/service_test.go:153`).
2. **Service-targeted events** (headers `flow.service_id` or `flow.deployment_revision`, or meta `target_service_id` or `deployment_revision`; `internal/domain/types.go:37-52`). The durable worker retries them with no attempt cap and never acknowledges or quarantines them (`worker_delivery.go:119-121`, `172-177`, `197-201`; compare the capped path at `64-67`, `91-96`). Broker redelivery is also unbounded: `NATS_MAX_DELIVER` must be `-1` (`jetstream.go:115-117`). This is documented ([worker.md](../worker.md) line 17, README line 75), tracked as `ROUTE-01` in [TODO.md](../TODO.md), and pinned by tests (`internal/application/worker_deployment_test.go`, `tests/e2e/service_target_test.go`, the latter requiring `NumAckPending > 0`).

Worker-hosted broker publish already rejects targeted events (`internal/services/broker/service.go:118-120`). The JetStream path has no equivalent gate in code I reviewed.

**Impact:** held events occupy the only in-flight slot of their shard consumer. A targeted event holds it indefinitely; an unmatched event holds it for roughly 45-90 minutes and is then quarantined unrecoverably (Finding 2). Every tenant on that shard waits. The documentation says "monitor pending deliveries" but not that unrelated tenants stall. Whether an untrusted tenant can publish such an event directly to the stream depends on NATS permissions, which I did not review.

**Fix options, in order of effort:**
- Document the blast radius in [worker.md](../worker.md) and add an alert on per-shard `NumAckPending` age.
- Add a publish-side gate wherever the project ships a publisher, mirroring the broker-mode check.
- Park instead of hold: re-publish the held event to a dedicated parking subject, then ACK, so the shard continues and the event is neither lost nor processed. This keeps the "not processed" invariant that the tests protect, and it depends on Finding 2's re-publish path for later release. It is a product decision, which is why this is not a one-line fix.

**Acceptance test:** a held event followed by a normal event on the same shard; the normal event produces an execution without waiting for the retry schedule.

## Checked and found working

- **Fencing inside the transaction.** The lease is asserted in the transaction that writes execution and outbox rows (`transaction.go:47-49`); a stale token fails and is classified retryable (`errors.go:290`, class Database), so a lease handoff does not quarantine events.
- **Inbox, execution and outbox commit together.** Writes and `MarkCommitted` share one transaction (`transaction.go:36-45`, `execution.go:70-78`). This closes the separate-SQL-calls crash window recorded in earlier tracking notes.
- **Outbox claiming is atomic.** One `UPDATE ... FROM (SELECT ... FOR UPDATE SKIP LOCKED)` sets `claimed`, attempts and claim expiry, and expired claims are re-eligible (`internal/adapters/postgres/outbox.go:77-98`). This closes the duplicate-claim issue in earlier notes.
- **Subscriber fan-out is idempotent across redelivery.** Effect IDs derive from tenant, event and subscriber, and inserts use `ON CONFLICT (effect_id) DO NOTHING` (`internal/services/events/subscriber_router.go:69`, `outbox.go:215`).
- **Settlement uses a detached, bounded context** (`worker_delivery.go:51`), so a cancelled request does not drop an ACK or retry.

## Evidence

| Check | Result |
|---|---|
| Baseline commit | `41164c5` on `main` |
| GitHub Actions on `41164c5`: Go CI, CodeQL Advanced, Docs | Passed (observed through the GitHub API after completion) |
| Local `go build`, `go vet`, `go test` | **Not run.** The review environment has Go 1.24.7; the module requires 1.26.0 and the toolchain download was refused by the network proxy. |
| Any finding reproduced against NATS and PostgreSQL | **Not run.** All findings come from reading source. |
| Test search | By grep for the failure messages and the `Contract` identifier in `internal/application` and `internal/services/events` tests, plus `IsPermanent`, `Replay` and deployment-target names repo-wide. Not exhaustive. |

Passing CI shows the existing tests pass. It does not show that Findings 1 and 2 are tested; the search above found no tests for them.

## Not reviewed

HTTP authentication and tenant scoping, the scheduler, shard lease renewal under partition, workflow orchestration, batch execution, rule compilation and evaluation, SQL/Mongo parity, NATS stream permissions, direct and broker worker modes, the Rust worker, and the web console. Absence from this report is not evidence they are correct.

## Suggested order

1. Finding 1: smallest change, and it removes a real stall.
2. Finding 2: ship the 501 or rename first if replay cannot land soon; then payload storage and re-publish.
3. Finding 3: documentation and alerting now; decide on parking before changing the retention guarantee that the tests protect.
