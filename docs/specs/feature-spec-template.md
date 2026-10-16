# Feature specification: <feature-name>

- **Status:** Draft | In review | Approved | Implemented | Verified | Superseded
- **Owner:** <team/person>
- **Created / reviewed:** YYYY-MM-DD / YYYY-MM-DD
- **Issue / PR:** <link>
- **Related plan:** <link>
- **Related tests/report:** <links>
- **Current-behavior source and SHA:** <paths, commit>

## 1. Problem and evidence
Describe current observable behavior, who is affected, impact, reproduction and evidence. Separate facts from assumptions.

## 2. Outcome and scope
### Goals
- <measurable outcome>
### Non-goals
- <explicitly excluded behavior>

## 3. Actors and trust boundaries
Identify producer, control-plane caller, router, worker, database, broker, outbox sender and destination. State who may assert tenant identity and internal routing metadata, and what authentication is required.

## 4. Current flow and source of truth
Trace entry point → service/domain logic → port/adapter → persistence/broker → output. Link source, schema, migrations and tests.

## 5. Contract and compatibility
List API/event fields, schema identity, persistence changes, generated artifacts and SDK impact. Describe mixed-version behavior, replay/backfill, deprecation and rollback.

## 6. Normative requirements
Give each requirement an ID. Use MUST for mandatory behavior, SHOULD for a strong recommendation and MAY for optional behavior. Make each requirement observable.

## 7. Invariants
Cover success, duplicate delivery, retries, concurrency, worker handoff, unknown commit outcome, restart and partial failure. Link [system invariants](invariants.md).

## 8. Acceptance criteria
| ID | Given | When | Then | Test/evidence |
|---|---|---|---|---|
| AC-01 | Valid event and active pinned ruleset | Worker processes it | One durable outcome is recorded and input is settled per delivery contract | <test or GAP> |

Include negative, boundary and failure cases.

## 9. Failure and recovery
Specify rollback, crash windows, lease loss, retry classification, quarantine, broker outage and ambiguous downstream outcomes. Identify which component owns each retry.

## 10. Security and privacy
Cover tenant isolation, control-plane scope, validation, secrets, payload logging, rate/size limits and trusted-proxy headers.

## 11. Operations and rollout
Define metrics, logs, alerts, readiness, capacity, migration ordering, feature flags, mixed versions, replay, rollback and irreversible actions.

## 12. Verification plan
| Layer | Exact test/command | Expected evidence | Status |
|---|---|---|---|
| Unit | | | Not run |
| Contract/schema | | | Not run |
| PostgreSQL/NATS integration | | | Not run |
| E2E/recovery | | | Not run |
| Security/static | | | Not run |
| Build/deployment/SDK | | | Not run |

## 13. Risks and decisions
- Risk and mitigation:
- Alternative rejected and why:
- Decision/owner:
- Accepted gap and approver:

## 14. Definition of done
- [ ] Acceptance criteria map to tests or explicit gaps.
- [ ] Contract, schema, migration and generated artifacts agree.
- [ ] Failure, retry, tenant and rollback paths are covered as applicable.
- [ ] Required checks ran against the exact commit; skipped tests are recorded.
- [ ] Operations docs are updated and unsupported guarantees are not claimed.
