# Change plan: <change-name>

- **Status:** Draft | Ready | In progress | Blocked | Complete
- **Owner:** 
- **Issue / PR / branch:** 
- **Spec:** 
- **Baseline SHA:** 
- **Target SHA:** <fill during verification>
- **Last updated:** YYYY-MM-DD

## 1. Outcome and success measure
State the observable result. Link acceptance criteria instead of copying the full spec.

## 2. Current behavior
Trace entry point → domain/use case → port → adapter → durable state/broker → output. Cite source, schema and tests. Separate confirmed defects from unverified risks.

## 3. Proposed design
Describe responsibilities, dependency direction, interfaces, transaction boundary and failure behavior. Explain how [invariants](../specs/invariants.md) remain true and list alternatives considered.

## 4. Scope and ownership
### In scope
- 
### Out of scope
- 
| Area | Files/packages | Owner | Notes |
|---|---|---|---|
| Domain/service | | | |
| Adapter/storage/broker | | | |
| API/schema/SDK | | | |
| Tests/docs/operations | | | |

## 5. Ordered work breakdown
| Step | Deliverable | Depends on | Acceptance evidence | Status |
|---|---|---|---|---|
| 1 | Add failing regression tests | None | Fails for old behavior | Not started |
| 2 | Implement minimal fix | Step 1 | Targeted test passes | Not started |
| 3 | Run adjacent/infrastructure tests | Step 2 | Results recorded | Not started |
| 4 | Update docs and rollout artifacts | Step 2 | Review complete | Not started |

## 6. Data and compatibility
List migrations, indexes, API/event changes, generated code, mixed-version behavior, retention/replay, backfill and rollback safety.

## 7. Failure and operations
Describe ACK ordering, retries, idempotency, leases/fencing, cancellation/shutdown, metrics, alerts, capacity and operator recovery. Cover unknown commit and ambiguous external outcomes.

## 8. Verification record
| Check | Exact command/test | Expected | Actual | Commit / CI |
|---|---|---|---|---|
| Focused regression | | | Not run | |
| Unit + race | | | Not run | |
| PostgreSQL/NATS integration | | | Not run | |
| E2E/recovery | | | Not run | |
| Schema/generated artifacts | | | Not run | |
| Rust/SDK/console if affected | | | Not run | |
| Lint/security/build/Compose | | | Not run | |

## 9. Risks and decisions
- Risk / mitigation:
- Alternative rejected / reason:
- Dependency / owner:
- Decision / deadline:
- Residual risk accepted by:

## 10. Exit criteria
- [ ] Acceptance criteria verified or gaps explicitly accepted.
- [ ] Regression tests cover changed failure boundaries.
- [ ] Migrations, schemas, generated code, SDKs, console and deployment are consistent.
- [ ] Required CI passed for exact final PR head.
- [ ] Skipped tests and limitations are visible.
- [ ] Operations and rollback steps are actionable.
