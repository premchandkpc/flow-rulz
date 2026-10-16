# Change plan: <change-name>

- **Status:** Draft | Ready | In progress | Blocked | Complete
- **Owner:** 
- **Spec:** 
- **Branch/PR:** 
- **Target:** 
- **Last updated:** YYYY-MM-DD

## Outcome
State the observable result and why it matters.

## Current behavior
Describe the actual path from entry point through domain logic, ports/adapters, persistence/broker, and tests. Cite source files.

## Proposed behavior
Describe the intended design and why it preserves relevant invariants.

## Scope
### In scope
- 
### Out of scope
- 

## Work breakdown
| Step | Change | Dependencies | Files/packages | Acceptance evidence | Status |
|---|---|---|---|---|---|
| 1 | | None | | | Not started |

## Data and compatibility
Document schema/migration changes, API/event compatibility, mixed-version behavior, backfill/replay, and rollback safety.

## Failure and operations
Describe transaction boundaries, retries, idempotency, concurrency, lease/fencing, observability, capacity, and operator recovery.

## Verification
| Check | Command/test | Required result | Actual result | Commit |
|---|---|---|---|---|
| Unit | | | Not run | |
| Integration | | | Not run | |
| E2E/recovery | | | Not run | |
| Static/security | | | Not run | |
| Build/deploy | | | Not run | |

## Risks and decisions
- **Risk / mitigation:** 
- **Alternative rejected / reason:** 
- **Decision needed:** 

## Exit criteria
- [ ] Spec acceptance criteria are satisfied or residual gaps accepted explicitly.
- [ ] Regression tests cover changed failure boundaries.
- [ ] Relevant Go and nested modules / Rust / console checks ran as applicable.
- [ ] Docs, schemas, examples and runbooks are consistent.
- [ ] Review confirms no weakened invariant or overstated guarantee.
- [ ] Final verification evidence references the exact PR head commit.
