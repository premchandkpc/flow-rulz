# Feature specification: <feature-name>

- **Status:** Draft | In review | Approved | Implemented | Verified | Superseded
- **Owner:** <team/person>
- **Created:** YYYY-MM-DD
- **Last reviewed:** YYYY-MM-DD
- **Related plan:** [link]
- **Related tests/report:** [links]

## 1. Problem and evidence
Describe the user/system problem, current behavior, impact, and source-backed evidence.

## 2. Goals and non-goals
### Goals
- 
### Non-goals
- 

## 3. Actors and trust boundaries
Identify producers, control-plane callers, workers, storage, broker, downstream effects, trusted inputs, tenant identity, and authorization boundaries.

## 4. Domain and contract changes
Describe affected concepts, event/API contracts, schemas, persistence records, migrations, and compatibility requirements. Link canonical definitions.

## 5. Required behavior
Use numbered, testable statements. Use MUST/SHOULD/MAY consistently.

## 6. Invariants
List properties that must remain true during success, retries, concurrency, crash recovery, and partial failure.

## 7. Acceptance criteria
| ID | Given | When | Then | Verification |
|---|---|---|---|---|
| AC-01 | <initial state> | <action> | <observable result> | <test path or gap> |

## 8. Failure and recovery
Cover timeouts, duplicate delivery, transaction rollback, process crash, lease loss/fencing, retry classification, poison messages, and downstream unavailability as applicable.

## 9. Security and privacy
Document authentication/authorization, tenant isolation, validation, secrets, sensitive payloads, logging, and abuse limits.

## 10. Operations and observability
Define metrics, logs, traces, alerts, readiness, capacity implications, and operator recovery steps.

## 11. Rollout and rollback
Include migration ordering, feature flags, mixed-version behavior, backfill/replay needs, rollback safety, and data compatibility.

## 12. Verification plan
| Layer | Test or command | Expected evidence | Status |
|---|---|---|---|
| Unit | | | Not run |
| Adapter/integration | | | Not run |
| E2E/recovery | | | Not run |
| Security/static checks | | | Not run |
| Build/deployment | | | Not run |

## 13. Risks, alternatives and open questions
- **Risk:** 
- **Alternative considered:** 
- **Open question:** 

## 14. Definition of done
- [ ] Acceptance criteria map to tests or documented gaps.
- [ ] Compatibility and rollback have been reviewed.
- [ ] Relevant docs, schemas, migrations and examples are updated.
- [ ] Required checks were run and results recorded against the exact commit.
- [ ] Remaining limitations are stated without overstating guarantees.
