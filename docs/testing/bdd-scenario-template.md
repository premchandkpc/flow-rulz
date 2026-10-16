# BDD scenarios: <feature>

- **Spec / acceptance IDs:** 
- **Implementation path:** 
- **Test path:** 
- **Status:** Proposed | Automated | Verified | Partial | Gap | Skipped
- **Last verified commit:** 
- **Infrastructure:** unit fake | PostgreSQL | NATS JetStream | full stack

## Scenario: <observable outcome>
**Given** <initial durable database state and configuration>

**And** <broker message, worker/lease state, tenant and partition identity>

**When** <event is processed or a failure is injected at a named boundary>

**Then** <observable API, database and broker result>

**And** <expected effect calls, ordering, retry or quarantine behavior>

## Failure variants
- Duplicate delivery of the same event ID
- SQL error or transaction rollback
- Crash before commit
- Crash after commit but before ACK
- Lease expiry / stale worker write
- NATS outage or redelivery after restart
- Quarantine store unavailable
- External destination timeout with ambiguous outcome
- Unauthorized tenant or global control-plane request
- Contract mismatch or unavailable ruleset revision

## Assertions
List exact persisted rows/fields, broker settlement/redelivery state, emitted effect IDs and relevant logs/metrics. Avoid asserting only that a function returned nil.

## Evidence
- Exact command:
- Test name/package:
- Commit SHA / CI run:
- Expected result:
- Actual result / exit code:
- Skipped cases:
- Remaining gap:
