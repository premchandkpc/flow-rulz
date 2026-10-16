# BDD scenarios: <feature>

- **Spec:** 
- **Implementation:** 
- **Status:** Proposed | Automated | Verified | Gap
- **Test path:** 
- **Last verified commit:** 

## Scenario: <observable behavior>
**Given** <initial configuration and durable state>

**And** <broker, database, lease or worker state>

**When** <event or failure is triggered>

**Then** <observable API/database/broker result>

**And** <expected effects and ordering>

## Failure variants
- Duplicate delivery:
- Transaction rollback:
- Crash/restart:
- Timeout or dependency unavailable:
- Unauthorized or cross-tenant access:
- Maximum retries / poison event:

## Evidence
- Test command:
- Infrastructure used:
- Expected result:
- Actual result:
- Known gap:
