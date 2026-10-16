# Rule lifecycle

This is a conceptual lifecycle, not a claim every step is automated. Confirm the supported API and storage behavior before using it as a runbook.

## 1. Draft
Create a candidate ruleset/revision without changing the active deployment. Record owner, purpose, event type, contract, mode and intended actions.

## 2. Validate
Validate required fields, unique IDs, supported operators/path syntax, action shape and contract identity. Reject unsupported semantics rather than silently accepting them.

## 3. Test
Test match/non-match, missing/null fields, unexpected types, boundary values, ordering, action output and failure classification. Use fixtures from the same event contract as producers.

## 4. Publish immutable revision
Persist a new revision and retain prior revisions for audit/rollback. Record the revision and canonical schema hash used for validation.

## 5. Assign
Pin the exact ruleset ID and revision in the intended Domain/Service configuration. Do not confuse execution assignment with subscriber registration. Review effects of removing or deactivating an assignment.

## 6. Activate and verify
Use the supported control-plane path, then verify that worker deployment uses the intended Service and ruleset revisions. The Domain API persists assignments, but automatic projection/reconciliation into every worker pool is not complete; deployment automation must keep configuration synchronized.

## 7. Observe
Inspect selected revision, execution outcomes, quarantine/retry behavior, outbox backlog and effect failures. A committed execution is not proof a downstream effect succeeded.

## 8. Roll back
Prefer a new configuration change pointing to a previously validated revision. Do not rewrite history. Rollback does not reverse events already processed or external effects already delivered.

## Evidence to retain
Ruleset ID/revision; contract name/version/hash; Domain/Service and deployment revision; validation/test run and commit; activation timestamp; rollback target; replay or side-effect implications.

If a step is unsupported by the current runtime, record a gap instead of inventing an operational procedure.
