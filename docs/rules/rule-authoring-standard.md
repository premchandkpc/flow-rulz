# Rule authoring standard

## Required properties
- Use stable rule IDs and explicit priorities.
- Pin each deployed Service assignment to an immutable ruleset revision.
- Declare the input contract when the rule depends on a specific payload schema.
- Keep predicates and actions explicit, bounded, reviewable, and deterministic.
- Avoid hidden network calls, wall-clock dependencies, random values, or unbounded work in evaluation.
- Treat emitted actions as durable effect intents, not proof that a downstream side effect completed.
- Validate rule documents before activation and preserve the previous active revision if validation fails.

## Predicate review
- Verify every referenced payload path against supported path syntax and contract.
- Define missing/null behavior and type comparison semantics.
- Keep nesting and collection sizes bounded.
- Test boundary values, unexpected types, empty arrays, missing paths, and conflicting predicates.
- Do not assume JSONPath features that the current evaluator does not implement.

## Action review
For each action, document destination, payload, idempotency key, retry behavior, failure classification, and observability. Preserve tenant/event identity where required and never embed secrets in rule documents.

## Revision and rollout
1. Create a new revision instead of editing a published revision.
2. Validate schema and rule semantics.
3. Run deterministic unit tests and contract checks.
4. Review compatibility and side-effect changes.
5. Activate the explicit revision.
6. Verify runtime selection and effect delivery with observable evidence.
7. Keep rollback instructions that select a previously valid revision without rewriting history.

## Review checklist
- [ ] Schema and supported operators match implementation.
- [ ] Contract paths are valid and tested.
- [ ] Rule ordering and first_match / all_matches semantics are explicit.
- [ ] Failure and missing-data behavior are intentional.
- [ ] Actions are idempotent or downstream deduplication is specified.
- [ ] Tenant boundaries and sensitive data handling are reviewed.
- [ ] Tests cover match, non-match, edge cases, and error paths.
