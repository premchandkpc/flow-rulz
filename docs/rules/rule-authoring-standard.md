# Rule authoring standard

Exact JSON fields and supported operators are defined by the [rules reference](../rules.md) and validated by the current implementation.

## 1. Structure and identity
- Use stable ruleset IDs and unique rule IDs within each ruleset.
- Set explicit priorities and confirm tie behavior in first_match mode.
- Pin an input contract when evaluation depends on a known payload shape.
- Publish a new immutable revision for semantic changes; do not mutate a published revision.
- Keep names, descriptions and tags useful for review. Never put secrets in rule metadata.

## 2. Predicate design
- Use only implemented operators and path syntax; do not assume full JSONPath.
- Define expected type, null/missing behavior and boundary semantics.
- Keep nesting, collection sizes and evaluation work bounded.
- Prefer explicit all/any/not groups over ambiguous expressions.
- Do not rely on implicit coercion unless the evaluator defines it.
- Test match, non-match, missing path, null, unexpected type, empty collection, boundary values and conflicting predicates.

## 3. Actions and side effects
- Treat actions as declarative effect intents, not inline network calls from the evaluator.
- Document destination/topic, payload mapping, stable effect ID/idempotency key, retry classification and observability.
- Keep effect delivery separate from rule evaluation so a failed delivery does not rerun the decision.
- Preserve tenant/event identity where required. Never treat producer-controlled routing metadata as authorization.
- Determine how the receiver handles duplicate requests and ambiguous timeouts.

## 4. Contract compatibility
- Event contract name, version and schema hash must agree with the registered/pinned contract where validation applies.
- Validate referenced payload paths against the declared schema.
- Publish a new contract version for semantic changes and review dependent rulesets.
- Keep valid and invalid fixtures so producer, evaluator and SDK behavior stays aligned.

## 5. Publish and activate
1. Create a candidate revision.
2. Validate structure, operators, paths and contract identity.
3. Run deterministic tests and contract fixtures.
4. Review idempotency, tenant safety and operational impact of actions.
5. Publish the immutable revision.
6. Update the explicit Domain/Service assignment or supported activation configuration.
7. Verify the worker actually selected the expected revision.
8. Record rollback target and evidence.

Do not assume a saved Domain assignment automatically updates every worker pool; current documentation identifies automatic reconciliation as incomplete.

## Review checklist
- [ ] Schema, operators and path syntax match source.
- [ ] Rule order and first_match/all_matches semantics are clear.
- [ ] Missing/null/type behavior is tested.
- [ ] Contract identity is correct.
- [ ] Actions are bounded, observable and safe to retry.
- [ ] Stable effect identity and destination dedupe/reconciliation are defined.
- [ ] Tenant/internal routing metadata cannot bypass policy.
- [ ] Positive, negative, boundary and error tests exist.
- [ ] Activation, rollback and operational ownership are documented.
