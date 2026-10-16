# Rule authoring and lifecycle

This section explains rule design and review. The existing [rules reference](../rules.md) remains the field-level source for JSON shape, operators, predicate grammar and actions. The [ruleset guide](../ruleset.md) covers revision storage and assignment.

## Documents
- [Rule authoring standard](rule-authoring-standard.md): safe, deterministic rule design.
- [Rule lifecycle](rule-lifecycle.md): draft → validate → test → publish → assign → activate → observe/rollback.

## Runtime model
An event type selects relevant rulesets under the current rule-resolution model. A ruleset has an immutable revision and a mode such as first_match or all_matches. Rules contain predicates and actions. Evaluation produces effect intents; it does not prove external effects completed.

Ruleset revision, contract identity, Domain/Service assignment and subscriber routing are distinct concerns. A receiver subscription is not an execution assignment.

## Source-of-truth policy
Before documenting an operator, path syntax, action or failure behavior, verify it exists in the evaluator and has tests. Do not assume full JSONPath support or arbitrary action execution. Unsupported behavior stays a documented limitation until implemented.
