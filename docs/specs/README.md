# Specifications

Specifications define what FlowRulz must do and the conditions under which behavior is correct. They do not prescribe implementation details unless a constraint is necessary for correctness, compatibility, or security.

## Files
- [Feature specification template](feature-spec-template.md): use for new features and behavior changes.
- [Core invariants](invariants.md): cross-cutting properties changes must preserve.

Every substantive spec should state the problem, goals and non-goals, actors and trust boundaries, contracts, invariants, acceptance criteria, failure and recovery behavior, observability, rollout/rollback, verification evidence, and open questions.

## Lifecycle
1. Draft the spec before implementation.
2. Review domain and compatibility implications.
3. Translate acceptance criteria into Given/When/Then scenarios.
4. Link each criterion to tests in the verification matrix.
5. Update the spec when implementation changes, preserving the distinction between intended and verified behavior.

A spec is not proof that behavior exists. Implementation and tests establish that.
