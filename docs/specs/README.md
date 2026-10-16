# Specifications

A specification defines required behavior, boundaries and acceptance criteria. It should be reviewable before implementation and testable afterwards. It is not proof that the behavior already exists.

## Documents
- [Feature specification template](feature-spec-template.md): API, event, persistence, concurrency, security and operational changes.
- [Cross-cutting invariants](invariants.md): properties changes must preserve.

## When to write or update a spec
Do so for event/API/schema/SDK changes; rule evaluation or activation semantics; transaction, dedupe, retry, outbox or ACK changes; tenant/auth changes; shard leases, fencing, ordering or recovery; new adapters, runtime parity claims, migrations or operational controls. A behavior-neutral refactor can link to its existing spec.

## Review gates
Reviewers should understand the problem, affected trust boundaries, canonical contract source, invariants under duplicates/retries/crashes, observable acceptance criteria, explicit non-goals and rollback implications.

## Lifecycle
Draft → review domain/security/compatibility → approve → implement against acceptance criteria → verify exact commit → update status and residual gaps. Keep intended behavior distinct from verified behavior.
