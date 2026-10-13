# Architecture and code-quality review — 2026-10-11

## Scope and status

Reviewed the stacked remediation branch `refactor/shared-http-validation-and-architecture-audit`, currently associated with PR #6. This is a source-level architecture review plus targeted fixes, not a claim that every package has been executed locally. CI is being used to validate the pushed commits.

**Important baseline limitation:** this branch is stacked on PR #5's older base. `main` has advanced with JWT authentication and related changes that are not yet incorporated into this branch. Do not merge this branch directly to `main` without first reconciling that divergence and reviewing the authentication changes. The current PR workflow previously filtered to PRs targeting `main`, which meant stacked PRs received no Go CI; the filter has been removed so stacked PRs are checked too.

## Targeted fixes made

### 1. Interface segregation and transaction boundaries

Before this change, `ports.TxRepos` exposed inbox, activation, rule, execution, outbox, shard lease, workflow, scheduler, batch, and quarantine repositories to every transaction consumer. Event processing and rule activation therefore depended on a broad aggregate even though they use different subsets.

Changes:
- Replaced the broad transaction bundle with `ports.EventTxRepos` and `ports.RuleTxRepos`.
- Added separate `EventTxRepositoryFactory` and `RuleTxRepositoryFactory` contracts.
- Added SQL and Mongo adapter-owned constructors in `internal/adapters/{sql,mongo}/transaction_repositories.go`.
- Moved concrete transaction type assertions and driver-specific repository wiring out of `internal/storage`.
- Updated API/worker composition to request only the bundle required by the use case.
- Added type-guard tests for both adapter factories.

This applies Interface Segregation Principle: event processing does not receive repositories used only by other use cases. It also improves Dependency Inversion because the use cases depend on narrow ports while driver-specific construction stays in the adapters.

### 2. Tenant isolation for workflow persistence

- Workflow repository reads now require both tenant ID and workflow ID.
- SQL and Mongo updates include tenant ID in the update predicate as well as the optimistic version condition.
- Correlation-based workflow reads require tenant scope.
- The workflow service rejects empty tenant scope and passes the verified tenant to repository methods.
- HTTP handlers pass tenant identity into the workflow service rather than fetching by global ID and relying only on an after-the-read check.
- Added HTTP regression tests that verify tenant identity is passed into workflow use cases.

The handler's post-read tenant check remains useful as defense in depth, but it is no longer the only boundary.

### 3. Reusable HTTP parsing

- `handlers/utils.go` centralizes bounded JSON/YAML body reading and strict single-document decoding.
- `handlers/query_params.go` centralizes validated pagination bounds.
- Existing handlers reuse these helpers instead of implementing slightly different parsing rules.

Do not extract every repeated `WriteError` call into a generic framework. Shared code is useful when it preserves a stable invariant; over-abstraction merely hides endpoint-specific behavior.

## SOLID assessment

| Principle | Assessment | Notes |
|---|---|---|
| Single Responsibility | Improving, not complete | Use cases are separate, but some HTTP handlers still contain substantial validation, authorization checks, and response mapping. Keep transport concerns in handlers and business state rules in services/domain. |
| Open/Closed | Moderate | Ports allow adapter substitution. New backend support still requires edits to storage composition, which is normal for a composition root. |
| Liskov Substitution | Needs conformance tests | SQL and Mongo must agree on not-found, duplicate, conflict, cancellation, claim expiry, and state-transition behavior. Interfaces alone do not prove substitutability. |
| Interface Segregation | Improved | Event and rule transaction repository bundles now expose only the dependencies each use case uses. Review other broad interfaces as use cases evolve. |
| Dependency Inversion | Improved, with debt | Services consume ports; concrete transaction construction is now owned by adapters. The legacy `internal/factory` still imports concrete adapters and overlaps with storage wiring. |

## Coupling and isolation findings

### Addressed in this branch
- Broad transaction repository bundle split into use-case-specific bundles.
- Concrete SQL/Mongo transaction assertions moved out of the storage composition package.
- Workflow get/update/correlation access paths now carry tenant scope.
- Shared HTTP request parsing and pagination behavior.

### Remaining work
1. **Synchronize with current main.** The branch's base predates the latest JWT-authentication changes. Reconcile those changes before merge; do not let an older middleware version overwrite newer authentication work.
2. **Retire or consolidate `internal/factory`.** It overlaps `internal/storage` and constructs a Mongo-specific stack. Verify callers and tests before removing it; do not keep adding new wiring methods to both places.
3. **Repository conformance suite.** Run the same contract cases against SQL and Mongo, including duplicate insert, no rows, cross-tenant read/update, optimistic conflict, expired claims, cancellation conflict, and transaction rollback.
4. **Transaction contract review.** `ports.Transaction` still includes context and fencing-token methods. Verify each is needed by the use case; if a concern is only used by one adapter protocol, narrow the abstraction rather than growing it.
5. **Effect/quarantine atomicity.** Outbox state settlement and quarantine persistence are still separate operations. Failure-injection and repairability tests are required.
6. **Operational semantics.** Scheduler publish failures still need bounded retry/backoff; effect delivery remains sequential under a fixed claim TTL. These are correctness/availability concerns, not folder-layout problems.
7. **Schema evolution.** Keep migrations forward-only and test upgrades from prior schemas. Confirm SQL/Mongo parity for every new field and status transition.
8. **Security validation.** Keep the API private until the current main JWT/authentication boundary is reconciled with tenant-scoped repository operations and cross-tenant denial tests pass.

## Folder and naming guidance

Current boundaries are broadly reasonable; do not move files only to make the tree look tidy.

- `cmd/{api,worker,codegen,migrate}`: process entrypoints and composition only.
- `internal/domain`: business entities, value types, state transitions, invariants, and domain errors. Consider splitting the broad `types.go` into cohesive files such as `event.go`, `rule.go`, `workflow.go`, and `outbox.go` in a separate low-risk change.
- `internal/ports`: consumer-facing interfaces. Split by concern when files become difficult to navigate, e.g. `transaction.go`, `repository.go`, `services.go`, and `runtime.go`; avoid one interface per file as a ritual.
- `internal/services/<usecase>`: orchestration and business workflow.
- `internal/adapters/{http,sql,mongo,nats,redis,effects,memory}`: protocol/storage implementations and translation.
- `internal/storage`: backend selection and process composition. It should not type-assert concrete transactions or contain business logic.
- `internal/factory`: legacy overlap with storage; do not expand it.
- `internal/runtime/{keyqueue,shard,scheduler}`: bounded execution, lease management, and scheduling.
- `api/` and `migrations/`: versioned external schemas and forward-only database changes.
- `sdk/` and `examples/`: independent Go module boundaries; CI must test each module separately.

Naming consistency issue to address later: SQL's rule adapter is named `Repository`/ `NewRepository`, while other adapters use domain-specific names such as `WorkflowRepository`. Prefer `RuleRepository`/ `NewRuleRepository`, but perform the rename with all call sites and tests in one dedicated change. Do not mix a broad rename with tenant-security fixes.

## Reusable-code policy

Promote code into a shared helper when:
- at least two callers need the same invariant;
- the helper has one clear responsibility and stable input/output;
- it reduces inconsistent behavior without hiding important business decisions;
- it can be tested independently.

Good candidates already in place: bounded request-body decoding, strict JSON/YAML single-document decoding, and pagination parsing.

Do not centralize code just because it looks similar when SQL and Mongo semantics differ. Keep backend-specific query construction and claim semantics inside each adapter, then enforce parity through shared contract tests.

## Verification gates

Before marking this work merge-ready:
- Go formatting check;
- root build and race-enabled tests;
- `go vet`;
- both SDK modules and all example modules;
- CodeQL/security scanning;
- SQL/Mongo repository conformance tests;
- migration upgrade tests;
- cross-tenant negative tests for workflow, quarantine, scheduled events, and batches;
- re-review against current `main` after the JWT-authentication changes are incorporated.

A green compile is necessary, not sufficient, for production readiness.
