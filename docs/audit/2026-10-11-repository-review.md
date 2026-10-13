# Repository review — 2026-10-11

## Scope and baseline

Reviewed the main-branch snapshot at commit ce533ad522a5616a9c5e0118d7d37aba00da97cc. The review covered the repository tree, root commands/configuration, Go process entrypoints, domain/ports/services, worker/key-queue/lease/scheduler flow, storage adapters, HTTP routing and middleware, code generation, deployment scripts, test layout, CI, and current architecture/audit documents. This is a source review, not proof from a production-like deployment. All validation results must come from actual workflow runs.

## Changes made in this review branch

- Fixed emitted child-event routing: worker effect publishing is now wired through the effect router; the router publishes to the same events.shard.N subject family used by the stream/consumer and uses the configured shard count rather than a hardcoded 4096/topic-derived broker subject.
- Added a regression test for the actual broker subject contract and configurable shard count.
- Aligned api/rule-schema.json with emit_event actions and valid composite predicate shapes; the compiler now rejects ambiguous multiple-composite and empty-composite predicates, with regression tests.
- Fixed the standalone SDK build discovered by expanded CI: moved the duplicate Effect declaration out of envelope.go, aligned its JetStream dependency with the supported parent NATS module, and added sdk/go/go.sum.
- Replaced the rules service N+1 ListActiveRules query with the existing activation repository List method so repository errors are propagated rather than silently skipped.
- Removed a redundant execution status update inside the event transaction by persisting the final completed status once.
- Added POST /v1/rules/{ruleSet}/revisions so documented/example rule deployment uses a real registered route. It accepts one bounded JSON or YAML rule document, normalizes YAML to JSON, validates that the body rule_set matches the route, and compiles/activates through the use-case interface.
- Bounded HTTP JSON/YAML body reads to 1 MiB and rejected extra JSON values/multiple YAML documents. Added handler regression tests for YAML, route/body mismatch, and multiple YAML documents.
- Refactored codegen and migrate CLIs to return errors through deferred cleanup. Migration CLI now gives an explicit error for unsupported down/status commands.
- Corrected example deployment scripts that called the nonexistent bin/codegen-yaml command, fixed the workflow-definition route, and made curl fail on HTTP errors. Rule files and generated artifacts remain checked-in source/output; codegen CLI operates on registered contracts.
- Aligned the local Compose environment so the worker may use its fake destination in development mode instead of exiting because production mode requires EFFECT_DESTINATION_URL.
- Removed the unused GRPC_ADDR configuration field and default gRPC port mapping because the API does not start a gRPC listener. Changed the native HTTP default to 127.0.0.1:8080 and bound local Compose host ports to loopback, reducing accidental exposure while tenant auth remains unfinished.
- Rewrote docs/operations.md to remove stale claims about hard-coded worker settings, missing shard enforcement, gRPC, and event subjects; documented the actual runtime flow and open outbox/security risks.
- Removed checked-in compiled root/sample executables and added ignore entries for those build outputs. Removed a hidden duplicate ecommerce `.generated` tree containing malformed, unused Go output; retained the valid `generated/go` sources and added an ignore rule for temporary `.generated` directories.
- Expanded Go CI to cover root commands/packages, both independent SDK modules, and all three example modules, plus formatting, JSON schema syntax, and shell syntax checks.
- Replaced AGENTS.md with current source-backed instructions and a file-by-file map. Replaced ARCHITECTURE_MAP.md and docs/README.md with current flow/route/guarantee statements, added the root README, and replaced the generic SECURITY.md template.
- Aligned Makefile targets to implemented codegen/migration CLIs, added non-mutating format verification (including correctly escaped shell variables), removed nonexistent E2E/protobuf/rollback recipes, and stopped the clean task from clearing the user's entire Go module cache. Corrected the check-fmt recipe after CI caught GNU Make expanding `$files` before the shell; it now uses `printf` with escaped shell variables.

## Open findings requiring follow-up

### P0 — No authenticated tenant boundary

Evidence: internal/adapters/http/middleware/middleware.go still extracts X-Tenant-ID or tenant_id query parameter and falls back to default. There is no authN/authZ layer that validates tenant identity and authorizes each resource operation.

Impact: callers can choose a tenant identifier. Tenant scoping must not be treated as a security boundary.

Next step: add authentication from a trusted provider (for example verified JWT claims or mTLS identity), derive tenant identity only from verified claims, authorize all resource IDs by tenant, reject body/query tenant mismatches, and add cross-tenant denial tests. Until then, keep the API private/local.

### P1 — Outbox transitions are not fully claimant-fenced

Evidence: ports.OutboxRepository.ScheduleRetry and Quarantine do not accept claimant identity, unlike MarkDelivered. The current effect service dispatches sequentially with a fixed claim TTL; some state-transition/quarantine persistence errors are logged instead of returned.

Impact: expired/competing claimants can race updates, slow sends can exceed claim TTL, and the process may hide a failed durable status update.

Next step: pass claimant/lease token through every transition, make all transitions conditional on current claim ownership, propagate durable-state errors, add per-effect timeouts and bounded concurrency only after claim semantics are correct, and test claim expiry/competing publishers/crash recovery.

### P1 — Transaction port leaks implementation details

Evidence: ports.Transaction exposes transaction context and fencing mutators; TxRepos groups many repository interfaces; storage construction type-asserts concrete transaction implementations.

Impact: use cases know too much about transaction internals and adapter conformance is harder.

Next step: design a consumer-owned unit-of-work interface and narrower per-use-case repository sets. Preserve fencing and all-or-nothing inbox/execution/outbox semantics with regression tests before deleting legacy interfaces.

### P1 — Adapter tests do not yet prove production storage semantics

Evidence: tests/contract uses in-memory repository implementations; tests/integration also uses mocks/in-memory repositories. The default CI suite is not a live PostgreSQL/NATS crash/recovery test.

Next step: run shared contract suites against real PostgreSQL and Mongo transaction-capable configurations; add tests for uniqueness races, rollback, ACK loss, lease expiry while paused, broker restart, outbox claim expiry, and destination timeouts. Mark unsupported backend semantics explicitly.

### P1 — Request-boundary and security work remains

This review adds request size limits and strict single-document decoding, but it does not add authentication, authorization, trusted tenant identity, consistent per-tenant authorization across all endpoints, or outbound destination allowlisting. Those remain separate required changes.

### P2 — Workflow service has placeholders

Evidence: internal/services/workflow/service.go includes definition methods that return not-implemented errors, while HTTP definition handlers receive WorkflowDefinitionRepository directly.

Next step: either implement these methods with a clear repository dependency and tests or remove the misleading placeholders.

### P2 — Composition code is duplicated

Evidence: cmd/api and cmd/worker use internal/storage, while internal/factory contains a broad Mongo-oriented factory with overlapping wiring.

Next step: verify consumers and tests, choose one supported composition root, migrate callers if any, then narrow/remove the legacy factory. It was not deleted by this review.

### P2 — Operational readiness is overstated by config/names

Evidence: /health checks only process liveness; cmd/api has no gRPC listener; cmd/migrate supports up only. Loopback defaults and Compose port bindings reduce accidental local-network exposure, but do not replace authentication.

Next step: introduce dependency-aware readiness; either implement gRPC or remove misleading config; add reversible migrations only after explicit design/tests.

### P2 — Performance and retention need measurements

Evidence: SQL outbox Insert performs one insert per effect; sequential effect delivery, table retention, hot-table constraints/indexes, p99, and throughput have not been proven with reproducible benchmarks.

Next step: benchmark realistic event/effect sizes and contention, inspect actual query plans, then make evidence-based changes. Add retention/partitioning and schema constraints through forward-only numbered migrations where appropriate.

## Verification record

This review was authored against main commit ce533ad522a5616a9c5e0118d7d37aba00da97cc; the change branch adds the source changes above. Remote file edits were committed through the GitHub API. No local test run is claimed. An early widened-CI run passed root build/tests/vet but exposed the original SDK's duplicate Effect declaration; that defect and its module checksums were fixed. The current branch also includes the separate sdk/go/flowrulz module added to main while review was in progress. The final widened Go CI and default CodeQL runs must be checked at the current PR head before merging. A green build does not close the open security/storage findings above.

## Recommended order

1. Trusted auth and tenant authorization.
2. Claimant-fenced outbox transitions with failure propagation and duplicate/expiry tests.
3. Live SQL/Mongo contract suites plus PostgreSQL/NATS failure-injection tests.
4. Unit-of-work/port redesign and removal of duplicate composition.
5. Readiness, schema/retention review, benchmarks and observability SLOs.
