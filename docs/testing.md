# Change and test checklist

Use this checklist whenever changing a Domain, Service assignment, ruleset, contract, event envelope, API route, or worker.

## Configuration boundary

- [ ] Domain request contains only `name` and `services[]`; the server owns `id` and timestamps.
- [ ] Each Service pins one or more immutable `ruleset_id + revision` assignments.
- [ ] No configuration request or response exposes `tenant_id`, `partition_key`, or internal `tenant_scope`.
- [ ] Rule management resolves revisions from `__domain_configuration__`.
- [ ] Global configuration routes require admin scope or trusted control-plane proxy authentication; tenant-only credentials cannot mutate global configuration.
- [ ] Legacy scope migration fails on conflicting ruleset contents, active revisions, contract hashes, priorities, or activation policies instead of silently merging them.

## Runtime event boundary

- [ ] Event envelope carries `meta.tenant_id` and `meta.partition_key`; unknown `meta` fields survive Go/Rust decode-encode round trips.
- [ ] Shard identity is derived from both fields.
- [ ] Inbox/deduplication, execution IDs, audit records, and effects retain the event's tenant identity.
- [ ] Contract name/version/hash are validated before rule evaluation.
- [ ] Subscriber fan-out remains separate from rule evaluation.
- [ ] Delivery effects retain event identity and use stable idempotency keys.

## Required test layers

| Change | Minimum checks |
|---|---|
| Domain/Service API | `internal/adapters/http/handlers/domain_deployments_test.go`, `internal/services/domainservices` |
| Global auth boundary | `internal/adapters/http/middleware/middleware_test.go`, rule handler tests |
| Ruleset resolution | `internal/services/events`, `internal/services/rules`, immutable revision tests |
| Event/schema change | Go event validation tests, Rust engine/consumer tests, schema + fixture JSON parsing |
| PostgreSQL persistence/migration | repository unit tests plus tagged integration/E2E suites that apply all migrations |
| Go worker/runtime | `go test -race -count=1 ./internal/...` and integration/E2E tags |
| Rust worker | `cd rust-worker && cargo fmt --all && cargo test --all-targets && cargo clippy --all-targets -- -D warnings` |
| API/console contract | generated-code compile check and React console tests/build |

## CI commands

Run the repository's normal validation before marking a PR ready:

```bash
make check-fmt
go test -race -count=1 ./internal/...
go test -race -count=1 -tags=integration ./tests/integration/...
go test -race -tags=e2e -count=1 ./tests/e2e/...
cd rust-worker
cargo fmt --all
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
```

CI also builds application/worker images, validates Compose, checks all JSON schemas and `data/examples/*.json`, compiles generated code, and builds/tests the React operations console.

## Known rollout boundary

The Domain API persists Service assignments and validates pinned revisions. A complete reconciler that automatically projects every Domain revision into both Go and Rust worker pools is not yet implemented. Until it exists, deployment automation must synchronize the worker's Service ID, deployment revision, and ruleset list with the Domain document.
