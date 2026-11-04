# Event transport decoupling

Reviewed against main commit `e7deee6f474e4566f9b6f9f6e78876f2481702c2`.

## Problem and change

Previously, the effect router and scheduler both selected event shards and broker base subjects. Their `EventPublisher` port also required batch publishing and connection closure, neither of which these callers used. The worker accepted a generic broker consumer, then asserted a separate locally duplicated sharded interface at runtime.

The revised flow is:

1. Effect routing decodes a child envelope; scheduling constructs an envelope.
2. Both call `ports.EventPublisher.PublishEvent(ctx, envelope)`.
3. `internal/adapters/nats/event_publisher.go` validates, encodes, and selects the shard using `EventEnvelope.VirtualShard`.
4. The existing NATS publisher maps legacy or configured ruleset subjects and awaits broker publication.

`cmd/worker` composes one envelope publisher with the stream base subject and shard count, shared by both callers. It still owns the underlying connection. The effect router takes explicit command, service and scheduling senders, replacing positional optional senders. Worker construction requires `ports.ShardedConsumer` directly; the duplicate interface, runtime type assertion, unused consumer field and unused shard-count field/argument are removed.

The scheduler copies headers before adding its idempotency key, preventing mutation of the persisted event object.

## Compatibility and failure behavior

- Internal constructor signatures change; all repository callers, including the tagged E2E harness, are updated. Public SDK modules and API routes do not change.
- The canonical envelope serializer remains the source of truth, including identity under `meta`. Child-event bytes are re-encoded, so formatting is not byte-for-byte preserved; canonical fields and metadata are preserved.
- Scheduled envelopes now receive the same required-field validation as child events before reaching the broker. Malformed stored schedules follow the existing retry/terminal-failure path.
- Broker errors propagate to existing outbox/scheduler retry policy. Claim tokens, transaction boundaries, ACK timing, fencing, tenant routing, and durable state transitions are unchanged.
- No schema, migration, environment or Compose changes are required. Rollback is a code rollback.
- Retaining subject/shard parameters in the application port was rejected because it would preserve the coupling. A new generic messaging framework is unnecessary: one envelope method plus the existing adapter suffices.

## Acceptance and verification

- Effect routing and scheduling have no broker subject, shard-count or `PublishToShard` dependency.
- Worker construction accepts a fake exposing only the four sharded-consumer capabilities.
- Adapter tests cover default/custom routing, canonical envelope and metadata preservation, ruleset subject mapping, caller context, invalid input/encoding, and broker errors.
- Use-case tests cover child publication/error propagation, scheduler claim/retry/terminal settlement, and stored-header immutability.
- Root build, race tests and vet are the applicable checks. Tagged E2E compilation checks constructor wiring; live NATS/PostgreSQL execution requires container infrastructure.

## Remaining candidates

These are identified boundaries for separate changes, not completed refactors:

| Location | Coupled responsibilities | Next boundary |
|---|---|---|
| `web/console/src/App.tsx` | API requests, settings persistence, polling, simulations, rule editing and rendering | Extract an API client, polling hooks and feature components. Move nested component definitions to module scope to avoid changing component identity each App render. |
| `internal/codegen/generator.go` | Safe output-file creation, multiple language templates, generated runtime and evaluator emission | Separate filesystem output from language/rendering responsibilities, preserving existing path-safety checks and generated-code tests. |
| `internal/services/events/service.go` | Contract validation, ruleset resolution, deduplication and execution/outbox persistence | Isolate routing/validation with narrow inputs; keep lease assertion, inbox, execution, subscriber intents and outbox within the same transaction. |

Do not remove a package solely because it looks old or has few callers. SDK modules, generated code, examples and operational tools have independent consumers and compatibility requirements.

Observed local results for this change: `go build ./cmd/...`, `go test -race -count=1 ./...`, `go vet ./...`, and `go test -tags=e2e -run '^$' ./tests/e2e` passed. Changed Go files are gofmt-clean and `git diff --check` passed. The E2E command compiles only; Docker is unavailable here, so live broker/database E2E and container builds were not run. Nested SDK/example modules were not changed and were not tested. Remote CI is separate from these local results.
