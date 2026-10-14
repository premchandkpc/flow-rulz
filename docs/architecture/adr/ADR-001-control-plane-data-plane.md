# ADR-001: Separate control plane from event data plane

- Status: Accepted
- Date: 2026-10-14

## Context

The API and worker had overlapping responsibilities: the API could publish business events, while the worker consumed and evaluated them. This couples management availability to broker connectivity and makes the API a second event-ingress path with separate validation and operational behavior.

## Decision

1. `cmd/api` is the control plane. It manages domains, contracts, rule revisions, workflow definitions, and registered provider/service capabilities. It may expose read-only execution and trace views.
2. `cmd/api` does not publish or consume business events and does not own the event-processing loop.
3. Producers publish business events directly to NATS JetStream using the platform event envelope and subject/shard contract.
4. `cmd/worker` is the core event data plane: consume, validate, resolve a published revision, evaluate flow instructions, persist durable transitions and output intents, and ACK only after durable commit.
5. The worker process is stateless with respect to authoritative business state. Execution progress, inbox/deduplication, outbox effects, waits, schedules, retries and workflow resumptions are durable.
6. Flow definitions use a validated and versioned instruction set. Keywords such as `when`, `if`, `emit`, `call`, `wait` and `schedule` describe behavior; they are not arbitrary executable code.
7. HTTP event ingestion is not part of the management API. The Go SDK must use NATS for event publication.

## Consequences

- The API no longer needs a NATS publisher merely to serve configuration requests.
- Producer credentials and NATS subject permissions become explicit ingress security boundaries.
- The HTTP SDK transport is rejected with an actionable configuration error instead of targeting a removed endpoint.
- A future dedicated receiver, if needed, must be a separate deployable with its own authentication, validation and backpressure policy. It must not be embedded in the control-plane API.
- Schedule and wait definitions still require a separate durable runtime implementation; this ADR does not claim those workflow keywords are fully implemented.

## Required invariants

- The API composition root cannot construct an event consumer or event publisher.
- The management router does not register business-event ingestion routes.
- A worker ACK is sent only after the durable execution transaction commits.
- Configuration revisions are immutable and executions pin the exact revision used.
- External effect delivery is at-least-once and relies on stable effect identity plus receiver-side idempotency.
