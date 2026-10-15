# FlowRulz Architecture Map

This is a source-backed map of the current implementation, not a target-state promise. Runtime code and tests take precedence over diagrams, roadmaps, and dated audit snapshots.

## Runtime topology

~~~mermaid
flowchart LR
  Producer[Producer / Go SDK] -->|events.shard.N| NATS[NATS JetStream]
  NATS --> Worker[cmd/worker]
  Worker --> App[application.Worker]
  App --> Queue[runtime/keyqueue]
  App --> Lease[runtime/shard lease manager]
  Lease --> LeaseDB[(Shard lease repository)]
  Queue --> Events[services/events]
  Events --> Tx[Transactional storage]
  Tx --> Inbox[(Inbox)]
  Tx --> Rules[(Rule revisions and activations)]
  Tx --> Executions[(Executions)]
  Tx --> Outbox[(Outbox effects)]
  Domains[(Global Domain configuration and subscriber registry)] --> SubRouter[Generic subscriber router]
  SubRouter --> Tx
  Outbox --> Effects[services/effects]
  Effects --> Router[Effect router]
  Router -->|emit event| NATS
  Router -->|emit topic| NATS
  Router -->|command| Commands[commands.destination.name]
  Router -->|service event| Services[services.tenant-hash.service-hash]
  Commands --> Receiver[Durable JetStream receiver]
  Services --> Receiver
  Scheduler[runtime/scheduler] --> Scheduled[(Scheduled events)]
  Scheduler -->|events.shard.N| NATS
  API[cmd/api] --> HTTP[HTTP router and handlers]
  HTTP --> Services[Use-case services]
  Services --> Storage[internal/storage]
  Storage --> PG[(PostgreSQL default)]
  Storage --> Mongo[(MongoDB option)]
~~~

## Process startup

### API — cmd/api/main.go

1. Load and validate configuration.
2. Open storage using internal/storage and initialize the selected backend/migrations.
3. Create rule compiler and the rule, workflow, batch, and quarantine services.
4. Create the HTTP router for configuration and management/read-only views. The API does not construct a NATS publisher and does not register `POST /v1/events/{eventType}`.
5. Serve HTTP and shut down when the process context is cancelled.

The API starts HTTP only and its readiness checks cover the database, not NATS. No gRPC listener/configuration exists. The default HTTP bind is loopback; Compose publishes host ports on 127.0.0.1. The /health route is liveness only. Event producers publish directly to NATS JetStream; the worker owns consumption and event processing.

Security contract:
- In development/test, tenant context may be supplied using X-Tenant-ID or tenant_id for local use only.
- In non-development environments, TenantExtractor requires FLOWRULZ_TRUSTED_PROXY_TOKEN, validates X-Proxy-Auth using constant-time comparison, and accepts tenant identity only from X-Authenticated-Tenant-ID. It fails closed when the token is missing or invalid.
- This is not first-party authentication. The trusted proxy must authenticate/authorize callers, strip inbound trusted headers, set the tenant header itself, and keep the API inaccessible to direct untrusted traffic.
- Workflow definitions are global platform configuration and are read-only through the tenant-facing API. Quarantine GET/replay/delete operations are tenant-scoped at repository query time. New effect quarantines inherit tenant identity from the event through the outbox, and legacy records are backfilled from authoritative outbox/execution data where available; orphan records remain unscoped. Do not expose the API to untrusted multi-tenant traffic until source tenant propagation, proxy authentication, and cross-tenant negative tests are verified.

### Worker — cmd/worker/main.go

1. Load config, start observability, and open/initialize storage.
2. Construct a NATS JetStream consumer and a separate publisher adapter. The stream accepts legacy events.shard.N subjects plus events.ruleset.<sha256(event-type)>.shard.N subjects. When NATS_RULESETS is configured consistently across API, worker, and SDK producers, the adapter maintains one durable consumer per configured ruleset and owned shard; unlisted types keep legacy routing.
3. Construct the compiler/evaluator, transactional event service, external sender, generic subscriber router and effect router. Subscriber matching is a separate routing policy, not part of rule evaluation.
4. Construct batch service, shard lease manager, scheduler and application.Worker.
5. Start scheduler/worker loops; on shutdown stop loops and close consumer, storage, and observability.

The effect router is part of runtime wiring, not an optional helper: emitted child events must be published to events.shard.N using the same NUM_SHARDS as the consumer. Domain and ruleset configuration use the internal global configuration scope; runtime tenant identity remains on each event. Generic effects publish to effects.<topic>; command effects publish to commands.<destination>.<name>; explicit service_event effects and subscriber-router deliveries publish to scope-scoped services.<scope-hash>.<subscription-hash> subjects (global scope for new subscriptions, legacy tenant scope for old records). The generic subscriber router resolves matching active tenant subscriptions and records one stable outbox intent per subscriber in the same transaction as a successful active-ruleset execution. It does not evaluate rules or contain subscriber-specific business logic. HTTP callback delivery is not supported. LeaseManager targets ceil(NUM_SHARDS/WORKER_REPLICAS) leases per worker. WORKER_REPLICAS defaults to 1 for single-worker local deployments and must be set to the actual fixed replica count on every multi-worker deployment. This makes the assumption explicit and configurable, but does not provide dynamic membership or coordinated rebalancing.

### CLI processes

- cmd/codegen is a DB-backed command that retrieves a registered contract and generates Go, Java, Protobuf or JSON Schema artifacts. It uses flags rather than subcommands; it is not a YAML-input generator.
- cmd/migrate currently supports up only. There are no implemented down/status operations.

## Event processing sequence

~~~mermaid
sequenceDiagram
  participant P as Producer
  participant N as NATS JetStream
  participant W as Worker and key queue
  participant S as Event service
  participant D as Transactional storage
  participant O as Outbox dispatcher
  P->>N: publish EventEnvelope to events.shard.N
  N->>W: deliver event for owned shard
  W->>W: validate envelope and obtain fencing token
  W->>S: Process(envelope, token, shard, worker)
  S->>D: begin transaction and assert lease/fencing
  S->>D: inbox lookup/insert
  S->>D: resolve active rulesets and contract bindings
  S->>S: validate contracts and evaluate rules
  S->>D: write execution and rule-declared effects
  S->>D: match active tenant subscriptions through generic router
  S->>D: write subscriber delivery intents
  S->>D: mark inbox committed
  S->>D: commit
  S-->>W: committed execution
  W->>N: ACK after successful commit
  O->>D: claim pending effects
  O->>N: publish declared child-event, command, generic or service_event effect
  O->>D: settle or retry effect state
~~~

The event transaction contains fencing validation, inbox deduplication, ruleset resolution/evaluation, execution persistence, rule-declared outbox writes, subscription matching through the generic router, subscriber delivery intents, and inbox commit state. Subscription routing is separate from rule evaluation but shares the transaction so a committed event cannot lose its delivery intents. Events with no active ruleset do not trigger subscriber delivery. A conflicting inbox insert with no corresponding execution must fail closed; never create a second execution to hide inconsistent state.

## Child-event routing contract

- Broker base subject is events.
- Legacy subjects remain events.shard.N for unlisted rule sets and older producers.
- Configured rule sets use events.ruleset.<sha256(event-type)>.shard.N. Hashing keeps arbitrary event types inside one NATS subject token.
- Set the same comma-separated NATS_RULESETS in API, worker, and SDK producer environments. Each configured rule set gets a distinct durable consumer per owned shard. Filters do not overlap, which is required by WorkQueue retention.
- Ordering is preserved within a ruleset/shard stream. Independent ruleset consumers do not promise global ordering between different event types that share a partition key; use a shared shard consumer if cross-ruleset order is a business invariant.
- Publisher and consumer adapters are composed separately; application services depend on EventPublisher and ShardedConsumer ports rather than a combined broker implementation.
- N is EventEnvelope.VirtualShard(NUM_SHARDS).
- EventEnvelope.Type carries business event type such as payment.requested; it is not part of the configured broker subject.
- The effect router and scheduler must use the same base subject and shard count as the stream/consumer. A publish to a different topic does not reach consumers if it is outside the stream subject filter.
- Generic emitted effects and commands publish through NATS JetStream to `effects.<topic>` and `commands.<destination>.<name>`. Explicit ruleset-declared `service_event` effects and subscriber-router deliveries publish to the scope-scoped `services.<scope-hash>.<subscription-hash>` subject (global scope for new subscriptions, legacy tenant scope for old records). Receivers deduplicate by the stable effect ID.
- Delivery is at-least-once. Receiver idempotency depends on durable consumer configuration and deduplication by stable effect ID; the overall system must not be described as exactly-once.

## Package and file responsibilities

| Package/path | Responsibilities |
|---|---|
| cmd/api | API composition root and lifecycle |
| cmd/worker | Worker composition root, NATS/effect wiring and shutdown |
| cmd/codegen | Database-backed contract code generation CLI |
| cmd/migrate | Apply SQL migrations |
| internal/config | Environment parsing, raw value parsing and validation |
| internal/domain | Envelopes, rules, predicates, actions, decisions, workflow/batch/schedule/lease types, validation, IDs and hashes |
| internal/ports | Broker, delivery, transaction, repository, cache, runtime and use-case interfaces |
| internal/rules | Rule compilation, validation and deterministic predicate/action evaluation |
| internal/services/events | Fencing, inbox dedup, rule lookup/evaluation, execution/outbox transaction |
| internal/services/domainservices | Domain/Service configuration validation and subscription use cases |
| internal/services/effects | Outbox claim, child-event/command/generic/explicit service_event dispatch, retry/backoff, quarantine coordination |
| internal/services/effects/router.go | Child-event broker publishing versus external command dispatch |
| internal/services/rules | Rule revision writes, activation CAS and rule queries |
| internal/services/batches | Batch formation, processing and query APIs |
| internal/services/workflow | Workflow instance transitions; definition helper methods currently contain placeholders |
| internal/services/quarantine | Quarantine query and replay-marking operations |
| internal/application/worker.go | Worker loops, dispatch, periodic publishing/batching, shard consumer reconciliation |
| internal/runtime/keyqueue | Bounded global work, per-key FIFO processing and hot-key checks |
| internal/runtime/shard | Lease acquisition/renewal, owned-shard tracking and fencing tokens |
| internal/runtime/scheduler | Claim due schedules and publish events |
| internal/storage/storage.go | PostgreSQL/Mongo selection and repository composition |
| internal/adapters/http | Router, request handlers, middleware, JSON/YAML decoding and responses |
| internal/adapters/postgres | PostgreSQL pool/transactions and repository queries |
| internal/adapters/mongo | Mongo client/session/transaction and repositories |
| internal/adapters/nats/jetstream.go | Stream configuration, sharded consumers, delivery ACK/NAK/retry and publishing |
| internal/adapters/redis | Redis client and cache implementation; not wired into the primary event path |
| internal/adapters/effects/fake.go | Fake/HTTP destinations plus helper clock/ID types; responsibilities are mixed |
| internal/adapters/effects/domain_service_sender.go | Legacy HTTP callback sender code; not the current NATS service_event delivery path |
| internal/adapters/memory | In-memory contract registry |
| internal/observability | Logging, metrics, tracing and HTTP handler |
| internal/codegen/generator.go | Go/Java/Protobuf/JSON Schema and runtime template generation |
| internal/services and internal/runtime tests | Unit/regression tests for application behavior |
| api/*.json | Public envelope/rule JSON schema source files |
| migrations | Numbered SQL migrations; no down-script set |
| sdk/go/runtime | Original standalone envelope, shard, producer, consumer and evaluation helper module |
| sdk/go/flowrulz | Separate client SDK module with configuration, contracts and the supported NATS event transport; HTTP ingestion, gRPC and local evaluation are unavailable |
| examples/{banking,ecommerce,food-delivery} | Independent modules with contract/rule/workflow sources and sample applications |
| tests/contract | In-memory repository contract suites, not full SQL/Mongo conformance |
| tests/integration | Service behavior tests using mocks/in-memory repositories, not live infrastructure tests |
| docs/diagrams/01-*.mmd to docs/diagrams/27-*.mmd | Focused flow and design diagrams |
| docs/audit | Timestamped static review findings that must be revalidated |

More detailed file-by-file instructions, build commands and invariants are in ../../AGENTS.md.

## Dependency direction

~~~mermaid
flowchart TD
  Entrypoints[cmd/* composition roots] --> Services[internal/services]
  Entrypoints --> Runtime[application and runtime]
  Entrypoints --> Storage[storage and adapters]
  HTTP[HTTP adapter] --> Ports[ports]
  Services --> Ports
  Services --> Domain[domain]
  Rules[rules compiler/evaluator] --> Domain
  Ports --> Domain
  Storage --> SQL[SQL adapter]
  Storage --> Mongo[Mongo adapter]
~~~

Rules:
- Domain and rules never import transport, adapters, services, or database drivers.
- Use-case services depend on domain and ports, not concrete adapters.
- Adapters implement interfaces and keep infrastructure concerns at the edge.
- EventTxRepos and RuleTxRepos are narrow transaction-scoped views; SQL/Mongo adapters own concrete transaction assertions and repository construction. The transaction port still exposes context/fencing details, which remain review debt.
- SDK generated code must not import unexported/internal server packages.

## Confirmed gaps and next steps

| Priority | Gap | Next step |
|---|---|---|
| P0 | Production auth config fails closed and global contract mutations require admin JWT scope; complete resource-level authorization and cross-tenant denial coverage remain incomplete | Keep public exposure blocked until every resource path and cross-tenant negative tests are verified |
| P1 | Outbox delivery/retry/quarantine transitions now require unique claimant tokens and unexpired claims; settlement errors propagate | Keep failure-injection tests and consider transactional quarantine/outbox settlement |
| P1 | Sequential effect dispatch and fixed claim TTL | Add per-effect timeouts and bounded concurrency only after idempotency/claim recovery is verified |
| P1 | Lease allocation uses a configured fixed replica count and releases excess leases when its target shrinks; no dynamic membership/rebalancing | Keep WORKER_REPLICAS consistent across the deployment; add membership and coordinated rebalancing before autoscaling |
| P1 | Batch execution passes a zero fencing token and bypasses the key queue | BATCH_MODE is rejected unless none; integrate batch work with lease fencing and key ordering before enabling |
| P1 | Mongo adapters previously treated JSON bytes as bson.Raw | JSON payloads/source/context now use BSON binary bytes; keep BSON round-trip tests for each JSON-backed field |
| P1 | Scheduled SQL claim fields did not exist in migration 011 | Migrations 015/016 add claim and retry fields; expired claims can be reclaimed and settlement is claimant/expiry fenced |
| P2 | Scheduled event publishing retries with bounded exponential backoff and a terminal attempt limit; validate recovery and adapter parity |
| P2 | Some tenant-scoped reads/writes remain unscoped and no trusted identity exists | AuthN/AuthZ and resource-level scoping remain release blockers |
| P1 | Transaction context/fencing leaks into the ports; adapter parity is not fully proven | Validate transaction port methods and add SQL/Mongo atomicity and repository conformance tests |
| P1 | Contract tests are in-memory-only | Reuse suites against actual SQL and Mongo adapters and test transaction/unique-key semantics |
| P1 | HTTP decoding is bounded and rejects trailing JSON/multiple YAML docs, but tenant identity is still caller-controlled and resource scoping is incomplete | Implement auth-derived tenant identity and authorize every resource operation |
| P2 | Workflow service contains not-implemented definition methods | Implement intentionally or remove placeholders and use a clear repository/service boundary |
| P2 | health is liveness only and no gRPC listener exists | Separate readiness/liveness or document that only liveness is available |
| P2 | No live crash/recovery suite or benchmark evidence | Add live PostgreSQL/NATS failure injection and reproducible benchmarks |
| P2 | SQL outbox insertion is one insert per effect | Measure throughput then introduce batch insert if warranted |
| P2 | Scheduler marks every publish error terminally failed | Add retry classification, attempts and backoff, then test crash/restart recovery |
| P2 | Batch API reads are now tenant-scoped, but batch processing is not integrated with leases/key queue | Keep non-none worker batch modes rejected until a safe processing path exists |

Do not turn this table into claims of completion without changes and tests. A follow-up audit should identify the reviewed main SHA and workflow runs, and label each old finding fixed/partial/open against current source.

## Domain builds

`domains/<domain>/builds/go/` holds typed Go code generated from contracts stored in the database (`make generate-domain DOMAIN=<domain>`). Each is a separate Go module depending on `sdk/go/flowrulz`; example services depend on it through `replace` directives. Run `make verify-generated` to compile them.

## Rule check API

`POST /v1/rules/check` is a dry run used by the console Rule studio. Body: `{"source": "<YAML or JSON rule set>", "event_type"?: string, "event"?: object}`. It compiles the source and, when `event` is given, evaluates it against a synthetic envelope and returns the decision (rule explanations, matched rules, effects). Nothing is persisted or activated; compile errors return HTTP 200 with `valid:false`. Implemented in `internal/services/rules/check.go`; the handler returns 501 if the rule service does not implement `ports.RuleChecker`.
