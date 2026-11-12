# Direct rule worker

For producers and consumers connecting to FlowRulz's own broker, use
[`WORKER_MODE=broker`](embedded-broker.md).

Direct mode provides `A → broker → rule worker → broker → B / another rule worker`.
It uses the existing NATS JetStream adapter and rule compiler/evaluator, without
PostgreSQL, MongoDB, Redis, the API, inbox, execution persistence, outbox, shard
leases, fencing, per-key queue, subscriber discovery, scheduler, or metrics server.
Durable mode remains the default and preserves the existing transactional path.

## Run

Start the minimal local stack (do not use the regular Compose stack at the same
time on port 4222):

```sh
docker compose -f docker-compose.direct.yml up --build
```

Or start only a JetStream-enabled NATS server and run the worker locally:

```sh
WORKER_MODE=direct \
WORKER_RULES_DIR=examples/direct-worker/rules \
NATS_URL=nats://127.0.0.1:4222 \
NATS_STREAM=flowrulz-direct \
NATS_CONSUMER=flowrulz-direct-worker \
NUM_SHARDS=16 \
go run ./cmd/worker
```

The worker loads 1–100 JSON rule files, each at most 256 KiB, at startup. One
revision per event type is allowed; `rule_set` must equal the incoming event's
`type`. Restart after editing files. Use identical rules on replicas sharing the
same consumer. Database/API environment settings are not required by this path.

Publish a normal EventEnvelope with `type: "a.created"` and `data: {"id":"123"}`.
Include a unique `id`, `tenant_id`, `partition_key`, and RFC3339 `occurred_at`.
Use the SDK/shared shard algorithm with **16 shards** and stream
`flowrulz-direct`; do not invent another routing hash. The sample's first rule
emits `a.processed`, which re-enters the same broker subject family and can be
processed by any replica. Its second rule sends a command to
`commands.b.handle`. Service B consumes the existing Effect JSON representation
and honors its `idempotency_key`. Command data uses the existing evaluator's
literal payload semantics; child-event data supports existing templates.

## Configuration and guarantees

| Setting | Direct mode |
|---|---|
| `WORKER_MODE` | `direct`; default `durable` |
| `WORKER_RULES_DIR` | Required directory of local JSON rules |
| `WORKER_INPUT_SUBJECT` | Default `events.shard.*`; must belong to the configured stream |
| `NATS_CONSUMER` | Shared consumer group base name; adapter appends `-direct` |
| `NATS_ACK_WAIT_MS` | Default 30000; processing/publishing deadline is half this value |
| `EVENT_RETRY_DELAY_MS` | Default 5000; applies only after failure |
| `NATS_MAX_DELIVER` | Must remain `-1`, so unprocessed inputs are not silently exhausted |
| `NUM_SHARDS` | Must match producers and child-event publishers |
| `MAX_GLOBAL_IN_FLIGHT` | Default 100; bounds broker pending deliveries across the shared consumer |

Each worker requests one input at a time from the shared direct consumer. The worker
evaluates synchronously, publishes each output immediately with broker confirmation,
and ACKs the input only after every output succeeds. There is no application
polling timer between commit and output, because there is no database commit.
No end-to-end latency target or throughput claim has been established.

Delivery remains at-least-once. A crash, ACK failure, or partial output failure can
repeat outputs. Stable effect/child IDs support receiver deduplication. Rule
templates involving current time can produce different payloads on retries;
there is no stored decision snapshot. Processing is sequential in each process;
strict key ordering across replicas, retries and ACK expiry is not guaranteed.

Malformed, unknown-type and unsupported contract-bound inputs remain in broker
retry rather than being silently acknowledged. There is no quarantine store;
monitor worker errors and fix the producer/rules. Registry-bound input contracts
and scheduled rules are explicitly unsupported; choose durable mode for those.
Direct mode supports `emit`, `command`, and `emit_event` actions.

Keep direct and durable consumers on separate streams or disjoint input subjects.
Different durable names on the same subject receive independent copies. Do not
run two pipelines on the same events unintentionally. Broker authentication and
subject permissions remain deployment responsibilities; this mode does not add
HTTP ingress. The sample Compose binding is loopback-only.

To roll back, stop direct workers and restore the durable configuration. Direct
processing creates no database execution records to backfill automatically;
retained broker events can be replayed deliberately, with duplicate handling.

## Design and evidence

`cmd/worker/direct.go` owns file I/O and composition; `application.DirectWorker`
owns evaluation/publication/settlement ordering; `adapters/nats/direct.go` owns
the shared broker consumer. Core application code depends on domain types and
narrow ports, never NATS types. Existing envelope, rule, effect and shard contracts
are reused. This explicit mode avoids a matrix of incompatible per-feature flags
and preserves durable-mode fencing/transaction invariants.
The `proposals/kafka-log-first` package is a separate, client-neutral Kafka
transaction sketch with its own envelope. It is not wired into the runtime;
direct mode reuses the production contracts instead of importing that prototype.

The application tests inject partial publication and settlement failures. The
tagged `TestDirectWorkerChainWithoutDatabase` uses only a real NATS container,
with no PostgreSQL, to prove the two-rule chain and command delivery. Existing
durable E2E tests remain in the same suite. This does not establish crash recovery
or multi-replica ordering for direct mode.
