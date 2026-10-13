# Random multi-client load generator

This command sends a bounded stream of randomized events through the public Go SDK transport and starts one live observer per simulated tenant. It is intended for local development and smoke testing, not a benchmark or a durable-consumer correctness test.

## Run

Start the local NATS JetStream service and the FlowRulz worker first. From the repository root:

```sh
NATS_URL=nats://localhost:4222 go run ./cmd/random-loadgen --clients 5 --messages 100
```

Run it from the SDK module directory:

```sh
cd sdk/go
NATS_URL=nats://localhost:4222 go run ./cmd/random-loadgen --clients 5 --messages 100
```

Flags:
- `--clients`: simulated tenant clients, 1-20 (default 5).
- `--messages`: total publish attempts, 1-100 (default 100).
- `--startup-delay`: time to let live observer subscriptions start before publishing (default 500ms).

The generator creates tenants named `load-client-01`, `load-client-02`, etc. Each event gets a unique payload, random account-like partition key, random value, and timestamp. Events use the `loadtest.message` rule-set/type and are sent through the SDK's transport interface; the tool does not import NATS libraries directly.

## Interpret the output

- `PUBLISHED` means the broker accepted the publish request.
- `CONSUMED` means the SDK's best-effort live observer saw the event.
- The final `observed` count is not a durable-processing guarantee. SDK subscriptions are non-durable core-NATS observers, so messages can be missed if a subscription is not ready or disconnects. The FlowRulz worker's JetStream consumer is a separate durable processing path.
- This smoke test does not create rules or contracts. Configure a matching `loadtest.message` rule set first if you want to exercise rule evaluation and execution, not only the producer/broker/observer path.
- Use only against a development environment. Do not point randomized traffic at production.
