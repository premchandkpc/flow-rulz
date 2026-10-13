# Random Producer Example

This example generates domain-shaped FlowRulz events using the existing e-commerce, banking, and food-delivery contracts, then publishes them to the same NATS JetStream subject family used by the worker. It is deliberately located under `examples/`: it drives realistic input traffic through the existing example workflows, rather than inventing a separate random message schema. It is not part of API/worker startup or a replacement for consumer-side end-to-end tests.

## Run against the local stack

From the repository root, start the stack and register the existing domain definitions first:

```sh
cp .env.example .env
docker compose up -d postgres nats redis api worker
(cd domains/ecommerce && ./deploy.sh)
(cd domains/banking && ./deploy.sh)
(cd domains/food-delivery && ./deploy.sh)
docker compose --profile examples run --rm random-producer
```

Or run it directly after starting NATS and the worker:

```sh
go run ./examples/random-producer --scenario all --tenant acme-corp --clients 10 --max-messages 100 --shards 4096 --concurrency 16 --nats nats://localhost:4222
```

Configuration:
- `RANDOM_PRODUCER_CLIENTS`: number of simulated business clients/accounts (1-1000), default 10.
- `RANDOM_PRODUCER_MESSAGES_MAX`: maximum messages per client (1-100), default 100.
- `RANDOM_PRODUCER_CONCURRENCY`: concurrent publishers (1-256), default 16.
- `RANDOM_PRODUCER_SCENARIO`: `all` (default), `ecommerce`, `banking`, or `food-delivery`. `all` cycles across the three initial event types.
- `RANDOM_PRODUCER_TENANT_ID`: tenant used by the deployed example rules; defaults to `acme-corp`. Simulated clients remain partition keys within that tenant. Set it to the same tenant used by the regular example producers.
- `NUM_SHARDS`: must match the worker's shard count, default 4096.
- `NATS_URL`: NATS endpoint; inside Compose use `nats://nats:4222`.

The example emits contract-shaped `order.created`, `transaction.initiated`, and/or `order.placed` inputs. The corresponding example contracts, rules, and workflows must be deployed for those events to exercise business processing. FlowRulz rules can then create downstream payment/fraud, compliance, and delivery events. It reports generated attempts, JetStream-acknowledged publications, and failures. Broker acknowledgement means the broker accepted a message; it does not prove that a worker processed it. Use integration/E2E tests to assert durable execution, ACK/redelivery behavior, inbox deduplication and lease failover.

## Ownership and boundaries

- `main.go` parses options, configures shutdown/deadline handling, and wires the NATS adapter.
- `producer.go` owns example-specific generation and the narrow publisher contract.
- `producer_test.go` verifies generated envelopes, domain-computed shard subjects, invalid settings, cancellation and publisher failures without requiring a live broker.
- The example reuses `internal/domain.EventEnvelope` and `VirtualShard`; its scenario tests read the existing YAML contracts from `domains/ecommerce`, `domains/banking`, and `domains/food-delivery` and verify generated payloads against their required fields and types. It does not reimplement the production envelope or shard hash algorithm.
- The transport implementation remains in `internal/adapters/nats`. No NATS types leak into the producer use case.

This is an example/load tool, not production runtime functionality. If a future reusable production use case needs load generation, promote it into an internal package only after a concrete non-example caller exists.
