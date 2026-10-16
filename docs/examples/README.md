# FlowRulz Examples

This directory contains example applications built on top of the FlowRulz event-driven business runtime platform.

## Examples

### 🛒 E-commerce (`examples/ecommerce/`)
Complete order processing flow demonstrating:
- Order creation → Payment → Fraud Check → Shipment
- Rules for payment requirements, fraud detection, auto-confirmation
- Workflow with 7 states and conditional transitions
- 8 contract definitions with full schema

### 🍕 Food Delivery (`examples/food-delivery/`)
Food delivery order flow demonstrating:
- Order → Restaurant Accept → Food Prepared → Driver Assignment → Delivered
- Rules for auto-acceptance based on restaurant rating
- Driver assignment with geographic partitioning
- Workflow with 6 states

### 🏦 Banking (`examples/banking/`)
Financial transaction processing demonstrating:
- Transaction initiation → Compliance check → Settlement/Block
- Rules for high-value and international transaction compliance
- Risk-based routing with audit trail
- Workflow with 4 states

## Quick Start

### 1. Start Infrastructure
```bash
# Start NATS JetStream
docker run -d -p 4222:4222 -p 8222:8222 nats:latest -js

# Start PostgreSQL
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=flowrulz postgres:15
```

### 2. Run FlowRulz API
```bash
cd /path/to/flowrulz
DATABASE_URL="postgres://postgres:postgres@localhost:5432/flowrulz?sslmode=disable" \
MIGRATIONS_DIR="migrations" \
go run ./cmd/api
```

### 3. Run FlowRulz Worker
```bash
MONGODB_URI="mongodb://localhost:27017" \
NATS_URL="nats://localhost:4222" \
go run ./cmd/worker
```

### 4. Deploy Example Contracts & Rules
```bash
# E-commerce
cd domains/ecommerce
./deploy.sh

# Food Delivery
cd domains/food-delivery
./deploy.sh

# Banking
cd domains/banking
./deploy.sh
```

### 5. Run Producer/Consumer
```bash
# Set the same tenant for the producer and consumer, and start the consumer first.
export FLOWRULZ_TENANT_ID=acme-corp
export FLOWRULZ_TRANSPORT=nats
export NATS_URL=nats://localhost:4222

# E-commerce consumer
cd examples/ecommerce/backend/consumer
go run .

# In another terminal, with the same environment:
cd examples/ecommerce/backend/producer
go run .
```

Example producers use the generated typed `Send` method. Consumers use the generated typed `Subscribe` method, which observes live core NATS publications without competing for JetStream messages owned by the FlowRulz worker. Start consumers before producing events; this subscription is non-durable, has no replay, and is intended for live observation rather than reliable business processing. The worker remains responsible for durable rule processing.

## Domain-aware load simulation

The opt-in [`random-producer`](random-producer/README.md) can seed the existing e-commerce, banking, and food-delivery flows with contract-shaped initial events. Deploy the relevant example contracts, rules, and workflow first, then run `docker compose --profile examples run --rm random-producer`. By default it cycles across all three domains under `RANDOM_PRODUCER_TENANT_ID=acme-corp`; the number of simulated business clients is separate from the FlowRulz tenant. A JetStream publish ACK only confirms broker acceptance, not completion by a worker.

## Code Generation

All examples support code generation from contracts:

```bash
# Generate Go types
flowrulz-codegen -contract order.created -version 1.0 -target go

# Generate Java classes
flowrulz-codegen -contract order.created -version 1.0 -target java

# Generate Protobuf
flowrulz-codegen -contract order.created -version 1.0 -target protobuf

# Generate JSON Schema
flowrulz-codegen -contract order.created -version 1.0 -target jsonschema
```

## Architecture Pattern

These examples demonstrate the core FlowRulz philosophy:

> **Define contracts + rules + workflows, let the runtime execute them.**

Instead of building:
- `order-service`
- `payment-service`
- `fraud-service`
- `shipment-service`
- `notification-service`

You define:
- **Contracts** (schemas for events/commands/effects)
- **Rules** (business logic as declarative predicates + actions)
- **Workflows** (state machines for long-running processes)

The FlowRulz **Worker runtime** handles:
- Event consumption & deduplication
- Sharding & ordering guarantees
- Rule evaluation (deterministic, pure)
- Execution recording with decision hashes
- Outbox pattern for reliable effects
- Child event chaining
- Workflow state progression
- Retry, quarantine, observability

## Generated Types

Generated domain code lives in the repository-level `domains/<domain>/builds/go/` modules (`make generate-domain DOMAIN=<domain>`), not inside each example. Examples import it together with the SDK from `sdk/go/flowrulz`.

Generating a Go target also emits a typed client with default-rule-set and explicit-rule-set send/subscribe methods. Generated Go clients import the separate FlowRulz client SDK module.

## Key Concepts Demonstrated

| Concept | E-commerce | Food Delivery | Banking |
|---------|------------|---------------|---------|
| Event chaining | ✅ | ✅ | ✅ |
| Conditional routing | ✅ | ✅ | ✅ |
| Sharding by key | ✅ (customer) | ✅ (postal code) | ✅ (account) |
| Workflow state machine | ✅ | ✅ | ✅ |
| Fraud/risk rules | ✅ | | ✅ |
| Geographic partitioning | | ✅ | |
| Compliance workflows | | | ✅ |
| Code generation | ✅ | ✅ | ✅ |

## Learn More

- [FlowRulz Architecture](../docs/architecture/map.md)
- [API Documentation](../docs/)
- [Rule Schema](../api/rule-schema.json)
- [Event Envelope Schema](../api/envelope-schema.json)