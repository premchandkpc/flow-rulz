# Food Delivery Example

This example demonstrates a food delivery order flow with restaurant acceptance, food preparation, driver assignment, and delivery.

## Architecture

```
OrderPlaced
      │
      ▼
Rule: auto-accept based on restaurant rating (rating >= 4.0)
      │
      ├── yes → RestaurantAccepted (auto)
      │
      └── no  → RestaurantAccepted (manual review)

RestaurantAccepted
      │
      ▼
FoodPrepared
      │
      ▼
Rule: assign driver (nearest available in postal code)
      │
      ▼
DriverAssigned
      │
      ▼
Delivered
```

## Contracts

| Contract | Description |
|----------|-------------|
| `order.placed` | Customer places order |
| `restaurant.accepted` | Restaurant accepts order |
| `food.prepared` | Food is ready for pickup |
| `driver.assigned` | Driver assigned for delivery |
| `delivered` | Order delivered to customer |

## Rules

| Rule Set | Description |
|----------|-------------|
| `order.placed` | Auto-accepts orders from high-rated restaurants |
| `food.prepared` | Assigns nearest available driver in delivery zone |
| `delivered` | Completes order workflow |

## Workflow

The `food-delivery` workflow defines the state machine:
- `placed` → `restaurant_review` (on `order.placed`)
- `restaurant_review` → `accepted` (on `restaurant.accepted`)
- `accepted` → `preparing` (on `food.prepared`)
- `preparing` → `driver_assigned` (on `driver.assigned`)
- `driver_assigned` → `delivered` (on `delivered`)

## Running the Example

### Prerequisites
- NATS JetStream server running on `localhost:4222`
- FlowRulz API running on `localhost:8080`
- FlowRulz Worker running

### 1. Register Contracts
```bash
for f in contracts/*.yaml; do
  curl -X POST http://localhost:8080/v1/contracts \
    -H "Content-Type: application/yaml" \
    -d @$f
done
```

### 2. Deploy Rules
```bash
for f in rules/*.yaml; do
  ruleSet=$(basename $f .yaml)
  curl -X POST http://localhost:8080/v1/rules/$ruleSet/revisions \
    -H "Content-Type: application/yaml" \
    -d @$f
done
```

### 3. Workflow (reference only)
The workflow YAML under `workflows/` documents the process; the tenant API does not expose workflow mutation, so it is not deployed.

### 4. Run Producer
```bash
cd backend/producer
go run main.go
```

### 5. Run Consumer
```bash
cd backend/consumer
go run main.go
```

## Code Generation

Generate Go types from contracts:
```bash
flowrulz-codegen -contract order.placed -version 1.0 -target go -output /tmp/out  # or: make generate-domain DOMAIN=food-delivery (writes domains/food-delivery/builds/go)
```

## Key Benefits

1. **Geographic partitioning** - Sharding by postal code ensures driver assignment locality
2. **Rating-based automation** - High-rated restaurants get auto-acceptance
3. **Real-time tracking** - Workflow state machine provides live order status
4. **Type-safe contracts** - Generated types for order, restaurant, driver entities
5. **Duplicate-resistant processing** - Inbox deduplication reduces duplicate rule execution
6. **Observable** - Full lineage from order → restaurant → driver → delivery