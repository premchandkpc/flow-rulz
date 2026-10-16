# E-commerce Example

This example demonstrates how to use FlowRulz to model an e-commerce order processing workflow using rules and child events instead of building separate microservices.

## Architecture

```
OrderCreated
     │
     ▼
Rule: payment required?
     │
     ├── yes → PaymentRequested
     │
     └── no  → OrderConfirmed

PaymentRequested
     │
     ▼
PaymentCompleted
     │
     ▼
Rule: fraud check needed? (amount >= $100)
     │
     ├── yes → FraudCheck
     │
     └── no  → OrderConfirmed

FraudCheck
     │
     ▼
FraudResult
     │
     ├── passed → ShipmentRequested
     │
     └── failed → FraudReview
```

## Contracts

| Contract | Description |
|----------|-------------|
| `order.created` | New order placed |
| `payment.requested` | Payment requested for order |
| `payment.completed` | Payment successfully processed |
| `fraud.check` | Trigger fraud check |
| `fraud.result` | Fraud check result |
| `fraud.review` | Manual fraud review required |
| `order.confirmed` | Order confirmed |
| `shipment.requested` | Shipment requested |

## Rules

| Rule Set | Description |
|----------|-------------|
| `order.created` | Checks if payment is required, auto-confirms free orders |
| `payment.completed` | Triggers fraud check for high-value orders, auto-confirms low-value |
| `fraud.result` | Routes to shipment or manual review based on fraud check |

## Workflow

The `order-processing` workflow defines the state machine:
- `pending` → `payment_pending` (on `payment.requested`)
- `pending` → `confirmed` (on `order.confirmed` for free orders)
- `payment_pending` → `payment_completed` (on `payment.completed`)
- `payment_completed` → `fraud_check` (on `fraud.check`)
- `fraud_check` → `fraud_review` (on `fraud.review`)
- `fraud_check` → `shipped` (on `shipment.requested`)
- `fraud_review` → `shipped` (on `shipment.requested`)

## Running the Example

### Prerequisites

- NATS JetStream server running on `localhost:4222`
- FlowRulz API running on `localhost:8080`
- FlowRulz Worker running

### 1. Register Contracts

```bash
# Register all contracts
for f in contracts/*.yaml; do
  curl -X POST http://localhost:8080/v1/contracts \
    -H "Content-Type: application/yaml" \
    -d @$f
done
```

### 2. Deploy Rules

```bash
# Deploy rules
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
# Generate Go types
flowrulz-codegen -contract order.created -version 1.0 -target go -output /tmp/out  # or: make generate-domain DOMAIN=ecommerce (writes domains/ecommerce/builds/go)

# Generate Java classes
flowrulz-codegen -contract order.created -version 1.0 -target java -output /tmp/out/java/OrderCreated.java

# Generate Protobuf
flowrulz-codegen -contract order.created -version 1.0 -target protobuf -output /tmp/out/protobuf/order_created.proto

# Generate JSON Schema
flowrulz-codegen -contract order.created -version 1.0 -target jsonschema -output /tmp/out/jsonschema/order_created.json
```

## Generated Artifacts

The generated types allow type-safe event handling in your application:

```go
// Generated Go type
type OrderCreated struct {
	OrderID      string  `json:"order_id" validate:"required"`
	CustomerID   string  `json:"customer_id" validate:"required"`
	TotalAmount  float64 `json:"total_amount" validate:"required"`
	Currency     string  `json:"currency"`
	Items        []OrderItem `json:"items" validate:"required"`
	ShippingAddress Address `json:"shipping_address" validate:"required"`
	CreatedAt    time.Time `json:"created_at" validate:"required"`
}

// Usage in your application
func handleOrderCreated(event *OrderCreated) {
    // Type-safe access to order data
    fmt.Printf("Order %s for customer %s: $%.2f\n", 
        event.OrderID, event.CustomerID, event.TotalAmount/100)
}
```

## Key Benefits

1. **No microservices needed** - Business logic expressed as rules
2. **Type-safe contracts** - Generated types prevent schema drift
3. **Audit trail** - Every decision recorded with decision hash
4. **Duplicate-resistant processing** - Inbox deduplication reduces duplicate rule execution
5. **Scalable** - Sharding by customer_id ensures ordering per customer
6. **Observable** - Full lineage from event → execution → effects → child events