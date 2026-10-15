package main

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// Scenario selects one of the business domains already documented in examples/.
type Scenario string

const (
	ScenarioAll          Scenario = "all"
	ScenarioEcommerce    Scenario = "ecommerce"
	ScenarioBanking      Scenario = "banking"
	ScenarioFoodDelivery Scenario = "food-delivery"
)

var supportedScenarios = []Scenario{
	ScenarioEcommerce,
	ScenarioBanking,
	ScenarioFoodDelivery,
}

func (s Scenario) Valid() bool {
	switch s {
	case ScenarioAll, ScenarioEcommerce, ScenarioBanking, ScenarioFoodDelivery:
		return true
	default:
		return false
	}
}

func scenarioForSequence(sequence int) Scenario {
	return supportedScenarios[(sequence-1)%len(supportedScenarios)]
}

func newScenarioEvent(scenario Scenario, tenantID, clientID string, sequence int, rng *rand.Rand) (domain.EventEnvelope, error) {
	now := time.Now().UTC()
	orderID := fmt.Sprintf("order-%s-%04d", clientID, sequence)
	eventType := ""
	partitionKey := ""
	var data map[string]any

	switch scenario {
	case ScenarioEcommerce:
		// Mirrors domains/ecommerce/contracts/order.created.yaml.
		eventType = "order.created"
		partitionKey = clientID
		data = map[string]any{
			"order_id":     orderID,
			"customer_id":  clientID,
			"total_amount": float64((rng.Intn(24000) + 1000)),
			"currency":     "USD",
			"items": []map[string]any{
				{"product_id": fmt.Sprintf("product-%03d", rng.Intn(50)+1), "quantity": int64(rng.Intn(3) + 1), "unit_price": float64(rng.Intn(9000) + 1000)},
			},
			"shipping_address": map[string]string{
				"street": fmt.Sprintf("%d Market Street", rng.Intn(900)+100),
				"city":   "San Francisco", "state": "CA", "postal_code": "94102", "country": "US",
			},
			"created_at": now.Format(time.RFC3339Nano),
		}
	case ScenarioBanking:
		// Mirrors domains/banking/contracts/transaction.initiated.yaml.
		eventType = "transaction.initiated"
		accountID := fmt.Sprintf("account-%04d", clientNumber(clientID))
		partitionKey = accountID
		counterparty := fmt.Sprintf("account-%04d", rng.Intn(1000)+1)
		if rng.Intn(4) == 0 {
			counterparty = fmt.Sprintf("intl-account-%04d", rng.Intn(1000)+1)
		}
		data = map[string]any{
			"transaction_id":       fmt.Sprintf("txn-%s-%04d", clientID, sequence),
			"account_id":           accountID,
			"amount":               float64(rng.Intn(2_000_000) + 500),
			"currency":             "USD",
			"transaction_type":     []string{"transfer", "payment", "withdrawal"}[rng.Intn(3)],
			"channel":              []string{"mobile", "web", "branch", "api"}[rng.Intn(4)],
			"counterparty_account": counterparty,
			"description":          fmt.Sprintf("simulated-transfer-%04d", sequence),
			"initiated_at":         now.Format(time.RFC3339Nano),
		}
	case ScenarioFoodDelivery:
		// Mirrors domains/food-delivery/contracts/order.placed.yaml.
		eventType = "order.placed"
		postalCode := []string{"94102", "94103", "94107", "94110"}[(clientNumber(clientID)-1)%4]
		partitionKey = postalCode
		quantity := int64(rng.Intn(3) + 1)
		unitPrice := float64(rng.Intn(2500) + 500)
		data = map[string]any{
			"order_id":      orderID,
			"customer_id":   clientID,
			"restaurant_id": fmt.Sprintf("restaurant-%03d", rng.Intn(30)+1),
			"total_amount":  unitPrice * float64(quantity),
			"currency":      "USD",
			"items": []map[string]any{
				{"menu_item_id": fmt.Sprintf("menu-item-%03d", rng.Intn(40)+1), "name": []string{"Margherita Pizza", "Rice Bowl", "Veggie Burger", "Noodles"}[rng.Intn(4)], "quantity": quantity, "unit_price": unitPrice},
			},
			"delivery_address": map[string]string{
				"street": fmt.Sprintf("%d Oak Avenue", rng.Intn(900)+100),
				"city":   "San Francisco", "state": "CA", "postal_code": postalCode,
			},
			"created_at": now.Format(time.RFC3339Nano),
		}
	default:
		return domain.EventEnvelope{}, fmt.Errorf("unsupported scenario %q", scenario)
	}

	payload, err := json.Marshal(data)
	if err != nil {
		return domain.EventEnvelope{}, fmt.Errorf("marshal %s payload: %w", scenario, err)
	}
	return domain.EventEnvelope{
		ID:           domain.NewID(),
		Type:         eventType,
		TenantID:     tenantID,
		PartitionKey: partitionKey,
		OccurredAt:   now,
		Data:         payload,
		Headers: map[string]string{
			"source":    "flowrulz-random-producer",
			"scenario":  string(scenario),
			"client_id": clientID,
		},
		Meta: map[string]any{
			"tenant_id":     tenantID,
			"partition_key": partitionKey,
			"client_id":     clientID,
			"scenario":      string(scenario),
		},
	}, nil
}

func clientNumber(clientID string) int {
	var number int
	if _, err := fmt.Sscanf(clientID, "client-%d", &number); err != nil || number < 1 {
		return 1
	}
	return number
}
