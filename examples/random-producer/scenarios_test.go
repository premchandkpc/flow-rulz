package main

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

type contractDefinition struct {
	Name   string                   `yaml:"name"`
	Fields map[string]contractField `yaml:"fields"`
}

type contractField struct {
	Type       string                   `yaml:"type"`
	Required   bool                     `yaml:"required"`
	Items      *contractField           `yaml:"items"`
	Properties map[string]contractField `yaml:"properties"`
}

func TestScenarioPayloadsMatchExistingExampleContracts(t *testing.T) {
	tests := []struct {
		scenario Scenario
		contract string
	}{
		{ScenarioEcommerce, "../../domains/ecommerce/contracts/order.created.yaml"},
		{ScenarioBanking, "../../domains/banking/contracts/transaction.initiated.yaml"},
		{ScenarioFoodDelivery, "../../domains/food-delivery/contracts/order.placed.yaml"},
	}

	for _, tt := range tests {
		t.Run(string(tt.scenario), func(t *testing.T) {
			contractBytes, err := os.ReadFile(tt.contract)
			if err != nil {
				t.Fatalf("read existing example contract %q: %v", tt.contract, err)
			}
			var contract contractDefinition
			if err := yaml.Unmarshal(contractBytes, &contract); err != nil {
				t.Fatalf("decode contract %q: %v", tt.contract, err)
			}

			event, err := newScenarioEvent(tt.scenario, DefaultTenantID, "client-0001", 1, rand.New(rand.NewSource(42)))
			if err != nil {
				t.Fatalf("newScenarioEvent() error = %v", err)
			}
			if event.Type != contract.Name {
				t.Fatalf("event type = %q, contract name = %q", event.Type, contract.Name)
			}
			if err := event.Validate(); err != nil {
				t.Fatalf("generated envelope invalid: %v", err)
			}

			var payload map[string]any
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			if err := validateContractObject(payload, contract.Fields, "$"); err != nil {
				t.Fatalf("payload does not match %s: %v", contract.Name, err)
			}
			if tt.scenario == ScenarioBanking {
				channel, ok := payload["channel"].(string)
				switch channel {
				case "mobile", "web", "branch", "api":
				default:
					t.Fatalf("banking channel = %q (present=%t), want mobile, web, branch, or api", channel, ok)
				}
			}
			if event.PartitionKey == "" {
				t.Fatal("partition key must be set")
			}
		})
	}
}

func TestAllScenarioSelectionCyclesAcrossExistingDomains(t *testing.T) {
	want := []Scenario{ScenarioEcommerce, ScenarioBanking, ScenarioFoodDelivery}
	for sequence, expected := range want {
		if got := scenarioForSequence(sequence + 1); got != expected {
			t.Errorf("scenarioForSequence(%d) = %q, want %q", sequence+1, got, expected)
		}
	}
	if got := scenarioForSequence(len(want) + 1); got != ScenarioEcommerce {
		t.Errorf("scenario cycle = %q, want %q", got, ScenarioEcommerce)
	}
}

func validateContractObject(value map[string]any, fields map[string]contractField, path string) error {
	for name, field := range fields {
		actual, exists := value[name]
		if !exists {
			if field.Required {
				return fmt.Errorf("%s.%s is required", path, name)
			}
			continue
		}
		if err := validateContractValue(actual, field, path+"."+name); err != nil {
			return err
		}
	}
	return nil
}

func validateContractValue(value any, field contractField, path string) error {
	switch field.Type {
	case "string":
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a string, got %T", path, value)
		}
	case "number":
		switch value.(type) {
		case float64, int, int64:
		default:
			return fmt.Errorf("%s must be a number, got %T", path, value)
		}
	case "integer":
		number, ok := value.(float64)
		if !ok || math.Trunc(number) != number {
			return fmt.Errorf("%s must be an integer, got %T(%v)", path, value, value)
		}
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return fmt.Errorf("%s must be an object, got %T", path, value)
		}
		if err := validateContractObject(object, field.Properties, path); err != nil {
			return err
		}
	case "array":
		items, ok := value.([]any)
		if !ok {
			return fmt.Errorf("%s must be an array, got %T", path, value)
		}
		if field.Items != nil {
			for index, item := range items {
				if err := validateContractValue(item, *field.Items, fmt.Sprintf("%s[%d]", path, index)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func TestScenarioPartitionKeysFollowDomainExamples(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for _, scenario := range supportedScenarios {
		event, err := newScenarioEvent(scenario, DefaultTenantID, "client-0007", 3, rng)
		if err != nil {
			t.Fatalf("%s: %v", scenario, err)
		}
		var payload map[string]any
		if err := json.Unmarshal(event.Data, &payload); err != nil {
			t.Fatal(err)
		}
		switch scenario {
		case ScenarioEcommerce:
			if event.PartitionKey != payload["customer_id"] {
				t.Errorf("ecommerce partition key = %q, customer_id = %v", event.PartitionKey, payload["customer_id"])
			}
		case ScenarioBanking:
			if event.PartitionKey != payload["account_id"] {
				t.Errorf("banking partition key = %q, account_id = %v", event.PartitionKey, payload["account_id"])
			}
		case ScenarioFoodDelivery:
			address, ok := payload["delivery_address"].(map[string]any)
			if !ok || event.PartitionKey != address["postal_code"] {
				t.Errorf("food-delivery partition key = %q, address = %v", event.PartitionKey, payload["delivery_address"])
			}
		}
	}
}
