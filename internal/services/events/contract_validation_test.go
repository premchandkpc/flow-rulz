package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func contractEnvelope(hash string) *domain.EventEnvelope {
	return &domain.EventEnvelope{
		Contract: &domain.ContractRef{Name: "order.created", Version: "1.0.0", SchemaHash: hash},
		Data:     json.RawMessage(`{}`),
	}
}

func TestContractViolationsArePermanent(t *testing.T) {
	tests := []struct {
		name     string
		schema   *domain.ContractSchema
		envelope *domain.EventEnvelope
	}{
		{
			name:     "deprecated contract",
			schema:   &domain.ContractSchema{Name: "order.created", Version: "1.0.0", Deprecated: true},
			envelope: contractEnvelope("h"),
		},
		{
			name:     "schema hash mismatch",
			schema:   &domain.ContractSchema{Name: "order.created", Version: "1.0.0", SchemaHash: "stored", Kind: domain.ContractKindEvent},
			envelope: contractEnvelope("other"),
		},
		{
			name:     "missing schema hash",
			schema:   &domain.ContractSchema{Name: "order.created", Version: "1.0.0", SchemaHash: "stored", Kind: domain.ContractKindEvent},
			envelope: contractEnvelope(""),
		},
		{
			name:     "not an event contract",
			schema:   &domain.ContractSchema{Name: "order.created", Version: "1.0.0", SchemaHash: "h", Kind: domain.ContractKind("entity")},
			envelope: contractEnvelope("h"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateEventPayload(tt.schema, tt.envelope)
			if tt.schema.Deprecated {
				err = validateResolvedContract(tt.schema, tt.envelope)
			}
			if !errors.Is(err, domain.ErrEventContractViolation) {
				t.Fatalf("error = %v, want ErrEventContractViolation", err)
			}
			if !domain.IsPermanent(err) {
				t.Fatalf("IsPermanent(%v) = false, want true", err)
			}
		})
	}
}

func TestMissingContractRegistryStaysRetryable(t *testing.T) {
	err := (&Service{}).validateEventContract(context.Background(), contractEnvelope("h"))
	if err == nil {
		t.Fatal("expected an error when the contract registry is not configured")
	}
	if domain.IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = true, want false: a missing registry is a deployment fault", err)
	}
}
