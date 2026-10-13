package postgres

import (
	"encoding/json"
	"testing"
	"time"
)

func TestContractSchemaRowToDomainPreservesTimestamps(t *testing.T) {
	createdAt := time.Date(2026, time.October, 12, 5, 24, 0, 0, time.UTC)
	updatedAt := createdAt.Add(time.Minute)
	row := contractSchemaRow{
		Name:      "order.created",
		Version:   "1.0",
		Fields:    json.RawMessage(`{"order_id":{"type":"string"}}`),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}

	schema, err := row.toDomain()
	if err != nil {
		t.Fatalf("toDomain() error = %v", err)
	}
	if !schema.CreatedAt.Equal(createdAt) {
		t.Errorf("CreatedAt = %v, want %v", schema.CreatedAt, createdAt)
	}
	if !schema.UpdatedAt.Equal(updatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", schema.UpdatedAt, updatedAt)
	}
	if schema.Fields["order_id"].Type != "string" {
		t.Errorf("Fields[order_id].Type = %q, want %q", schema.Fields["order_id"].Type, "string")
	}
}
