package postgres

import (
	"encoding/json"
	"testing"
	"time"
)

func TestContractSchemaRowToDomainPreservesTimestamps(t *testing.T) {
	row := timestampedContractRow()
	schema, err := row.toDomain()
	if err != nil {
		t.Fatalf("toDomain() error = %v", err)
	}
	if !schema.CreatedAt.Equal(row.CreatedAt) || !schema.UpdatedAt.Equal(row.UpdatedAt) {
		t.Errorf("timestamps = (%v, %v), want (%v, %v)", schema.CreatedAt, schema.UpdatedAt, row.CreatedAt, row.UpdatedAt)
	}
	if schema.Fields["order_id"].Type != "string" {
		t.Errorf("Fields[order_id].Type = %q, want string", schema.Fields["order_id"].Type)
	}
}

func timestampedContractRow() contractSchemaRow {
	createdAt := time.Date(2026, time.October, 12, 5, 24, 0, 0, time.UTC)
	return contractSchemaRow{
		Name: "order.created", Version: "1.0",
		Fields:    json.RawMessage(`{"order_id":{"type":"string"}}`),
		CreatedAt: createdAt, UpdatedAt: createdAt.Add(time.Minute),
	}
}
