package flowrulz

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeUsesExtensibleMeta(t *testing.T) {
	env := &Envelope{
		ID: "evt-1", Type: "orders.created", TenantID: "merchant-42", PartitionKey: "order-1",
		OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC), Data: json.RawMessage(`{"order_id":"order-1"}`),
		Contract: &ContractRef{Name: "OrderCreated", Version: "v1", SchemaHash: "abc"},
		Meta:     map[string]json.RawMessage{"producer": json.RawMessage(`"checkout-api"`), "future": json.RawMessage(`{"flag":true}`)},
	}
	encoded, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &top); err != nil {
		t.Fatal(err)
	}
	if _, ok := top["tenant_id"]; ok {
		t.Fatal("tenant_id must not be top-level")
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(top["meta"], &meta); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tenant_id", "partition_key", "producer", "future"} {
		if _, ok := meta[key]; !ok {
			t.Errorf("meta.%s missing", key)
		}
	}
	var decoded Envelope
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.TenantID != "merchant-42" || decoded.PartitionKey != "order-1" {
		t.Fatalf("identity not decoded: %#v", decoded)
	}
	if string(decoded.Meta["future"]) != `{"flag":true}` {
		t.Fatalf("future metadata not preserved: %s", decoded.Meta["future"])
	}
}

func TestEnvelopeRejectsTopLevelRuntimeMetadata(t *testing.T) {
	var env Envelope
	err := json.Unmarshal([]byte(`{"id":"evt-1","type":"orders.created","tenant_id":"merchant-42","partition_key":"order-1","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{}}`), &env)
	if err == nil {
		t.Fatal("expected top-level runtime identity to be rejected")
	}
}
