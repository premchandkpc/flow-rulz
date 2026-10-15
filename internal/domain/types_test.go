package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestShardHashUsesStableFNV1aValues(t *testing.T) {
	tests := []struct {
		input string
		want  uint32
	}{
		{input: "", want: 2166136261},
		{input: "a", want: 3826002220},
		{input: "worker-test", want: 930350870},
	}

	for _, tt := range tests {
		if got := ShardHash(tt.input); got != tt.want {
			t.Errorf("ShardHash(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestComputeExecutionIDIsDeterministic(t *testing.T) {
	first := ComputeExecutionID("tenant-a", "event-123", "orders", 7)
	second := ComputeExecutionID("tenant-a", "event-123", "orders", 7)
	if first == "" {
		t.Fatal("execution ID must not be empty")
	}
	if first != second {
		t.Fatalf("same execution identity inputs produced different IDs: %q != %q", first, second)
	}
}

func TestComputeExecutionIDChangesWhenIdentityChanges(t *testing.T) {
	base := ComputeExecutionID("tenant-a", "event-123", "orders", 7)
	tests := []struct {
		name     string
		tenantID string
		eventID  string
		ruleSet  string
		revision int64
	}{
		{name: "tenant", tenantID: "tenant-b", eventID: "event-123", ruleSet: "orders", revision: 7},
		{name: "event", tenantID: "tenant-a", eventID: "event-456", ruleSet: "orders", revision: 7},
		{name: "rule set", tenantID: "tenant-a", eventID: "event-123", ruleSet: "payments", revision: 7},
		{name: "revision", tenantID: "tenant-a", eventID: "event-123", ruleSet: "orders", revision: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeExecutionID(tt.tenantID, tt.eventID, tt.ruleSet, tt.revision)
			if got == base {
				t.Fatalf("identity changed (%s) but execution ID stayed the same: %q", tt.name, got)
			}
		})
	}
}

func TestComputeChildEventIDIsTenantScoped(t *testing.T) {
	first := ComputeChildEventID("tenant-a", "evt-1", "rule", 0)
	same := ComputeChildEventID("tenant-a", "evt-1", "rule", 0)
	otherTenant := ComputeChildEventID("tenant-b", "evt-1", "rule", 0)
	if first != same {
		t.Fatal("child event ID must be deterministic for the same tenant and inputs")
	}
	if first == otherTenant {
		t.Fatal("child event IDs must not collide across tenants")
	}
}

func TestEventEnvelopeSerializesRuntimeIdentityInsideExtensibleMeta(t *testing.T) {
	event := EventEnvelope{
		ID: "evt-1", Type: "orders.created", TenantID: "merchant-42",
		PartitionKey: "order-1007", OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC),
		Data:    json.RawMessage(`{"order_id":"order-1007"}`),
		Meta:    map[string]json.RawMessage{"producer": json.RawMessage(`"checkout-api"`), "attempt": json.RawMessage(`2`)},
		Headers: map[string]string{"x-request-id": "req-1"},
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if _, exists := wire["tenant_id"]; exists {
		t.Fatal("tenant_id must not be top-level")
	}
	if _, exists := wire["partition_key"]; exists {
		t.Fatal("partition_key must not be top-level")
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(wire["meta"], &meta); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tenant_id", "partition_key", "producer", "attempt", "headers"} {
		if _, exists := meta[key]; !exists {
			t.Errorf("meta.%s missing from encoded event", key)
		}
	}
	if !strings.Contains(string(encoded), `"producer":"checkout-api"`) {
		t.Fatalf("unknown metadata not preserved: %s", encoded)
	}
}

func TestEventEnvelopeRoundTripsUnknownMetaFields(t *testing.T) {
	input := []byte(`{
		"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z",
		"contract":{"name":"OrderCreated","version":"v1","schema_hash":"abc"},
		"data":{"order_id":"order-1"},
		"meta":{"tenant_id":"merchant-42","partition_key":"order-1","producer":"checkout-api","future":{"flag":true}}
	}`)
	var event EventEnvelope
	if err := json.Unmarshal(input, &event); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if event.TenantID != "merchant-42" || event.PartitionKey != "order-1" {
		t.Fatalf("decoded identity = %q/%q", event.TenantID, event.PartitionKey)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatal(err)
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(got["meta"], &meta); err != nil {
		t.Fatal(err)
	}
	if string(meta["producer"]) != `"checkout-api"` || string(meta["future"]) != `{"flag":true}` {
		t.Fatalf("unknown meta fields were not preserved: %s", got["meta"])
	}
}

func TestEventEnvelopeRejectsLegacyTopLevelRuntimeMetadata(t *testing.T) {
	var event EventEnvelope
	err := json.Unmarshal([]byte(`{"id":"evt-1","type":"orders.created","tenant_id":"merchant-42","partition_key":"order-1","occurred_at":"2026-10-15T07:30:00Z","data":{}}`), &event)
	if err == nil {
		t.Fatal("expected legacy top-level tenant_id and partition_key to be rejected")
	}
}
