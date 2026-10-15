package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventEnvelopeSerializesIdentityAndExtensibleFieldsInsideMeta(t *testing.T) {
	event := EventEnvelope{
		ID: "evt-1", Type: "orders.created", TenantID: "merchant-42",
		PartitionKey: "order-1007", OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC),
		Data:    json.RawMessage(`{"order_id":"order-1007"}`),
		Headers: map[string]string{"traceparent": "trace"},
		Meta:    map[string]any{"region": "ap-south-1", "source_system": "checkout-api"},
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatalf("unmarshal serialized event: %v", err)
	}
	if _, exists := wire["tenant_id"]; exists {
		t.Fatal("tenant_id must not be a top-level envelope field")
	}
	if _, exists := wire["partition_key"]; exists {
		t.Fatal("partition_key must not be a top-level envelope field")
	}
	meta, ok := wire["meta"].(map[string]any)
	if !ok {
		t.Fatalf("meta has type %T, want object", wire["meta"])
	}
	for key, want := range map[string]string{
		"tenant_id": "merchant-42", "partition_key": "order-1007",
		"region": "ap-south-1", "source_system": "checkout-api",
	} {
		if got := meta[key]; got != want {
			t.Errorf("meta[%q] = %#v, want %q", key, got, want)
		}
	}
	if _, exists := wire["headers"]; !exists {
		t.Fatal("transport headers should remain in headers")
	}
}

func TestEventEnvelopeUnmarshalsMetaAndPreservesUnknownMetadata(t *testing.T) {
	const payload = `{
		"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z",
		"data":{"order_id":"order-1007"},
		"meta":{"tenant_id":"merchant-42","partition_key":"order-1007","region":"ap-south-1","future_context":{"source":"checkout"}}
	}`
	var event EventEnvelope
	if err := json.Unmarshal([]byte(payload), &event); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if event.TenantID != "merchant-42" || event.PartitionKey != "order-1007" {
		t.Fatalf("runtime identity = tenant %q / partition %q", event.TenantID, event.PartitionKey)
	}
	if event.Meta["region"] != "ap-south-1" {
		t.Fatalf("region metadata lost: %#v", event.Meta)
	}
	future, ok := event.Meta["future_context"].(map[string]any)
	if !ok || future["source"] != "checkout" {
		t.Fatalf("future metadata lost: %#v", event.Meta["future_context"])
	}
}

func TestEventEnvelopeRejectsTopLevelRuntimeIdentity(t *testing.T) {
	tests := []string{
		`{"id":"evt-1","type":"orders.created","tenant_id":"merchant-42","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"partition_key":"order-1007","tenant_id":"merchant-42"}}`,
		`{"id":"evt-1","type":"orders.created","partition_key":"order-1007","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"partition_key":"order-1007","tenant_id":"merchant-42"}}`,
		`{"id":"evt-1","type":"orders.created","tenant_id":"merchant-42","partition_key":"order-1007","occurred_at":"2026-10-15T07:30:00Z","data":{}}`,
	}
	for _, payload := range tests {
		var event EventEnvelope
		if err := json.Unmarshal([]byte(payload), &event); err == nil {
			t.Errorf("Unmarshal() accepted top-level runtime identity: %s", payload)
		}
	}
}

func TestEventEnvelopeMarshalsIdentityFromMetaWhenTypedFieldsAreUnset(t *testing.T) {
	event := EventEnvelope{
		ID: "evt-1", Type: "orders.created",
		OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC),
		Data:       json.RawMessage(`{}`),
		Meta: map[string]any{
			"tenant_id":      "merchant-42",
			"partition_key":  "order-1007",
			"future_context": map[string]any{"source": "checkout"},
		},
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var roundTrip EventEnvelope
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	if roundTrip.TenantID != "merchant-42" || roundTrip.PartitionKey != "order-1007" {
		t.Fatalf("runtime identity was not read from meta: %#v", roundTrip)
	}
	if roundTrip.Meta["future_context"] == nil {
		t.Fatalf("unknown metadata was not preserved: %#v", roundTrip.Meta)
	}
}

func TestEventEnvelopeRejectsConflictingTypedAndMetaIdentity(t *testing.T) {
	event := EventEnvelope{
		ID: "evt-1", Type: "orders.created", TenantID: "tenant-a", PartitionKey: "order-1",
		OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC), Data: json.RawMessage(`{}`),
		Meta: map[string]any{"tenant_id": "tenant-b", "partition_key": "order-1"},
	}
	if _, err := json.Marshal(event); err == nil {
		t.Fatal("Marshal() accepted conflicting tenant identity")
	}
}

func TestEventEnvelopeRejectsUnknownTopLevelAndInvalidMetaIdentityTypes(t *testing.T) {
	tests := []string{
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"unexpected":true}`,
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":42,"partition_key":"order-1"}}`,
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":"tenant-a","partition_key":false}}`,
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":"tenant-a"}}`,
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":" ","partition_key":"order-1"}}`,
	}
	for i, payload := range tests {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			var event EventEnvelope
			if err := json.Unmarshal([]byte(payload), &event); err == nil {
				t.Fatalf("Unmarshal() accepted invalid envelope: %s", payload)
			}
		})
	}
}
