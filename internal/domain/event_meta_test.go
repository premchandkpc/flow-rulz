package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEventEnvelopeSerializesIdentityAndExtensibleFieldsInsideMeta(t *testing.T) {
	event := EventEnvelope{
		ID: "evt-1", Type: "orders.created", TenantID: "merchant-42",
		PartitionKey: "order-1007", OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC),
		Data: json.RawMessage(`{"order_id":"order-1007"}`),
		Headers: map[string]string{"traceparent": "trace"},
		Meta: map[string]any{"region": "ap-south-1", "source_system": "checkout-api"},
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

func TestEventEnvelopeAcceptsLegacyIdentityDuringRollingUpgradeButWritesCanonicalMeta(t *testing.T) {
	const legacy = `{"id":"evt-1","type":"orders.created","tenant_id":"merchant-42","partition_key":"order-1007","occurred_at":"2026-10-15T07:30:00Z","data":{}}`
	var event EventEnvelope
	if err := json.Unmarshal([]byte(legacy), &event); err != nil {
		t.Fatalf("legacy Unmarshal() error = %v", err)
	}
	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("canonical Marshal() error = %v", err)
	}
	if strings.Contains(string(raw), `"tenant_id":`) && strings.Contains(string(raw), `"partition_key":`) {
		var wire map[string]any
		if err := json.Unmarshal(raw, &wire); err != nil {
			t.Fatal(err)
		}
		if _, exists := wire["tenant_id"]; exists {
			t.Fatalf("legacy tenant_id leaked to top level: %s", raw)
		}
		if _, exists := wire["partition_key"]; exists {
			t.Fatalf("legacy partition_key leaked to top level: %s", raw)
		}
	}
}

func TestEventEnvelopeRejectsUnknownTopLevelAndInvalidMetaIdentityTypes(t *testing.T) {
	tests := []string{
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"unexpected":true}`,
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":42,"partition_key":"order-1"}}`,
		`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":"tenant-a","partition_key":false}}`,
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
