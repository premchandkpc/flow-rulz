package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventEnvelopeSerializesAllMetadataInsideMeta(t *testing.T) {
	event := EventEnvelope{
		ID: "evt-1", Type: "orders.created", TenantID: "merchant-42", PartitionKey: "order-1007",
		WorkflowID: "workflow-1", OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC),
		Data:        json.RawMessage(`{"order_id":"order-1007"}`),
		MessageKind: "event", CorrelationID: "corr-1", CausationID: "evt-parent",
		Headers: map[string]string{"traceparent": "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"},
		Meta:    map[string]json.RawMessage{"source": json.RawMessage(`"checkout-api"`), "producer_sequence": json.RawMessage(`1042`)},
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"tenant_id", "partition_key", "workflow_id", "message_kind", "correlation_id", "causation_id", "headers"} {
		if _, ok := wire[forbidden]; ok {
			t.Fatalf("%s must be inside meta, got top-level envelope %s", forbidden, encoded)
		}
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(wire["meta"], &meta); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{"tenant_id": "merchant-42", "partition_key": "order-1007", "workflow_id": "workflow-1", "message_kind": "event", "correlation_id": "corr-1", "causation_id": "evt-parent", "source": "checkout-api"} {
		var got string
		if err := json.Unmarshal(meta[key], &got); err != nil || got != want {
			t.Fatalf("meta.%s = %q, want %q (err=%v)", key, got, want, err)
		}
	}
	var sequence int
	if err := json.Unmarshal(meta["producer_sequence"], &sequence); err != nil || sequence != 1042 {
		t.Fatalf("producer_sequence = %d (err=%v)", sequence, err)
	}
	var headers map[string]string
	if err := json.Unmarshal(meta["headers"], &headers); err != nil || headers["traceparent"] == "" {
		t.Fatalf("meta.headers = %#v (err=%v)", headers, err)
	}
}

func TestEventEnvelopeRoundTripsUnknownMetadataAndLegacyEvents(t *testing.T) {
	const input = `{
		"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z",
		"data":{"order_id":"order-1"},
		"meta":{"tenant_id":"tenant-a","partition_key":"order-1","producer_sequence":1042,"custom":{"region":"ap-south-1"}}
	}`
	var event EventEnvelope
	if err := json.Unmarshal([]byte(input), &event); err != nil {
		t.Fatal(err)
	}
	if event.TenantID != "tenant-a" || event.PartitionKey != "order-1" {
		t.Fatalf("runtime identity not decoded: %#v", event)
	}
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]json.RawMessage
	_ = json.Unmarshal(encoded, &wire)
	var meta map[string]json.RawMessage
	_ = json.Unmarshal(wire["meta"], &meta)
	var sequence int
	if err := json.Unmarshal(meta["producer_sequence"], &sequence); err != nil || sequence != 1042 {
		t.Fatalf("unknown scalar metadata lost: %s", encoded)
	}
	var custom map[string]string
	if err := json.Unmarshal(meta["custom"], &custom); err != nil || custom["region"] != "ap-south-1" {
		t.Fatalf("unknown object metadata lost: %s", encoded)
	}

	const legacy = `{"id":"evt-old","type":"orders.created","tenant_id":"tenant-old","partition_key":"order-old","occurred_at":"2026-10-15T07:30:00Z","data":{}}`
	var old EventEnvelope
	if err := json.Unmarshal([]byte(legacy), &old); err != nil {
		t.Fatal(err)
	}
	if old.TenantID != "tenant-old" || old.PartitionKey != "order-old" {
		t.Fatalf("legacy metadata not read: %#v", old)
	}
	normalized, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	var normalizedWire map[string]json.RawMessage
	_ = json.Unmarshal(normalized, &normalizedWire)
	if _, ok := normalizedWire["tenant_id"]; ok {
		t.Fatalf("legacy event must serialize using new meta format: %s", normalized)
	}
}
