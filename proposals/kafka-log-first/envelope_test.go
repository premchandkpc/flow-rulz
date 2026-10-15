package logfirst

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEventEnvelopeUsesExtensibleMetaAndPreservesUnknownFields(t *testing.T) {
	event := EventEnvelope{
		SchemaVersion: 1,
		ID:            "evt-1", TenantID: "merchant-42", EventType: "orders.created",
		PartitionKey: "order-1007", OccurredAt: time.Date(2026, 10, 15, 7, 30, 0, 0, time.UTC),
		Payload:  json.RawMessage(`{"order_id":"order-1007"}`),
		Contract: &EventContract{Name: "OrderCreated", Version: "v1", SchemaHash: "abc"},
		Metadata: EventMetadata{
			TargetServiceID: "commerce/orders", DeploymentRevision: 3,
			Extra: map[string]json.RawMessage{
				"producer": json.RawMessage(`"checkout-api"`),
				"future":   json.RawMessage(`{"enabled":true}`),
			},
		},
	}
	encoded, err := json.Marshal(event)
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
	if _, ok := top["partition_key"]; ok {
		t.Fatal("partition_key must not be top-level")
	}
	if _, ok := top["payload"]; ok {
		t.Fatal("payload must use the public data field")
	}
	var meta map[string]json.RawMessage
	if err := json.Unmarshal(top["meta"], &meta); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"tenant_id", "partition_key", "producer", "future", "flow.service_id", "flow.deployment_revision", "schema_version"} {
		if _, ok := meta[key]; !ok {
			t.Errorf("meta.%s missing", key)
		}
	}
	var decoded EventEnvelope
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.TenantID != "merchant-42" || decoded.PartitionKey != "order-1007" || decoded.SchemaVersion != 1 {
		t.Fatalf("decoded event identity/version incorrect: %#v", decoded)
	}
	if string(decoded.Metadata.Extra["future"]) != `{"enabled":true}` {
		t.Fatalf("unknown metadata lost: %s", decoded.Metadata.Extra["future"])
	}
}

func TestEventEnvelopeRejectsTopLevelRuntimeIdentity(t *testing.T) {
	var event EventEnvelope
	err := json.Unmarshal([]byte(`{"id":"evt-1","type":"orders.created","tenant_id":"merchant-42","partition_key":"order-1","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{}}`), &event)
	if err == nil {
		t.Fatal("expected top-level runtime identity to be rejected")
	}
}
