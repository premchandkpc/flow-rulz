package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDeterministicLogRecordID(t *testing.T) {
	first := DeterministicLogRecordID(LogRecordExecutionRecorded, "tenant-a", "order-1", "event-1", "orders@3")
	retry := DeterministicLogRecordID(LogRecordExecutionRecorded, "tenant-a", "order-1", "event-1", "orders@3")
	differentTenant := DeterministicLogRecordID(LogRecordExecutionRecorded, "tenant-b", "order-1", "event-1", "orders@3")
	if first != retry {
		t.Fatal("same logical fact must have the same ID")
	}
	if first == differentTenant {
		t.Fatal("tenant scope must be part of the deterministic ID")
	}
}

func TestEventLogRecordValidate(t *testing.T) {
	record := EventLogRecord{
		RecordID: DeterministicLogRecordID(LogRecordEventAccepted, "tenant-a", "order-1", "event-1", ""),
		Kind: LogRecordEventAccepted, TenantID: "tenant-a", PartitionKey: "order-1",
		EventID: "event-1", Payload: json.RawMessage(`{"type":"orders.created"}`),
		OccurredAt: time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC),
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("valid record rejected: %v", err)
	}
	record.Payload = json.RawMessage(`{"broken":`)
	if err := record.Validate(); err == nil {
		t.Fatal("invalid JSON payload accepted")
	}
}

func TestEffectRecordRequiresEffectID(t *testing.T) {
	record := EventLogRecord{
		RecordID: "record-1", Kind: LogRecordEffectRequested, TenantID: "tenant-a",
		PartitionKey: "order-1", EventID: "event-1", Payload: json.RawMessage(`{}`),
		OccurredAt: time.Now().UTC(),
	}
	if err := record.Validate(); err == nil {
		t.Fatal("effect record without effect_id accepted")
	}
}
