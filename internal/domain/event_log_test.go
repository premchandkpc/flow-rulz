package domain

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDeterministicLogRecordID(t *testing.T) {
	first := DeterministicLogRecordID(LogRecordExecutionCommitted, "tenant-a", "order-1", "event-1", "orders@3")
	retry := DeterministicLogRecordID(LogRecordExecutionCommitted, "tenant-a", "order-1", "event-1", "orders@3")
	differentTenant := DeterministicLogRecordID(LogRecordExecutionCommitted, "tenant-b", "order-1", "event-1", "orders@3")
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
		Kind:     LogRecordEventAccepted, TenantID: "tenant-a", PartitionKey: "order-1",
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

func TestDeterministicLogRecordIDDoesNotConflateDelimiterValues(t *testing.T) {
	first := DeterministicLogRecordID(LogRecordEventAccepted, "tenant", "a\x00b", "event", "c")
	second := DeterministicLogRecordID(LogRecordEventAccepted, "tenant", "a", "b\x00event", "c")
	if first == second {
		t.Fatal("different field boundaries must produce different IDs")
	}
}

func TestEventLogRecordValidateRequiresCanonicalRecordID(t *testing.T) {
	record := EventLogRecord{
		RecordID: DeterministicLogRecordID(LogRecordEventAccepted, "tenant-a", "order-1", "event-1", ""),
		Kind: LogRecordEventAccepted, TenantID: "tenant-a", PartitionKey: "order-1",
		EventID: "event-1", Payload: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
	}
	record.RecordID = "arbitrary-id"
	if err := record.Validate(); err == nil {
		t.Fatal("record with a non-canonical deterministic ID accepted")
	}
}

func TestExecutionAndEffectRecordsRequireTheirIdentities(t *testing.T) {
	for _, tc := range []struct {
		name string
		kind LogRecordKind
	}{
		{name: "execution", kind: LogRecordExecutionCommitted},
		{name: "effect requested", kind: LogRecordEffectRequested},
		{name: "effect settled", kind: LogRecordEffectSettled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := EventLogRecord{
				RecordID: "not-canonical", Kind: tc.kind, TenantID: "tenant",
				PartitionKey: "key", EventID: "event",
				Payload: json.RawMessage(`{}`), OccurredAt: time.Now().UTC(),
			}
			if err := record.Validate(); err == nil {
				t.Fatalf("%s record without required identity accepted", tc.name)
			}
		})
	}
}
