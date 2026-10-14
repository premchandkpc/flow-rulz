package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// LogRecordKind identifies an immutable processing fact.
type LogRecordKind string

const (
	// LogRecordEventAccepted records an accepted input event.
	LogRecordEventAccepted       LogRecordKind = "event.accepted"
	// LogRecordExecutionCommitted records an atomic rule execution outcome and its complete effect-intent set.
	LogRecordExecutionCommitted  LogRecordKind = "execution.committed"
	// LogRecordEffectRequested records an effect intent.
	LogRecordEffectRequested     LogRecordKind = "effect.requested"
	// LogRecordEffectSettled records an effect delivery outcome.
	LogRecordEffectSettled       LogRecordKind = "effect.settled"
	// LogRecordEventQuarantined records a terminal processing failure.
	LogRecordEventQuarantined    LogRecordKind = "event.quarantined"
	// LogRecordScheduleRequested records a requested scheduled event.
	LogRecordScheduleRequested  LogRecordKind = "schedule.requested"
	// LogRecordScheduleCancelled records cancellation of a scheduled event.
	LogRecordScheduleCancelled LogRecordKind = "schedule.cancelled"
	// LogRecordScheduleFired records that a scheduled event became due.
	LogRecordScheduleFired      LogRecordKind = "schedule.fired"
	// LogRecordConsumerProgressed records durable consumer progress.
	LogRecordConsumerProgressed LogRecordKind = "consumer.progressed"
	// LogRecordFenceAdvanced records a monotonic partition fencing epoch.
	LogRecordFenceAdvanced      LogRecordKind = "fence.advanced"
)

// EventLogRecord is an immutable fact in the processing log. Sequence is assigned
// by the log adapter; RecordID is deterministic so replay can recognize the same
// logical fact even when a producer retries an append.
type EventLogRecord struct {
	RecordID     string          `json:"record_id"`
	Kind         LogRecordKind   `json:"kind"`
	TenantID     string          `json:"tenant_id"`
	PartitionKey string          `json:"partition_key"`
	EventID      string          `json:"event_id"`
	ExecutionID  string          `json:"execution_id,omitempty"`
	EffectID     string          `json:"effect_id,omitempty"`
	Payload      json.RawMessage `json:"payload"`
	OccurredAt   time.Time       `json:"occurred_at"`
}

// DeterministicID hashes a namespace and any number of length-prefixed fields.
// It is shared with the Rust runtime to keep identities byte-for-byte compatible.
func DeterministicID(namespace string, parts ...string) string {
	h := sha256.New()
	fields := make([]string, 1, len(parts)+1)
	fields[0] = namespace
	fields = append(fields, parts...)
	var length [8]byte
	for _, field := range fields {
		n := uint64(len(field))
		for i := len(length) - 1; i >= 0; i-- {
			length[i] = byte(n)
			n >>= 8
		}
		_, _ = h.Write(length[:])
		_, _ = h.Write([]byte(field))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// DeterministicLogRecordID returns a stable ID for a logical log fact.
// The discriminator is empty for one-per-event facts and identifies effect facts.
func DeterministicLogRecordID(kind LogRecordKind, tenantID, partitionKey, eventID, discriminator string) string {
	return DeterministicID(string(kind), tenantID, partitionKey, eventID, discriminator)
}

// Validate checks the required identity and payload invariants for a log record.
func (r EventLogRecord) Validate() error {
	if r.RecordID == "" || r.TenantID == "" || r.PartitionKey == "" || r.EventID == "" {
		return errors.New("log record requires record_id, tenant_id, partition_key, and event_id")
	}
	switch r.Kind {
	case LogRecordEventAccepted, LogRecordExecutionCommitted, LogRecordEffectRequested, LogRecordEffectSettled, LogRecordEventQuarantined, LogRecordScheduleRequested, LogRecordScheduleCancelled, LogRecordScheduleFired, LogRecordConsumerProgressed, LogRecordFenceAdvanced:
	default:
		return fmt.Errorf("unsupported log record kind %q", r.Kind)
	}
	if !json.Valid(r.Payload) {
		return errors.New("log record payload must be valid JSON")
	}
	if r.OccurredAt.IsZero() {
		return errors.New("log record occurred_at is required")
	}
	if r.Kind == LogRecordExecutionCommitted && r.ExecutionID == "" {
		return errors.New("execution.committed requires execution_id")
	}
	if (r.Kind == LogRecordEffectRequested || r.Kind == LogRecordEffectSettled) && r.EffectID == "" {
		return errors.New("effect record requires effect_id")
	}
	if r.Kind == LogRecordExecutionCommitted {
		var committed ExecutionCommittedPayload
		if err := json.Unmarshal(r.Payload, &committed); err != nil {
			return fmt.Errorf("execution.committed payload is invalid: %w", err)
		}
		expectedExecutionID := DeterministicID("execution", r.TenantID, r.PartitionKey, r.EventID)
		if committed.ExecutionID != r.ExecutionID || committed.ExecutionID != expectedExecutionID {
			return errors.New("execution.committed execution_id must match its deterministic event identity")
		}
		if err := committed.Validate(r.TenantID, r.EventID); err != nil {
			return fmt.Errorf("invalid execution.committed payload: %w", err)
		}
	}
	discriminator := ""
	switch r.Kind {
	case LogRecordExecutionCommitted:
		discriminator = r.ExecutionID
	case LogRecordEffectRequested, LogRecordEffectSettled:
		discriminator = r.EffectID
	}
	expected := DeterministicLogRecordID(r.Kind, r.TenantID, r.PartitionKey, r.EventID, discriminator)
	if r.RecordID != expected {
		return fmt.Errorf("record_id does not match deterministic identity for kind %q", r.Kind)
	}
	return nil
}
