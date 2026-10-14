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
	LogRecordEventAccepted LogRecordKind = "event.accepted"
	// LogRecordExecutionRecorded records a deterministic rule execution.
	LogRecordExecutionCommitted LogRecordKind = "execution.committed"
	// LogRecordEffectRequested records an effect intent.
	LogRecordEffectRequested LogRecordKind = "effect.requested"
	// LogRecordEffectSettled records an effect delivery outcome.
	LogRecordEffectSettled LogRecordKind = "effect.settled"
	// LogRecordEventQuarantined records a terminal processing failure.
	LogRecordEventQuarantined LogRecordKind = "event.quarantined"
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

// DeterministicLogRecordID returns a stable ID for a logical log fact. The
// discriminator should be a revision, effect ID, or settlement attempt identity
// when more than one fact of the same kind can exist for an event.
func DeterministicLogRecordID(kind LogRecordKind, tenantID, partitionKey, eventID, discriminator string) string {
	// Length-prefix each UTF-8 field. Delimiter-based encodings are ambiguous
	// when values themselves contain the delimiter.
	h := sha256.New()
	fields := []string{string(kind), tenantID, partitionKey, eventID, discriminator}
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

// Validate checks the required identity and payload invariants for a log record.
func (r EventLogRecord) Validate() error {
	if r.RecordID == "" || r.TenantID == "" || r.PartitionKey == "" || r.EventID == "" {
		return errors.New("log record requires record_id, tenant_id, partition_key, and event_id")
	}
	switch r.Kind {
	case LogRecordEventAccepted, LogRecordExecutionCommitted, LogRecordEffectRequested, LogRecordEffectSettled, LogRecordEventQuarantined:
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
