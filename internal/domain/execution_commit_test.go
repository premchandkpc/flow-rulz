package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validExecutionCommittedRecord(t *testing.T) EventLogRecord {
	t.Helper()

	const tenantID = "tenant-a"
	const eventID = "event-1"
	executionID := DeterministicLogRecordID(LogRecordExecutionCommitted, tenantID, "order-1", eventID, "execution")
	effectPayload := json.RawMessage(`{"type":"orders.approved","order_id":"order-1"}`)
	commit := ExecutionCommittedPayload{
		ExecutionID:  executionID,
		Outcome:      "completed",
		DecisionHash: strings.Repeat("a", 64),
		RuleRevisions: []LogRuleRevisionRef{{
			TenantID: tenantID, RuleSet: "orders", Revision: 3,
			ContentHash: strings.Repeat("b", 64), CompilerVersion: "flowrulz-compiler-v1",
		}},
		MatchedRuleIDs: []string{"approve-order"},
		ResultPayload:  json.RawMessage(`{"approved":true}`),
		Effects: []LogEffectIntent{{
			EffectID: effectID, EffectKey: effectKey, TenantID: tenantID, SourceEventID: eventID,
			ExecutionID: executionID,
			OutputContract: LogContractRef{
				Name: "orders.approved", Version: "1",
				ContentHash: strings.Repeat("c", 64),
			},
			Route:         LogOutputRoute{Kind: "consumer", Destination: "orders.approved"},
			Kind:          "publish_event",
			Payload:       effectPayload,
			PayloadSHA256: payloadSHA256(effectPayload),
			PartitionKey:  "order-1",
		}},
		FencingEpoch: 1,
	}
	payload, err := json.Marshal(commit)
	if err != nil {
		t.Fatalf("marshal execution commit: %v", err)
	}
	return EventLogRecord{
		RecordID: DeterministicLogRecordID(LogRecordExecutionCommitted, tenantID, "order-1", eventID, executionID),
		Kind:     LogRecordExecutionCommitted, TenantID: tenantID, PartitionKey: "order-1",
		EventID: eventID, ExecutionID: executionID, Payload: payload,
		OccurredAt: time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC),
	}
}

func TestExecutionCommittedRecordValidate(t *testing.T) {
	record := validExecutionCommittedRecord(t)
	if err := record.Validate(); err != nil {
		t.Fatalf("valid execution commit rejected: %v", err)
	}
}

func TestExecutionCommittedRequiresExplicitEffectsArray(t *testing.T) {
	record := validExecutionCommittedRecord(t)
	var commit ExecutionCommittedPayload
	if err := json.Unmarshal(record.Payload, &commit); err != nil {
		t.Fatal(err)
	}
	commit.Effects = nil
	record.Payload = marshalCommittedPayload(t, commit)
	if err := record.Validate(); err == nil {
		t.Fatal("execution commit without an explicit effects array accepted")
	}
}

func TestExecutionCommittedRejectsMismatchedEffectPayloadHash(t *testing.T) {
	record := validExecutionCommittedRecord(t)
	var commit ExecutionCommittedPayload
	if err := json.Unmarshal(record.Payload, &commit); err != nil {
		t.Fatal(err)
	}
	commit.Effects[0].Payload = json.RawMessage(`{"type":"orders.rejected"}`)
	record.Payload = marshalCommittedPayload(t, commit)
	if err := record.Validate(); err == nil {
		t.Fatal("effect payload changed without changing payload_sha256")
	}
}

func TestExecutionCommittedRejectsCrossTenantEffect(t *testing.T) {
	record := validExecutionCommittedRecord(t)
	var commit ExecutionCommittedPayload
	if err := json.Unmarshal(record.Payload, &commit); err != nil {
		t.Fatal(err)
	}
	commit.Effects[0].TenantID = "tenant-b"
	record.Payload = marshalCommittedPayload(t, commit)
	if err := record.Validate(); err == nil {
		t.Fatal("cross-tenant effect accepted")
	}
}

func TestExecutionCommittedRejectsEffectFromAnotherExecution(t *testing.T) {
	record := validExecutionCommittedRecord(t)
	var commit ExecutionCommittedPayload
	if err := json.Unmarshal(record.Payload, &commit); err != nil {
		t.Fatal(err)
	}
	commit.Effects[0].ExecutionID = "execution-2"
	record.Payload = marshalCommittedPayload(t, commit)
	if err := record.Validate(); err == nil {
		t.Fatal("effect for another execution accepted")
	}
}

func TestLogOutputRouteRejectsAmbiguousTargets(t *testing.T) {
	route := LogOutputRoute{Kind: "consumer", Destination: "orders.approved", RuleSet: "another-rule-set"}
	if err := route.Validate(); err == nil {
		t.Fatal("route with multiple targets accepted")
	}
}

func marshalCommittedPayload(t *testing.T, commit ExecutionCommittedPayload) json.RawMessage {
	t.Helper()
	payload, err := json.Marshal(commit)
	if err != nil {
		t.Fatalf("marshal execution commit payload: %v", err)
	}
	return payload
}

func TestExecutionCommittedRejectsNondeterministicExecutionID(t *testing.T) {
	record := validExecutionCommittedRecord(t)
	var commit ExecutionCommittedPayload
	if err := json.Unmarshal(record.Payload, &commit); err != nil {
		t.Fatal(err)
	}
	commit.ExecutionID = "random-execution-id"
	commit.Effects[0].ExecutionID = commit.ExecutionID
	record.ExecutionID = commit.ExecutionID
	record.Payload = marshalCommittedPayload(t, commit)
	if err := record.Validate(); err == nil {
		t.Fatal("execution ID unrelated to its source event accepted")
	}
}
