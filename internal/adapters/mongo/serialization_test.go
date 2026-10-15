package mongo

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"go.mongodb.org/mongo-driver/bson"
)

func mongoBSONRoundTrip(t *testing.T, input, output any) {
	t.Helper()
	raw, err := bson.Marshal(input)
	if err != nil {
		t.Fatalf("bson.Marshal(%T): %v", input, err)
	}
	if err := bson.Unmarshal(raw, output); err != nil {
		t.Fatalf("bson.Unmarshal(%T): %v", output, err)
	}
}

func TestJSONPayloadDocumentsRoundTripThroughBSON(t *testing.T) {
	t.Run("event data and headers", func(t *testing.T) {
		source := &domain.EventEnvelope{
			ID: "evt-1", TenantID: "tenant-a", Type: "order.created", PartitionKey: "order-1",
			OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"amount":42,"nested":{"ok":true}}`),
			Headers:     map[string]string{"trace-id": "trace-1"},
			Meta:        map[string]json.RawMessage{"producer": json.RawMessage(`"checkout-api"`), "future": json.RawMessage(`{"flag":true}`)},
			Contract:    &domain.ContractRef{Name: "OrderCreated", Version: "v1", SchemaHash: "hash"},
			MessageKind: "event", CorrelationID: "corr-1", CausationID: "parent-1",
		}
		var stored EventDoc
		mongoBSONRoundTrip(t, EventDocFromDomain(source), &stored)
		got := stored.ToDomain()
		if !bytes.Equal(got.Data, source.Data) {
			t.Fatalf("event data = %s, want %s", got.Data, source.Data)
		}
		if got.Headers["trace-id"] != "trace-1" {
			t.Fatalf("event headers = %#v", got.Headers)
		}
		if string(got.Meta["producer"]) != `"checkout-api"` || string(got.Meta["future"]) != `{"flag":true}` {
			t.Fatalf("event metadata = %#v", got.Meta)
		}
		if got.Contract == nil || got.Contract.Name != "OrderCreated" || got.MessageKind != "event" ||
			got.CorrelationID != "corr-1" || got.CausationID != "parent-1" {
			t.Fatalf("event metadata fields were not persisted: %#v", got)
		}
	})

	t.Run("inbox payload", func(t *testing.T) {
		source := &domain.InboxEntry{
			TenantID: "tenant-a", EventID: "evt-2", Payload: json.RawMessage(`{"inbox":true}`),
			FirstSeenAt: time.Now().UTC(),
		}
		var stored InboxDoc
		mongoBSONRoundTrip(t, InboxDocFromDomain(source), &stored)
		if got := stored.ToDomain().Payload; !bytes.Equal(got, source.Payload) {
			t.Fatalf("inbox payload = %s, want %s", got, source.Payload)
		}
	})

	t.Run("outbox payload", func(t *testing.T) {
		source := &domain.OutboxEffect{
			ID: "effect-1", ExecutionID: "exec-1", EffectType: domain.EffectTypeCommand,
			Payload: json.RawMessage(`{"outbox":"payload"}`), CreatedAt: time.Now().UTC(),
		}
		var stored OutboxDoc
		mongoBSONRoundTrip(t, OutboxDocFromDomain(source), &stored)
		if got := stored.ToDomain().Payload; !bytes.Equal(got, source.Payload) {
			t.Fatalf("outbox payload = %s, want %s", got, source.Payload)
		}
	})

	t.Run("scheduled event payload and headers", func(t *testing.T) {
		source := &domain.ScheduledEvent{
			EventID: "scheduled-1", TenantID: "tenant-a", EventType: "order.reminder",
			PartitionKey: "order-1", Payload: json.RawMessage(`{"scheduled":true}`),
			Headers: map[string]string{"origin": "scheduler"}, ScheduledAt: time.Now().UTC(),
		}
		var stored ScheduledDoc
		mongoBSONRoundTrip(t, ScheduledDocFromDomain(source), &stored)
		got := stored.ToDomain()
		if !bytes.Equal(got.Payload, source.Payload) {
			t.Fatalf("scheduled payload = %s, want %s", got.Payload, source.Payload)
		}
		if got.Headers["origin"] != "scheduler" {
			t.Fatalf("scheduled headers = %#v", got.Headers)
		}
	})

	t.Run("workflow context", func(t *testing.T) {
		source := &domain.WorkflowInstance{
			WorkflowID: "workflow-1", TenantID: "tenant-a", WorkflowType: "order",
			State: "pending", Context: json.RawMessage(`{"context":{"attempt":2},"correlation_id":"corr-1"}`),
			CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		}
		var stored WorkflowDoc
		mongoBSONRoundTrip(t, WorkflowDocFromDomain(source), &stored)
		if got := stored.ToDomain().Context; !bytes.Equal(got, source.Context) {
			t.Fatalf("workflow context = %s, want %s", got, source.Context)
		}
		if stored.CorrelationID != "corr-1" {
			t.Fatalf("stored correlation ID = %q, want corr-1", stored.CorrelationID)
		}
	})

	t.Run("rule source and compiled rules", func(t *testing.T) {
		source := &domain.RuleRevision{
			TenantScope: "tenant-a", RuleID: "orders.created", Revision: 1,
			Source:   json.RawMessage(`{"rule_set":"orders.created","revision":1,"rules":[]}`),
			Compiled: []domain.CompiledRule{},
		}
		var stored RevisionDoc
		mongoBSONRoundTrip(t, RevisionDocFromDomain(source.TenantScope, source), &stored)
		got := stored.ToDomain()
		if !bytes.Equal(got.Source, source.Source) {
			t.Fatalf("rule source = %s, want %s", got.Source, source.Source)
		}
		if len(got.Compiled) != 0 {
			t.Fatalf("compiled rule count = %d, want 0", len(got.Compiled))
		}
	})
}
