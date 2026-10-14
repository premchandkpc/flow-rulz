package effects

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type testPublisher struct {
	subject string
	payload []byte
}

func (p *testPublisher) PublishBatch(_ context.Context, _ []ports.PublishMessage) error {
	return nil
}

func (p *testPublisher) PublishToShard(_ context.Context, baseSubject string, shard uint32, data []byte) error {
	p.subject = fmt.Sprintf("%s.shard.%d", baseSubject, shard)
	p.payload = append([]byte(nil), data...)
	return nil
}

func (p *testPublisher) Close() error {
	return nil
}

type testDestination struct {
	sent bool
}

func (d *testDestination) Send(_ context.Context, _ *domain.Effect, _ string) error {
	d.sent = true
	return nil
}

func TestRouterPublishesChildEvent(t *testing.T) {
	childEnvelope := domain.EventEnvelope{
		ID: "evt-child", Type: "payment.requested", TenantID: "acme", PartitionKey: "order-42",
		OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"order_id":"order-42"}`),
	}
	child, err := json.Marshal(childEnvelope)
	if err != nil {
		t.Fatalf("marshal child: %v", err)
	}
	publisher := &testPublisher{}
	external := &testDestination{}
	router := NewRouterWithConfig(publisher, external, "events", 64)

	err = router.Send(context.Background(), &domain.Effect{
		EffectType:  domain.EffectTypeEmitEvent,
		Destination: "events.payment.requested",
		Payload:     child,
	}, "test-idempotency-key")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	// Routing subject is fixed; the business event type stays in the envelope.
	expectedSubject := fmt.Sprintf("events.shard.%d", childEnvelope.VirtualShard(64))
	if publisher.subject != expectedSubject {
		t.Fatalf("subject = %q, want %q", publisher.subject, expectedSubject)
	}
	if len(publisher.payload) == 0 {
		t.Fatal("publisher received an empty payload")
	}
	if external.sent {
		t.Fatal("external destination received an internal event")
	}
}

func TestRouterSendsScheduledIntentToDurableSender(t *testing.T) {
	publisher := &testPublisher{}
	command := &testDestination{}
	scheduled := &testDestination{}
	router := NewRouterWithConfig(publisher, command, "events", 64, nil, scheduled)

	err := router.Send(context.Background(), &domain.Effect{
		EffectType: domain.EffectTypeScheduleEvent,
		Destination: "scheduled_events",
		Name: "order.reminder",
		Payload: json.RawMessage(`{}`),
	}, "effect-schedule-1")
	if err != nil {
		t.Fatalf("send scheduled intent: %v", err)
	}
	if !scheduled.sent {
		t.Fatal("scheduled-event sender did not receive the intent")
	}
	if command.sent || publisher.subject != "" {
		t.Fatal("scheduled intent was sent to a command or event publisher")
	}
}
