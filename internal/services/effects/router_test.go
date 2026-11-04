package effects

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type testPublisher struct {
	event *domain.EventEnvelope
	err   error
}

func (p *testPublisher) PublishEvent(_ context.Context, event *domain.EventEnvelope) error {
	p.event = event
	return p.err
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
	router := NewRouter(publisher, external, nil, nil)

	err = router.Send(context.Background(), &domain.Effect{
		EffectType:  domain.EffectTypeEmitEvent,
		Destination: "events.payment.requested",
		Payload:     child,
	}, "test-idempotency-key")
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if publisher.event == nil || publisher.event.ID != childEnvelope.ID || publisher.event.Type != childEnvelope.Type {
		t.Fatalf("publisher received incorrect envelope: %+v", publisher.event)
	}
	if string(publisher.event.Data) != string(childEnvelope.Data) {
		t.Fatalf("data = %s, want %s", publisher.event.Data, childEnvelope.Data)
	}
	if external.sent {
		t.Fatal("external destination received an internal event")
	}
}

func TestRouterSendsScheduledIntentToDurableSender(t *testing.T) {
	publisher := &testPublisher{}
	command := &testDestination{}
	scheduled := &testDestination{}
	router := NewRouter(publisher, command, nil, scheduled)

	err := router.Send(context.Background(), &domain.Effect{
		EffectType:  domain.EffectTypeScheduleEvent,
		Destination: "scheduled_events",
		Name:        "order.reminder",
		Payload:     json.RawMessage(`{}`),
	}, "effect-schedule-1")
	if err != nil {
		t.Fatalf("send scheduled intent: %v", err)
	}
	if !scheduled.sent {
		t.Fatal("scheduled-event sender did not receive the intent")
	}
	if command.sent || publisher.event != nil {
		t.Fatal("scheduled intent was sent to a command or event publisher")
	}
}

func TestRouterPropagatesChildPublishFailure(t *testing.T) {
	want := errors.New("broker unavailable")
	publisher := &testPublisher{err: want}
	child, err := json.Marshal(domain.EventEnvelope{ID: "evt", Type: "test", TenantID: "a", PartitionKey: "key", OccurredAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	err = NewRouter(publisher, nil, nil, nil).Send(context.Background(), &domain.Effect{EffectType: domain.EffectTypeEmitEvent, Payload: child}, "effect-1")
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func TestRouterRejectsMalformedChildBeforePublishing(t *testing.T) {
	publisher := &testPublisher{}
	err := NewRouter(publisher, nil, nil, nil).Send(context.Background(), &domain.Effect{EffectType: domain.EffectTypeEmitEvent, Payload: json.RawMessage(`{`)}, "effect-1")
	if err == nil {
		t.Fatal("malformed event accepted")
	}
	if publisher.event != nil {
		t.Fatal("malformed event reached publisher")
	}
}
