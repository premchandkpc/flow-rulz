package effects

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type recordingEffectEnvelopePublisher struct {
	event *domain.EventEnvelope
	err error
}

func (p *recordingEffectEnvelopePublisher) PublishEvent(_ context.Context, event *domain.EventEnvelope) error {
	p.event = event
	return p.err
}

func TestEnvelopeRouterPublishesChildEvent(t *testing.T) {
	event := domain.EventEnvelope{
		ID: "child-1", Type: "payment.completed", TenantID: "tenant-a",
		PartitionKey: "account-1", OccurredAt: time.Now().UTC(), Data: json.RawMessage("{}"),
	}
	data, err := json.Marshal(event)
	if err != nil { t.Fatal(err) }
	publisher := &recordingEffectEnvelopePublisher{}
	router := NewEnvelopeRouter(publisher, nil, nil, nil)
	if err := router.Send(context.Background(), &domain.Effect{EffectType: domain.EffectTypeEmitEvent, Payload: data}, "effect-1"); err != nil {
		t.Fatal(err)
	}
	if publisher.event == nil || publisher.event.ID != event.ID || publisher.event.Type != event.Type {
		t.Fatalf("publisher received wrong event: %+v", publisher.event)
	}
}

func TestEnvelopeRouterPropagatesPublishError(t *testing.T) {
	want := errors.New("broker unavailable")
	event := domain.EventEnvelope{
		ID: "child-1", Type: "payment.completed", TenantID: "tenant-a",
		PartitionKey: "account-1", OccurredAt: time.Now().UTC(),
	}
	data, err := json.Marshal(event)
	if err != nil { t.Fatal(err) }
	publisher := &recordingEffectEnvelopePublisher{err: want}
	router := NewEnvelopeRouter(publisher, nil, nil, nil)
	if err := router.Send(context.Background(), &domain.Effect{EffectType: domain.EffectTypeEmitEvent, Payload: data}, "effect-1"); !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
