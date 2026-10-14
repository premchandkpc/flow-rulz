package effects

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type commandPublisherStub struct {
	subject string
	payload []byte
	err     error
}

func (p *commandPublisherStub) Publish(_ context.Context, subject string, payload []byte) error {
	p.subject = subject
	p.payload = append([]byte(nil), payload...)
	return p.err
}

func TestNATSDestinationPublishesCommandWithStableIdempotencyKey(t *testing.T) {
	publisher := &commandPublisherStub{}
	destination := NewNATSDestination(publisher)
	err := destination.Send(context.Background(), &domain.Effect{
		ID: "effect-1", ExecutionID: "execution-1", TenantID: "tenant-a",
		Destination: "payment", Name: "authorize",
		EffectType: domain.EffectTypeCommand, Payload: json.RawMessage(`{"order_id":"order-1"}`),
	}, "effect-effect-1")
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got, want := publisher.subject, "commands.payment.authorize"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}
	var delivered domain.Effect
	if err := json.Unmarshal(publisher.payload, &delivered); err != nil {
		t.Fatalf("decode command: %v", err)
	}
	if delivered.ID != "effect-1" || delivered.IdempotencyKey != "effect-effect-1" {
		t.Fatalf("delivery identity = (%q, %q), want stable effect identity", delivered.ID, delivered.IdempotencyKey)
	}
}

func TestNATSDestinationRejectsWildcardOrMalformedSubjects(t *testing.T) {
	for _, destination := range []string{"", "payment.*", "payment>", "bad subject"} {
		t.Run(destination, func(t *testing.T) {
			destinationSender := NewNATSDestination(&commandPublisherStub{})
			err := destinationSender.Send(context.Background(), &domain.Effect{
				ID: "effect-1", Destination: destination, Name: "authorize",
				EffectType: domain.EffectTypeCommand, Payload: json.RawMessage(`{}`),
			}, "effect-effect-1")
			if err == nil {
				t.Fatal("Send() error = nil, want invalid destination error")
			}
		})
	}
}

func TestNATSDestinationReturnsPublishErrorForOutboxRetry(t *testing.T) {
	destination := NewNATSDestination(&commandPublisherStub{err: errors.New("broker unavailable")})
	err := destination.Send(context.Background(), &domain.Effect{
		ID: "effect-1", Destination: "payment", Name: "authorize",
		EffectType: domain.EffectTypeCommand, Payload: json.RawMessage(`{}`),
	}, "effect-effect-1")
	if err == nil || !strings.Contains(err.Error(), "publish effect") {
		t.Fatalf("Send() error = %v, want wrapped publish error", err)
	}
}

func TestNATSDestinationRoutesEmitTopicAsEffectsSubject(t *testing.T) {
	publisher := &commandPublisherStub{}
	destination := NewNATSDestination(publisher)
	err := destination.Send(context.Background(), &domain.Effect{
		ID: "effect-2", Destination: "orders.created", Name: "orders.created",
		EffectType: domain.EffectTypeEmit, Payload: json.RawMessage(`{"order_id":"order-2"}`),
	}, "effect-effect-2")
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if got, want := publisher.subject, "effects.orders.created"; got != want {
		t.Fatalf("subject = %q, want %q", got, want)
	}
}

func TestNATSDestinationRequiresStableDeliveryIdentity(t *testing.T) {
	destination := NewNATSDestination(&commandPublisherStub{})
	err := destination.Send(context.Background(), &domain.Effect{
		Destination: "payment", Name: "authorize", EffectType: domain.EffectTypeCommand,
		Payload: json.RawMessage(`{}`),
	}, "")
	if err == nil || !strings.Contains(err.Error(), "stable effect ID") {
		t.Fatalf("Send() error = %v, want stable identity validation", err)
	}
}
