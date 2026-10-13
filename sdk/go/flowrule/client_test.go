package flowrulz

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeDataUsesRawJSON(t *testing.T) {
	const payload = `{"total_amount":2500}`
	var envelope Envelope
	if err := json.Unmarshal([]byte(`{"data":{"total_amount":2500}}`), &envelope); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if string(envelope.Data) != payload {
		t.Fatalf("Envelope.Data = %s, want %s", envelope.Data, payload)
	}

	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var roundTrip Envelope
	if err := json.Unmarshal(encoded, &roundTrip); err != nil {
		t.Fatalf("round-trip json.Unmarshal() error = %v", err)
	}
	if string(roundTrip.Data) != payload {
		t.Fatalf("round-tripped Envelope.Data = %s, want %s", roundTrip.Data, payload)
	}
}

func TestSubscribeRejectsUnsupportedTransport(t *testing.T) {
	client := &Client{transport: &httpTransport{}}

	err := client.Subscribe(context.Background(), "order.created", func(context.Context, *Envelope) error {
		return nil
	})
	if err != ErrSubscriptionsUnsupported {
		t.Fatalf("Subscribe() error = %v, want %v", err, ErrSubscriptionsUnsupported)
	}
}

func TestCloseCancelsActiveSubscription(t *testing.T) {
	lifecycleCtx, lifecycleClose := context.WithCancel(context.Background())
	transport := &subscriptionTestTransport{started: make(chan struct{})}
	client := &Client{
		transport:      transport,
		lifecycleCtx:   lifecycleCtx,
		lifecycleClose: lifecycleClose,
	}

	subscribeDone := make(chan error, 1)
	go func() {
		subscribeDone <- client.Subscribe(context.Background(), "order.created", func(context.Context, *Envelope) error {
			return nil
		})
	}()

	select {
	case <-transport.started:
	case <-time.After(time.Second):
		t.Fatal("subscription did not start")
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-subscribeDone:
		if err != context.Canceled {
			t.Fatalf("Subscribe() error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel the active subscription")
	}
}

type subscriptionTestTransport struct {
	started chan struct{}
}

func (t *subscriptionTestTransport) Send(context.Context, *Envelope) error { return nil }

func (t *subscriptionTestTransport) SendBatch(context.Context, []*Envelope) error { return nil }

func (t *subscriptionTestTransport) Close() error { return nil }

func (t *subscriptionTestTransport) Name() string { return "test" }

func (t *subscriptionTestTransport) Subscribe(ctx context.Context, _ string, _ EventHandler) error {
	close(t.started)
	<-ctx.Done()
	return ctx.Err()
}
