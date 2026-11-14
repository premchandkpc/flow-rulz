package flowrulz

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type typedOrder struct {
	OrderID string `json:"order_id"`
}

func TestTypedHandlerPreservesEnvelopeAndHandlerError(t *testing.T) {
	envelope := &Envelope{Type: "order.created", Data: json.RawMessage(`{"order_id":"order-1"}`)}
	want := errors.New("handler failed")
	handler := func(ctx context.Context, order *typedOrder, received *Envelope) error {
		if ctx != context.Background() || order.OrderID != "order-1" || received != envelope {
			t.Fatalf("handler received incorrect context, payload or envelope: %+v", order)
		}
		return want
	}
	if err := typedEventHandler(handler)(context.Background(), envelope); !errors.Is(err, want) {
		t.Fatalf("handler error = %v, want %v", err, want)
	}
}

func TestTypedHandlerRejectsInvalidEnvelope(t *testing.T) {
	handler := typedEventHandler(func(context.Context, *typedOrder, *Envelope) error {
		t.Fatal("invalid envelope reached handler")
		return nil
	})
	for _, envelope := range []*Envelope{nil, {Type: "order.created", Data: json.RawMessage(`{`)}} {
		if err := handler(context.Background(), envelope); err == nil {
			t.Fatal("expected decoding error")
		}
	}
}

func TestSubscribeTypedRejectsNilDependencies(t *testing.T) {
	handler := func(context.Context, *typedOrder, *Envelope) error { return nil }
	if err := SubscribeTyped(context.Background(), nil, "orders", handler); err == nil {
		t.Fatal("nil client accepted")
	}
	if err := SubscribeTyped[typedOrder](context.Background(), &Client{}, "orders", nil); err == nil {
		t.Fatal("nil handler accepted")
	}
}
