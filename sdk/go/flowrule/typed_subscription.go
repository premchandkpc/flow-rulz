package flowrulz

import (
	"context"
	"encoding/json"
	"fmt"
)

// SubscribeTyped decodes event payloads before invoking handler. It has the same
// delivery and cancellation semantics as Client.Subscribe.
func SubscribeTyped[T any](ctx context.Context, client *Client, ruleSet string, handler func(context.Context, *T, *Envelope) error) error {
	if client == nil {
		return ErrInvalidArgument("client is required")
	}
	if handler == nil {
		return ErrInvalidArgument("event handler is required")
	}
	return client.Subscribe(ctx, ruleSet, typedEventHandler(handler))
}

func typedEventHandler[T any](handler func(context.Context, *T, *Envelope) error) EventHandler {
	return func(ctx context.Context, envelope *Envelope) error {
		if envelope == nil {
			return ErrInvalidArgument("event envelope is required")
		}
		var payload T
		if err := json.Unmarshal(envelope.Data, &payload); err != nil {
			return fmt.Errorf("decode %s payload: %w", envelope.Type, err)
		}
		return handler(ctx, &payload, envelope)
	}
}
