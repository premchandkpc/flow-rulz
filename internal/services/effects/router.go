// Package effects provides effect routing.
package effects

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Router dispatches effects through transport-neutral delivery ports.
type Router struct {
	publisher ports.EventPublisher
	command   ports.EffectSender
	service   ports.EffectSender
	scheduled ports.EffectSender
}

// NewRouter wires explicit senders for each effect destination. The event
// publisher owns broker routing; the router only decodes the child envelope.
func NewRouter(publisher ports.EventPublisher, command, service, scheduled ports.EffectSender) *Router {
	return &Router{publisher: publisher, command: command, service: service, scheduled: scheduled}
}

// Send routes an effect to the appropriate destination based on its type.
// Broker subjects and shard selection belong to the configured adapters.
func (r *Router) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if effect == nil {
		return fmt.Errorf("send effect: effect is nil")
	}
	if effect.EffectType == domain.EffectTypeScheduleEvent {
		if r.scheduled == nil {
			return fmt.Errorf("schedule event: scheduled-event sender is not configured")
		}
		return r.scheduled.Send(ctx, effect, idempotencyKey)
	}
	if effect.EffectType != domain.EffectTypeEmitEvent {
		if effect.EffectType == domain.EffectTypeServiceEvent {
			if r.service == nil {
				return fmt.Errorf("send subscribed service event: service sender is not configured")
			}
			return r.service.Send(ctx, effect, idempotencyKey)
		}
		if r.command == nil {
			return fmt.Errorf("send command or emit effect: sender is not configured")
		}
		return r.command.Send(ctx, effect, idempotencyKey)
	}
	if r.publisher == nil {
		return fmt.Errorf("publish child event: broker publisher is not configured")
	}
	var child domain.EventEnvelope
	if err := json.Unmarshal(effect.Payload, &child); err != nil {
		return fmt.Errorf("decode child event: %w", err)
	}
	if err := child.Validate(); err != nil {
		return fmt.Errorf("validate child event: %w", err)
	}

	return r.publisher.PublishEvent(ctx, &child)
}

var _ ports.EffectSender = (*Router)(nil)
