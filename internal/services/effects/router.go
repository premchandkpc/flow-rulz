// Package effects provides effect routing.
package effects

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Router sends child events and command/service effects through NATS-backed senders.
type Router struct {
	publisher   ports.EventPublisher
	command     ports.EffectSender
	service     ports.EffectSender
	scheduled   ports.EffectSender
	baseSubject string
	numShards   uint32
}

// NewRouter creates a router using the default event subject and shard count.
func NewRouter(publisher ports.EventPublisher, commandSender ports.EffectSender) *Router {
	return NewRouterWithConfig(publisher, commandSender, "events", 4096)
}

// NewRouterWithConfig creates a router aligned with the stream's subject family
// and the shard count used by producers, consumers, and the scheduler.
func NewRouterWithConfig(publisher ports.EventPublisher, commandSender ports.EffectSender, baseSubject string, numShards uint32, serviceSenders ...ports.EffectSender) *Router {
	if baseSubject == "" {
		baseSubject = "events"
	}
	if numShards == 0 {
		numShards = 4096
	}
	router := &Router{publisher: publisher, command: commandSender, baseSubject: baseSubject, numShards: numShards}
	if len(serviceSenders) > 0 {
		router.service = serviceSenders[0]
	}
	if len(serviceSenders) > 1 {
		router.scheduled = serviceSenders[1]
	}
	return router
}

// Send routes an effect to the appropriate destination based on its type.
// Child events go to the shard publisher, service events to their unique NATS subject,
// and command/emit effects to their NATS subjects.
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
			return fmt.Errorf("send command or emit effect: NATS sender is not configured")
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

	// Event type is carried in the envelope. The broker subject must match the
	// stream filter shared by consumers and the scheduler: <baseSubject>.shard.N.
	shard := child.VirtualShard(r.numShards)
	return r.publisher.PublishToShard(ctx, r.baseSubject, shard, effect.Payload)
}

var _ ports.EffectSender = (*Router)(nil)
