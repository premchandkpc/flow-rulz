// Package effects provides effect routing.
package effects

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Router sends child events and command/service effects through selected senders.
type Router struct {
	publisher         ports.EventPublisher
	envelopePublisher ports.EnvelopePublisher
	command           ports.EffectSender
	service           ports.EffectSender
	scheduled         ports.EffectSender
	baseSubject       string
	numShards         uint32
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
	return router.withServiceSenders(serviceSenders)
}

func (router *Router) withServiceSenders(serviceSenders []ports.EffectSender) *Router {
	if len(serviceSenders) > 0 {
		router.service = serviceSenders[0]
	}
	if len(serviceSenders) > 1 {
		router.scheduled = serviceSenders[1]
	}
	return router
}

// NewEnvelopeRouter keeps broker routing out of durable effect processing.
// Direct and embedded modes retain NewRouterWithConfig for compatibility.
func NewEnvelopeRouter(publisher ports.EnvelopePublisher, command, service, scheduled ports.EffectSender) *Router {
	return &Router{envelopePublisher: publisher, command: command, service: service, scheduled: scheduled}
}

// Send routes an effect to the appropriate destination based on its type.
// Child events go to the shard publisher, service events to their unique NATS subject,
// and command/emit effects to their NATS subjects.
func (router *Router) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if effect == nil {
		return fmt.Errorf("send effect: effect is nil")
	}
	switch effect.EffectType {
	case domain.EffectTypeScheduleEvent:
		return router.sendScheduledEvent(ctx, effect, idempotencyKey)
	case domain.EffectTypeServiceEvent:
		return router.sendServiceEvent(ctx, effect, idempotencyKey)
	case domain.EffectTypeEmitEvent:
		return router.sendChildEvent(ctx, effect)
	default:
		return router.sendCommand(ctx, effect, idempotencyKey)
	}
}

// sendScheduledEvent forwards schedule intents to the durable scheduled-event sender.
func (router *Router) sendScheduledEvent(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if router.scheduled == nil {
		return fmt.Errorf("schedule event: scheduled-event sender is not configured")
	}
	return router.scheduled.Send(ctx, effect, idempotencyKey)
}

// sendServiceEvent forwards subscribed service events to their service sender.
func (router *Router) sendServiceEvent(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if router.service == nil {
		return fmt.Errorf("send subscribed service event: service sender is not configured")
	}
	return router.service.Send(ctx, effect, idempotencyKey)
}

// sendCommand forwards command effects to the command sender.
func (router *Router) sendCommand(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if router.command == nil {
		return fmt.Errorf("send command or emit effect: sender is not configured")
	}
	return router.command.Send(ctx, effect, idempotencyKey)
}

// sendChildEvent decodes, validates and publishes a child event. The event
// type is carried in the envelope. The broker subject must match the stream
// filter shared by consumers and the scheduler: <baseSubject>.shard.N.
func (router *Router) sendChildEvent(ctx context.Context, effect *domain.Effect) error {
	if router.publisher == nil && router.envelopePublisher == nil {
		return fmt.Errorf("publish child event: broker publisher is not configured")
	}
	child, err := decodeChildEvent(effect.Payload)
	if err != nil {
		return err
	}
	if router.envelopePublisher != nil {
		return router.envelopePublisher.PublishEvent(ctx, child)
	}
	shard := child.VirtualShard(router.numShards)
	return router.publisher.PublishToShard(ctx, router.baseSubject, shard, effect.Payload)
}

var _ ports.EffectSender = (*Router)(nil)

func decodeChildEvent(payload []byte) (*domain.EventEnvelope, error) {
	var child domain.EventEnvelope
	if err := json.Unmarshal(payload, &child); err != nil {
		return nil, fmt.Errorf("decode child event: %w", err)
	}
	if err := child.Validate(); err != nil {
		return nil, fmt.Errorf("validate child event: %w", err)
	}
	return &child, nil
}
