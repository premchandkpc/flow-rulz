package nats

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type shardPublisher interface {
	PublishToShard(context.Context, string, uint32, []byte) error
}

// EventPublisher maps domain events onto the configured broker subject family.
// It shares the underlying publisher; its owner remains responsible for closing it.
type EventPublisher struct {
	publisher   shardPublisher
	baseSubject string
	numShards   uint32
}

// NewEventPublisher creates an envelope publisher using the stream's routing
// configuration. The underlying publisher retains ruleset-specific subject mapping.
func NewEventPublisher(publisher shardPublisher, baseSubject string, numShards uint32) *EventPublisher {
	if baseSubject == "" {
		baseSubject = "events"
	}
	if numShards == 0 {
		numShards = 4096
	}
	return &EventPublisher{publisher: publisher, baseSubject: baseSubject, numShards: numShards}
}

// PublishEvent validates and encodes an envelope, then publishes to its shard.
// Success means broker acceptance, not completion of downstream processing.
func (p *EventPublisher) PublishEvent(ctx context.Context, event *domain.EventEnvelope) error {
	if p.publisher == nil {
		return fmt.Errorf("publish event: broker publisher is not configured")
	}
	if event == nil {
		return fmt.Errorf("publish event: envelope is nil")
	}
	if err := event.Validate(); err != nil {
		return fmt.Errorf("validate event: %w", err)
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.publisher.PublishToShard(ctx, p.baseSubject, event.VirtualShard(p.numShards), data)
}

var _ ports.EnvelopePublisher = (*EventPublisher)(nil)
