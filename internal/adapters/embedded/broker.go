// Package embedded connects the rule worker to the in-process broker.
package embedded

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
)

// Consumer receives the events topic for the internal rule-processing group.
type Consumer struct{ broker *broker.Broker }

// NewConsumer registers rule processing before external publishers start.
func NewConsumer(b *broker.Broker) (*Consumer, error) {
	if b == nil {
		return nil, errors.New("embedded broker is required")
	}
	if err := b.Register("", "events", "__rules"); err != nil {
		return nil, err
	}
	return &Consumer{broker: b}, nil
}

// Next blocks for one event and wakes immediately when an event is published.
func (c *Consumer) Next(ctx context.Context) (ports.Delivery, error) {
	claim, err := c.broker.Next(ctx, "", "events", "__rules", time.Second)
	if err != nil || claim == nil {
		return nil, err
	}
	return &delivery{broker: c.broker, claim: *claim}, nil
}

type delivery struct {
	broker *broker.Broker
	claim  broker.Claim
}

func (d *delivery) Event() (*domain.EventEnvelope, error) {
	var env domain.EventEnvelope
	err := json.Unmarshal(d.claim.Record.Payload, &env)
	return &env, err
}
func (d *delivery) Ack(_ context.Context) error {
	return d.broker.Settle("", d.claim.Record.Topic, d.claim.Group, d.claim.Record.Partition, d.claim.Receipt, false, 0)
}
func (d *delivery) Nak(ctx context.Context) error { return d.Retry(ctx, 0) }
func (d *delivery) Retry(_ context.Context, delay time.Duration) error {
	return d.broker.Settle("", d.claim.Record.Topic, d.claim.Group, d.claim.Record.Partition, d.claim.Receipt, true, delay)
}
func (d *delivery) Raw() []byte                { return append([]byte{}, d.claim.Record.Payload...) }
func (d *delivery) Headers() map[string]string { return nil }
func (d *delivery) Subject() string            { return d.claim.Record.Topic }

// Publisher implements existing effect-router ports through local broker appends.
type Publisher struct {
	Broker     *broker.Broker
	Partitions uint32
}

// Publish routes command and generic-effect payloads using their stable identity.
func (p *Publisher) Publish(ctx context.Context, topic string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var effect domain.Effect
	if err := json.Unmarshal(data, &effect); err != nil {
		return err
	}
	_, err := p.Broker.Publish(topic, effect.TenantID, effect.ID, data)
	return err
}

// PublishToShard routes child envelopes through the existing shared shard function.
func (p *Publisher) PublishToShard(ctx context.Context, topic string, shard uint32, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var env domain.EventEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return err
	}
	if err := env.Validate(); err != nil {
		return err
	}
	if env.VirtualShard(p.Partitions) != shard {
		return errors.New("embedded event shard mismatch")
	}
	_, err := p.Broker.Publish(topic, env.TenantID, env.PartitionKey, data)
	return err
}

// PublishBatch preserves the existing sequential publication failure semantics.
func (p *Publisher) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for i, msg := range messages {
		if err := p.Publish(ctx, msg.Subject, msg.Data); err != nil {
			return fmt.Errorf("publish embedded batch item %d: %w", i, err)
		}
	}
	return nil
}

// Close has no resource work because the composition root owns broker storage.
func (p *Publisher) Close() error { return nil }

var _ ports.EventPublisher = (*Publisher)(nil)
var _ ports.SubjectPublisher = (*Publisher)(nil)
