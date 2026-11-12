// Package remote publishes worker outputs through a selected broker protocol client.
package remote

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Publisher shares publication policy across HTTP, TCP and NATS peer clients.
type Publisher struct {
	Client     ports.BrokerCaller
	Partitions uint32
}

// Publish sends an existing command/effect representation to the peer's topic.
func (p *Publisher) Publish(ctx context.Context, topic string, data []byte) error {
	var effect domain.Effect
	if err := json.Unmarshal(data, &effect); err != nil {
		return err
	}
	return p.append(ctx, topic, effect.TenantID, effect.ID, data)
}

// PublishToShard sends a child event with its existing tenant/key identity.
func (p *Publisher) PublishToShard(ctx context.Context, topic string, shard uint32, data []byte) error {
	var event domain.EventEnvelope
	if err := json.Unmarshal(data, &event); err != nil {
		return err
	}
	if err := event.Validate(); err != nil {
		return err
	}
	if p.Partitions == 0 || event.VirtualShard(p.Partitions) != shard {
		return errors.New("remote event shard mismatch")
	}
	return p.append(ctx, topic, event.TenantID, event.PartitionKey, data)
}

func (p *Publisher) append(ctx context.Context, topic, tenant, key string, data []byte) error {
	if p.Client == nil {
		return errors.New("remote broker client is required")
	}
	response, err := p.Client.Call(ctx, ports.BrokerRequest{Version: ports.BrokerProtocolVersion, Operation: "append", TenantID: tenant, Topic: topic, Key: key, Payload: data})
	if err != nil {
		return fmt.Errorf("publish to remote broker: %w", err)
	}
	if response.Code != "accepted" {
		return errors.New("remote broker rejected publication")
	}
	return nil
}

// PublishBatch retains sequential publication and partial-failure semantics.
func (p *Publisher) PublishBatch(ctx context.Context, messages []ports.PublishMessage) error {
	for i, message := range messages {
		if err := p.Publish(ctx, message.Subject, message.Data); err != nil {
			return fmt.Errorf("remote publish batch item %d: %w", i, err)
		}
	}
	return nil
}

// Close releases the selected protocol client.
func (p *Publisher) Close() error { return p.Client.Close() }

var _ ports.EventPublisher = (*Publisher)(nil)
var _ ports.SubjectPublisher = (*Publisher)(nil)
