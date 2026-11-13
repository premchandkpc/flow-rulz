package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	natslib "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// DirectConsumer shares one durable pull consumer across worker replicas.
// Broker delivery replaces database-backed application shard ownership.
type DirectConsumer struct {
	owner    *Consumer
	consumer jetstream.Consumer
}

// NewDirectConsumer receives the configured subject without creating shard consumers.
// Use a distinct consumer name and non-overlapping stream/subjects from durable workers.
func NewDirectConsumer(ctx context.Context, cfg Config, subject string, maxPending int) (*DirectConsumer, error) {
	if subject == "" || len(cfg.RuleSets) > 0 || maxPending <= 0 {
		return nil, errors.New("direct consumer requires an input subject, positive pending limit and no NATS_RULESETS")
	}
	owner, err := NewConsumer(ctx, cfg)
	if err != nil {
		return nil, err
	}
	consumer, err := owner.js.CreateOrUpdateConsumer(ctx, owner.cfg.Stream, jetstream.ConsumerConfig{
		Durable: owner.baseDurable + "-direct", FilterSubject: subject,
		AckPolicy: jetstream.AckExplicitPolicy, AckWait: owner.cfg.AckWait,
		MaxDeliver: -1, MaxAckPending: maxPending,
	})
	if err != nil {
		_ = owner.Close()
		return nil, fmt.Errorf("create direct consumer: %w", err)
	}
	return &DirectConsumer{owner: owner, consumer: consumer}, nil
}

// Next returns as soon as one message arrives; it never waits to fill a batch.
func (c *DirectConsumer) Next(ctx context.Context) (ports.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	msg, err := c.consumer.Next(jetstream.FetchMaxWait(time.Second))
	if errors.Is(err, natslib.ErrTimeout) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &jetstreamDelivery{msg: msg}, nil
}

// Close releases the connection; the broker retains the durable consumer.
func (c *DirectConsumer) Close() error { return c.owner.Close() }
