package runtime

import (
	"context"
	"time"
)

// Consumer consumes events from a message broker
type Consumer interface {
	// Fetch fetches messages from the broker
	Fetch(ctx context.Context, maxMessages int) ([]Delivery, error)
	// FetchShards fetches messages from specific shards
	FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]Delivery, error)
	// EnsureShardConsumer ensures a consumer exists for a shard
	EnsureShardConsumer(ctx context.Context, shard uint32) error
	// RemoveShardConsumer removes a consumer for a shard
	RemoveShardConsumer(ctx context.Context, shard uint32) error
	// OwnedShards returns the list of shards this consumer owns
	OwnedShards() []uint32
	// ShardCount returns the total number of shards
	ShardCount() uint32
	// Close closes the consumer
	Close() error
}

// Delivery represents a message delivery from the broker
type Delivery interface {
	// Event returns the parsed event envelope
	Event() (*Envelope, error)
	// Raw returns the raw message bytes
	Raw() []byte
	// Ack acknowledges the message
	Ack(ctx context.Context) error
	// Nak negatively acknowledges the message
	Nak(ctx context.Context) error
	// Retry retries the message after a delay
	Retry(ctx context.Context, delay time.Duration) error
	// Headers returns the message headers
	Headers() map[string]string
	// Subject returns the message subject
	Subject() string
}
