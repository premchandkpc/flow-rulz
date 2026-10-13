package runtime

import (
	"context"
)

// Publisher publishes events to a message broker
type Publisher interface {
	// Publish publishes an event to the broker
	Publish(ctx context.Context, subject string, data []byte) error
	// PublishToShard publishes an event to a specific shard
	PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error
	// PublishBatch publishes multiple events
	PublishBatch(ctx context.Context, messages []PublishMessage) error
	// Close closes the publisher
	Close() error
}

// PublishMessage represents a message to publish
type PublishMessage struct {
	Subject string
	Data    []byte
}
