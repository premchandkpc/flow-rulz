// Package ports defines consumer-facing interfaces for external systems.
package ports

import (
	"context"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// BrokerConsumer defines the interface for consuming messages from a message broker
type BrokerConsumer interface {
	Fetch(ctx context.Context, maxMessages int) ([]Delivery, error)
	Subscribe(ctx context.Context, subjects []string) error
	Unsubscribe(ctx context.Context) error
	Close() error
}

// DeliveryAttempt is an optional capability implemented by adapters that can
// report the broker's delivery count. Application retry policy uses it only
// as a hint; correctness must not depend on the metadata being available.
type DeliveryAttempt interface {
	Attempt() uint64
}

// Delivery represents a message delivery from the broker
type Delivery interface {
	Event() (*domain.EventEnvelope, error)
	Ack(ctx context.Context) error
	Nak(ctx context.Context) error
	Retry(ctx context.Context, delay time.Duration) error
	Raw() []byte
	Headers() map[string]string
	Subject() string
}

// BrokerPublisher is the generic transport-level publishing contract.
// Business use cases should prefer a narrower contract such as EventPublisher.
type BrokerPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
	PublishBatch(ctx context.Context, messages []PublishMessage) error
	PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error
	Close() error
}

// EventConsumer is the worker-facing contract for broker deliveries.
// Implementations own all transport-specific consumer and delivery types.
type EventConsumer interface {
	Fetch(ctx context.Context, maxMessages int) ([]Delivery, error)
	FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]Delivery, error)
	EnsureShardConsumer(ctx context.Context, shard uint32) error
	RemoveShardConsumer(ctx context.Context, shard uint32) error
	OwnedShards() []uint32
	Close() error
}

// SubjectPublisher publishes a message to a literal broker subject.
// Effect destinations depend only on this capability, not consumer lifecycle APIs.
type SubjectPublisher interface {
	Publish(ctx context.Context, subject string, data []byte) error
}

// EventPublisher is the fixed publishing contract used by application services.
// Callers provide a base subject and shard; adapters encode transport-specific routing.
type EventPublisher interface {
	PublishToShard(ctx context.Context, baseSubject string, shard uint32, data []byte) error
	PublishBatch(ctx context.Context, messages []PublishMessage) error
	Close() error
}

// OrderEventBroker is a domain-specific contract for order event flows.
// It deliberately exposes payloads and deliveries, never broker client types.
type OrderEventBroker interface {
	SendOrderCreate(ctx context.Context, shard uint32, payload []byte) error
	ConsumeOrderCreate(ctx context.Context, maxMessages int, shards []uint32) ([]Delivery, error)
}

// PublishMessage represents a message to be published to a broker.
type PublishMessage struct {
	Subject string
	Data    []byte
	Headers map[string]string
}

// StreamAdmin provides administrative operations for streams.
type StreamAdmin interface {
	CreateStream(ctx context.Context, config StreamConfig) error
	DeleteStream(ctx context.Context, name string) error
	CreateConsumer(ctx context.Context, stream, consumer string, config ConsumerConfig) error
	DeleteConsumer(ctx context.Context, stream, consumer string) error
	GetStreamInfo(ctx context.Context, name string) (*StreamInfo, error)
}

// PlacementConfig configures stream placement.
type PlacementConfig struct {
	Tags map[string]string
}

// StreamConfig configures a stream.
type StreamConfig struct {
	Name      string
	Subjects  []string
	Retention RetentionPolicy
	Storage   StorageType
	MaxMsgs   int64
	MaxBytes  int64
	MaxAge    time.Duration
	Replicas  int
	Placement *PlacementConfig
}

// ConsumerConfig configures a consumer.
type ConsumerConfig struct {
	Durable       string
	AckPolicy     AckPolicy
	DeliverPolicy DeliverPolicy
	FilterSubject string
	MaxDeliver    int
	AckWait       time.Duration
	MaxAckPending int
	NumReplicas   int
}

// StreamInfo provides stream metadata.
type StreamInfo struct {
	Name      string
	Subjects  []string
	State     StreamState
	Msgs      uint64
	Bytes     uint64
	Consumers []ConsumerInfo
}

// ConsumerInfo provides consumer metadata.
type ConsumerInfo struct {
	Name          string
	NumPending    uint64
	NumAckPending uint64
	Delivered     uint64
	AckFloor      uint64
}

// RetentionPolicy defines the stream retention policy.
type RetentionPolicy string

const (
	// RetentionPolicyLimits retains messages up to configured limits.
	RetentionPolicyLimits RetentionPolicy = "limits"
	// RetentionPolicyInterest retains messages while there are interested consumers.
	RetentionPolicyInterest RetentionPolicy = "interest"
	// RetentionPolicyWorkQueue retains messages for work queue processing.
	RetentionPolicyWorkQueue RetentionPolicy = "workqueue"
)

// StorageType defines the stream storage type.
type StorageType string

const (
	// StorageTypeFile uses file-based storage.
	StorageTypeFile StorageType = "file"
	// StorageTypeMemory uses in-memory storage.
	StorageTypeMemory StorageType = "memory"
)

// AckPolicy defines the acknowledgment policy.
type AckPolicy string

const (
	// AckPolicyExplicit requires explicit acknowledgment.
	AckPolicyExplicit AckPolicy = "explicit"
	// AckPolicyAll acknowledges all messages up to the current one.
	AckPolicyAll AckPolicy = "all"
	// AckPolicyNone disables acknowledgment.
	AckPolicyNone AckPolicy = "none"
)

// DeliverPolicy defines the delivery policy.
type DeliverPolicy string

const (
	// DeliverPolicyAll delivers all available messages.
	DeliverPolicyAll DeliverPolicy = "all"
	// DeliverPolicyLast delivers the last available message.
	DeliverPolicyLast DeliverPolicy = "last"
	// DeliverPolicyNew delivers only new messages.
	DeliverPolicyNew DeliverPolicy = "new"
	// DeliverPolicyByStartSeq delivers messages from a start sequence.
	DeliverPolicyByStartSeq DeliverPolicy = "by_start_sequence"
	// DeliverPolicyByStartTime delivers messages from a start time.
	DeliverPolicyByStartTime DeliverPolicy = "by_start_time"
)

// StreamState defines the stream state.
type StreamState string

const (
	// StreamStateActive indicates the stream is active.
	StreamStateActive StreamState = "active"
	// StreamStateDeleted indicates the stream has been deleted.
	StreamStateDeleted StreamState = "deleted"
)

// ShardedConsumer is the minimal worker-facing pull-consumer contract.
// It intentionally excludes generic Subscribe/Unsubscribe operations.
type ShardedConsumer interface {
	EnsureShardConsumer(ctx context.Context, shard uint32) error
	RemoveShardConsumer(ctx context.Context, shard uint32) error
	FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]Delivery, error)
	OwnedShards() []uint32
}

// RulesetConsumer is a consumer bound to a specific ruleset.
// It handles only events for its configured event types.
type RulesetConsumer interface {
	EventConsumer
	Ruleset() string
	EventTypes() []string
}

// ConsumerRegistry manages multiple ruleset-bound consumers.
// It allows the worker to fetch events for specific rulesets independently.
type ConsumerRegistry interface {
	// Register adds a consumer for a specific ruleset.
	Register(ctx context.Context, rc RulesetConsumer) error

	// Get returns the consumer for a ruleset.
	Get(ruleset string) (RulesetConsumer, bool)

	// FetchForRuleset fetches events for a specific ruleset.
	FetchForRuleset(ctx context.Context, ruleset string, maxMessages int) ([]Delivery, error)

	// Start begins consuming for all registered consumers.
	Start(ctx context.Context) error

	// Stop stops all registered consumers.
	Stop(ctx context.Context) error

	// RegisteredRulesets returns all registered rulesets.
	RegisteredRulesets() []string
}
