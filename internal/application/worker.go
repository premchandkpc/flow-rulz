// Package application provides application-level worker orchestration.
package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
)

// WorkerConfig holds runtime limits and polling intervals for a worker.
type WorkerConfig struct {
	MaxGlobalInFlight      int
	FetchBatchSize         int
	EffectPublishInterval  time.Duration
	EffectPublishBatchSize int
	RetryDelay             time.Duration
	MaxPerKeyQueue         int
	KeyQueueWorkers        int
	HotKeyThreshold        int
	HotKeyCheckInterval    time.Duration
	HotKeyCallback         func(partitionKey string, depth int)
}

// DefaultWorkerConfig returns production-safe worker defaults.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		MaxGlobalInFlight:      100,
		FetchBatchSize:         100,
		EffectPublishInterval:  5 * time.Second,
		EffectPublishBatchSize: 10,
		RetryDelay:             5 * time.Second,
		MaxPerKeyQueue:         100,
		KeyQueueWorkers:        1,
		HotKeyThreshold:        1000,
		HotKeyCheckInterval:    10 * time.Second,
	}
}

// shardedConsumer is the transport capability required by sharded workers.
type shardedConsumer interface {
	EnsureShardConsumer(ctx context.Context, shard uint32) error
	RemoveShardConsumer(ctx context.Context, shard uint32) error
	FetchShards(ctx context.Context, maxMessages int, shards []uint32) ([]ports.Delivery, error)
	OwnedShards() []uint32
}

// Worker coordinates event consumption, per-key processing, and outbox publishing.
// Runtime loops and delivery settlement live in focused companion files.
type Worker struct {
	consumer        ports.BrokerConsumer
	events          ports.EventProcessor
	effects         ports.EffectPublisher
	batches         ports.BatchProcessor
	batchInterval   time.Duration
	clock           ports.Clock
	shardService    ports.ShardService
	keyQueue        ports.KeyQueue
	workerID        string
	numShards       uint32
	config          WorkerConfig
	shardedConsumer shardedConsumer
}

// NewWorker validates dependencies and configuration without starting resources.
// The key queue is created by Run so its lifecycle is owned by the worker.
func NewWorker(
	consumer ports.BrokerConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	batches ports.BatchProcessor,
	batchInterval time.Duration,
	clock ports.Clock,
	shardService ports.ShardService,
	workerID string,
	numShards uint32,
	config WorkerConfig,
) (*Worker, error) {
	if err := validateWorkerDependencies(consumer, events, effects, clock, shardService, workerID); err != nil {
		return nil, err
	}

	config, err := normalizeWorkerConfig(config)
	if err != nil {
		return nil, err
	}
	if numShards == 0 {
		numShards = 4096
	}

	sharded, ok := consumer.(shardedConsumer)
	if !ok {
		return nil, errors.New("consumer must implement sharded consumer; non-sharded mode is not supported")
	}

	return &Worker{
		consumer:        consumer,
		events:          events,
		effects:         effects,
		batches:         batches,
		batchInterval:   batchInterval,
		clock:           clock,
		shardService:    shardService,
		workerID:        workerID,
		numShards:       numShards,
		config:          config,
		shardedConsumer: sharded,
	}, nil
}

func validateWorkerDependencies(
	consumer ports.BrokerConsumer,
	events ports.EventProcessor,
	effects ports.EffectPublisher,
	clock ports.Clock,
	shardService ports.ShardService,
	workerID string,
) error {
	switch {
	case consumer == nil:
		return errors.New("worker consumer is required")
	case events == nil:
		return errors.New("worker event processor is required")
	case effects == nil:
		return errors.New("worker effect publisher is required")
	case clock == nil:
		return errors.New("worker clock is required")
	case shardService == nil:
		return errors.New("worker shard service is required")
	case workerID == "":
		return errors.New("worker ID is required")
	default:
		return nil
	}
}

func normalizeWorkerConfig(config WorkerConfig) (WorkerConfig, error) {
	defaults := DefaultWorkerConfig()
	if config.MaxGlobalInFlight == 0 {
		config = defaults
	}
	if config.MaxGlobalInFlight < 0 {
		config.MaxGlobalInFlight = defaults.MaxGlobalInFlight
	}

	applyWorkerConfigDefaults(&config, defaults)
	if config.FetchBatchSize > config.MaxGlobalInFlight {
		return WorkerConfig{}, fmt.Errorf(
			"worker fetch batch size (%d) must not exceed max global in-flight (%d)",
			config.FetchBatchSize,
			config.MaxGlobalInFlight,
		)
	}
	return config, nil
}

func applyWorkerConfigDefaults(config *WorkerConfig, defaults WorkerConfig) {
	if config.FetchBatchSize <= 0 {
		config.FetchBatchSize = defaults.FetchBatchSize
	}
	if config.EffectPublishInterval <= 0 {
		config.EffectPublishInterval = defaults.EffectPublishInterval
	}
	if config.EffectPublishBatchSize <= 0 {
		config.EffectPublishBatchSize = defaults.EffectPublishBatchSize
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = defaults.RetryDelay
	}
	if config.MaxPerKeyQueue <= 0 {
		config.MaxPerKeyQueue = defaults.MaxPerKeyQueue
	}
	if config.KeyQueueWorkers <= 0 {
		config.KeyQueueWorkers = defaults.KeyQueueWorkers
	}
	if config.HotKeyThreshold <= 0 {
		config.HotKeyThreshold = defaults.HotKeyThreshold
	}
	if config.HotKeyCheckInterval <= 0 {
		config.HotKeyCheckInterval = defaults.HotKeyCheckInterval
	}
}
