package ports

import (
	"context"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// Worker manages the event processing lifecycle
type Worker interface {
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

// KeyQueue manages per-partition-key ordering
type KeyQueue interface {
	Submit(ctx context.Context, item QueueItem) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Stats() QueueStats
}

// QueueItem represents a work item in the key queue.
type QueueItem struct {
	Event        *domain.EventEnvelope
	Delivery     Delivery
	FencingToken int64
	Shard        uint32
	WorkerID     string
	Priority     int
	SubmittedAt  time.Time
}

// QueueStats represents queue statistics.
type QueueStats struct {
	ActiveKeys     int
	TotalQueued    int64
	Processing     int
	HotKeys        []HotKeyInfo
	GlobalDepth    int
	MaxGlobalDepth int
}

// HotKeyInfo represents a hot key in the queue.
type HotKeyInfo struct {
	PartitionKey string
	Depth        int
	Since        time.Time
}

// Scheduler manages time-based event execution
type Scheduler interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Schedule(ctx context.Context, event *domain.ScheduledEvent) error
}

// ShardLeaseView exposes only the ownership snapshot needed by a use case.
// It deliberately excludes lease mutation and lifecycle operations.
type ShardLeaseView interface {
	IsOwner(shard uint32) bool
	GetFencingToken(shard uint32) (int64, bool)
}

// ShardService manages shard ownership and leases for a worker.
// It abstracts the lease lifecycle and ownership queries.
type ShardService interface {
	ShardLeaseView
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	OwnedShards() []uint32
	VirtualShardForEvent(*domain.EventEnvelope) uint32
}

// ShardManager manages shard ownership and leases
type ShardManager interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	OwnedShards() []uint32
	IsOwner(shard uint32) bool
	GetFencingToken(shard uint32) (int64, bool)
	AcquireShard(ctx context.Context, shard uint32) error
	ReleaseShard(ctx context.Context, shard uint32) error
}

// BatchProcessor processes accumulated events
type BatchProcessor interface {
	Tick(ctx context.Context) (int, error)
}

// EffectDispatcher dispatches effects to their destinations
type EffectDispatcher interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Dispatch(ctx context.Context, effect *domain.Effect) error
}
