// Package logfirst defines the application-owned persistence boundary.
//
// Concrete implementations belong in adapters. Domain and application code must
// not depend on Kafka, NATS, SQL, filesystem, or remote-service SDK types.
package logfirst

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrLogUnavailable indicates the selected log adapter cannot currently serve requests.
	ErrLogUnavailable       = errors.New("log storage unavailable")
	// ErrPositionUnavailable indicates a requested replay position is no longer available.
	ErrPositionUnavailable  = errors.New("log position unavailable")
	// ErrAmbiguousCommit indicates the caller must resolve a commit using idempotent retry or status lookup.
	ErrAmbiguousCommit      = errors.New("log transaction outcome is ambiguous")
	// ErrIdempotencyConflict indicates a stable record ID was reused with different content.
	ErrIdempotencyConflict = errors.New("record id reused with different content")
)

// LogRecord is the backend-neutral unit persisted in the durable log.
// ID must remain stable across retries. Adapters deduplicate identical IDs within a tenant and kind; reusing an ID with different content must return ErrIdempotencyConflict. PartitionKey defines the ordering lane.
type LogRecord struct {
	ID          string
	TenantID    string
	PartitionKey string
	Kind        string
	Key         []byte
	Value       []byte
	OccurredAt  time.Time
}

// LogPosition identifies an existing durable log entry. The backend interprets
// Stream and Partition; Sequence is the entry's stable position within the partition.
type LogPosition struct {
	Stream    string
	Partition string
	Sequence  int64
}

// InputCheckpoint records the next input sequence to consume for a consumer group.
// This is deliberately distinct from LogPosition, which identifies an existing entry.
type InputCheckpoint struct {
	Stream       string
	Partition    string
	NextSequence int64
}

// LogEntry couples a record with its durable position so callers can replay
// without reconstructing offsets from record contents or list indexes.
type LogEntry struct {
	Position LogPosition
	Record   LogRecord
}

// LogTransaction atomically appends outputs and records input progress.
// Commit must make all appends and the input position visible together, or none
// of them. Adapters that cannot provide this guarantee must not implement this
// interface by silently weakening the semantics.
type LogTransaction interface {
	Append(context.Context, LogRecord) error
	CommitInputPosition(context.Context, string, []InputCheckpoint) error
	Commit(context.Context) error
	Abort(context.Context) error
}

// LogStore is the application port for durable append and replay. Append must
// return only after the adapter meets its configured durability policy. ReadFrom
// starts at the inclusive fromSequence and returns stable entry positions. The
// initial deployment may use one adapter, while a future remote storage service
// can implement the same contract without changing use cases.
type LogStore interface {
	Begin(context.Context) (LogTransaction, error)
	Append(context.Context, LogRecord) (LogPosition, error)
	ReadFrom(context.Context, string, string, int64, int) ([]LogEntry, error)
	Health(context.Context) error
	Close() error
}

// LogStoreConfig contains semantic requirements, not vendor-specific options.
// Adapter-specific configuration belongs in the composition/bootstrap layer.
type LogStoreConfig struct {
	Namespace         string
	RequireDurability bool
	MaxBatchRecords   int
}
