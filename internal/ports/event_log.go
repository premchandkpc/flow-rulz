package ports

import (
	"context"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// LogPosition is an opaque, monotonically increasing cursor within one log.
// Callers must not assume that cursors from different streams are comparable.
type LogPosition uint64

// EventLog is the application boundary for the durable processing log.
// Append must be idempotent for a stable RecordID across the full configured
// recovery horizon, not merely within JetStream's short duplicate window. If a
// backend cannot guarantee that itself, its adapter must reconcile against the
// retained log before accepting a retry as a new fact.
type EventLog interface {
	Append(ctx context.Context, record domain.EventLogRecord) (LogPosition, error)
	Read(ctx context.Context, after LogPosition, limit int) ([]PositionedLogRecord, error)
}

// PositionedLogRecord couples an immutable record with its backend cursor.
type PositionedLogRecord struct {
	Position LogPosition
	Record   domain.EventLogRecord
}
