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
// recovery horizon, not merely within a transport duplicate window. A repeated
// RecordID with identical immutable content returns the original position. A
// repeated RecordID with different content must return an identity-conflict error.
// Read returns records strictly after the cursor in ascending position order.
// A non-positive limit is invalid. Cursor expiry/gaps must be reported explicitly;
// adapters must never silently skip retained-but-unread records.
type EventLog interface {
	Append(ctx context.Context, record domain.EventLogRecord) (LogPosition, error)
	Read(ctx context.Context, after LogPosition, limit int) ([]PositionedLogRecord, error)
}

// PositionedLogRecord couples an immutable record with its backend cursor.
type PositionedLogRecord struct {
	Position LogPosition
	Record   domain.EventLogRecord
}
