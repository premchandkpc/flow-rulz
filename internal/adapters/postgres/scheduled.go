// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/jackc/pgx/v5"
)

// ScheduledEventRepository defines the scheduled event repository operation.
type ScheduledEventRepository struct {
	db Querier
}

// NewScheduledEventRepository performs the new scheduled event repository operation.
func NewScheduledEventRepository(db Querier) *ScheduledEventRepository {
	return &ScheduledEventRepository{db: db}
}

// Save performs the save operation.
func (r *ScheduledEventRepository) Save(ctx context.Context, event *domain.ScheduledEvent) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO scheduled_events (event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (event_id) DO NOTHING
	`, event.EventID, event.TenantID, event.EventType, event.PartitionKey, event.WorkflowID, event.Payload, event.Headers, event.ScheduledAt, event.Status, event.CreatedAt)
	if err != nil {
		return fmt.Errorf("save scheduled event: %w", err)
	}
	return nil
}

// GetDue performs the get due operation.
func (r *ScheduledEventRepository) GetDue(ctx context.Context, before time.Time, limit int) ([]*domain.ScheduledEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.Query(ctx, `
		SELECT event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at, released_at, retry_at, retry_count, last_error
		FROM scheduled_events
		WHERE (status = $2 AND ((retry_at IS NOT NULL AND retry_at <= $1) OR (retry_at IS NULL AND scheduled_at <= $1)))
		   OR (status = $3 AND claim_expires_at <= $1)
		ORDER BY COALESCE(retry_at, scheduled_at)
		LIMIT $4
	`, before, domain.ScheduledEventStatusPending, domain.ScheduledEventStatusClaimed, limit)
	if err != nil {
		return nil, fmt.Errorf("get due scheduled events: %w", err)
	}
	defer rows.Close()
	return scanScheduledEvents(rows)
}

// Claim performs the claim operation.
func (r *ScheduledEventRepository) Claim(ctx context.Context, eventIDs []string, claimant string, leaseTTL time.Duration) ([]*domain.ScheduledEvent, error) {
	if len(eventIDs) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(claimant) == "" {
		return nil, fmt.Errorf("claim scheduled events: claimant is required")
	}
	if leaseTTL <= 0 {
		return nil, fmt.Errorf("claim scheduled events: lease TTL must be positive")
	}

	now := time.Now().UTC()
	query, args := buildScheduledClaimQuery(eventIDs, claimant, now, now.Add(leaseTTL))
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("claim scheduled events: %w", err)
	}
	defer rows.Close()
	return scanScheduledEvents(rows)
}

func buildScheduledClaimQuery(eventIDs []string, claimant string, now, expiresAt time.Time) (string, []any) {
	placeholders := scheduledEventPlaceholders(len(eventIDs))
	args := scheduledClaimArguments(eventIDs, claimant, now, expiresAt)
	pendingStatusArg := len(eventIDs) + 4
	claimedStatusArg := len(eventIDs) + 5
	query := fmt.Sprintf(`
		UPDATE scheduled_events
		SET status = $%d, claimant = $1, claim_expires_at = $2, claimed_at = $3
		WHERE event_id IN (%s)
		  AND (
		    (status = $%d AND ((retry_at IS NOT NULL AND retry_at <= $3) OR (retry_at IS NULL AND scheduled_at <= $3)))
		    OR (status = $%d AND claim_expires_at <= $3)
		  )
		RETURNING event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at, released_at, retry_at, retry_count, last_error
	`, claimedStatusArg, strings.Join(placeholders, ","), pendingStatusArg, claimedStatusArg)
	return query, args
}

func scheduledEventPlaceholders(count int) []string {
	placeholders := make([]string, count)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+4)
	}
	return placeholders
}

func scheduledClaimArguments(eventIDs []string, claimant string, now, expiresAt time.Time) []any {
	args := make([]any, len(eventIDs)+5)
	args[0], args[1], args[2] = claimant, expiresAt, now
	for i, eventID := range eventIDs {
		args[i+3] = eventID
	}
	args[len(eventIDs)+3] = domain.ScheduledEventStatusPending
	args[len(eventIDs)+4] = domain.ScheduledEventStatusClaimed
	return args
}

// MarkReleased performs the mark released operation.
func (r *ScheduledEventRepository) MarkReleased(ctx context.Context, eventID, claimant string, releasedAt time.Time) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE scheduled_events
		SET status = $4, released_at = $3, retry_at = NULL,
		    claimant = NULL, claimed_at = NULL, claim_expires_at = NULL
		WHERE event_id = $1 AND status = $5 AND claimant = $2 AND claim_expires_at > NOW()
	`, eventID, claimant, releasedAt, domain.ScheduledEventStatusReleased, domain.ScheduledEventStatusClaimed)
	if err != nil {
		return fmt.Errorf("mark scheduled event released: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark scheduled event released: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

// ScheduleRetry performs the schedule retry operation.
func (r *ScheduledEventRepository) ScheduleRetry(ctx context.Context, eventID, claimant string, retryAt time.Time, errMsg string) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE scheduled_events
		SET status = $4, retry_at = $3, retry_count = retry_count + 1, last_error = $5,
		    claimant = NULL, claimed_at = NULL, claim_expires_at = NULL
		WHERE event_id = $1 AND status = $6 AND claimant = $2 AND claim_expires_at > NOW()
	`, eventID, claimant, retryAt, domain.ScheduledEventStatusPending, errMsg, domain.ScheduledEventStatusClaimed)
	if err != nil {
		return fmt.Errorf("schedule scheduled event retry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("schedule scheduled event retry: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

// MarkFailed performs the mark failed operation.
func (r *ScheduledEventRepository) MarkFailed(ctx context.Context, eventID, claimant, errMsg string) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE scheduled_events
		SET status = $4, retry_count = retry_count + 1, last_error = $3,
		    claimant = NULL, claimed_at = NULL, claim_expires_at = NULL
		WHERE event_id = $1 AND status = $5 AND claimant = $2 AND claim_expires_at > NOW()
	`, eventID, claimant, errMsg, domain.ScheduledEventStatusFailed, domain.ScheduledEventStatusClaimed)
	if err != nil {
		return fmt.Errorf("mark scheduled event failed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark scheduled event failed: claim for event %q is no longer owned by %q", eventID, claimant)
	}
	return nil
}

// Get performs the get operation.
func (r *ScheduledEventRepository) Get(ctx context.Context, tenantID, eventID string) (*domain.ScheduledEvent, error) {
	var evt domain.ScheduledEvent
	var releasedAt *time.Time
	err := r.db.QueryRow(ctx, `
		SELECT event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at, released_at, retry_at, retry_count, last_error
		FROM scheduled_events
		WHERE tenant_id = $1 AND event_id = $2
	`, tenantID, eventID).Scan(
		&evt.EventID, &evt.TenantID, &evt.EventType, &evt.PartitionKey,
		&evt.WorkflowID, &evt.Payload, &evt.Headers, &evt.ScheduledAt,
		&evt.Status, &evt.CreatedAt, &releasedAt, &evt.RetryAt, &evt.RetryCount, &evt.LastError,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get scheduled event: %w", err)
	}
	evt.ReleasedAt = releasedAt
	return &evt, nil
}

// List performs the list operation.
func (r *ScheduledEventRepository) List(ctx context.Context, tenantID string, limit, offset int) ([]*domain.ScheduledEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	rows, err := r.db.Query(ctx, `
		SELECT event_id, tenant_id, event_type, partition_key, workflow_id, payload, headers, scheduled_at, status, created_at, released_at, retry_at, retry_count, last_error
		FROM scheduled_events
		WHERE tenant_id = $1
		ORDER BY scheduled_at DESC, created_at DESC
		LIMIT $2 OFFSET $3
	`, tenantID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list scheduled events: %w", err)
	}
	defer rows.Close()
	return scanScheduledEvents(rows)
}

// Cancel cancels a pending scheduled event belonging to tenantID. Claimed,
// released, failed, or foreign-tenant events cannot be cancelled through this API.
func (r *ScheduledEventRepository) Cancel(ctx context.Context, tenantID, eventID string) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE scheduled_events
		SET status = $3
		WHERE tenant_id = $1 AND event_id = $2 AND status = $4
	`, tenantID, eventID, domain.ScheduledEventStatusCancelled, domain.ScheduledEventStatusPending)
	if err != nil {
		return fmt.Errorf("cancel scheduled event: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("cancel scheduled event: event %q not found or not pending for tenant %q", eventID, tenantID)
	}
	return nil
}

func scanScheduledEvents(rows pgx.Rows) ([]*domain.ScheduledEvent, error) {
	var events []*domain.ScheduledEvent
	for rows.Next() {
		var evt domain.ScheduledEvent
		var releasedAt *time.Time
		if err := rows.Scan(
			&evt.EventID, &evt.TenantID, &evt.EventType, &evt.PartitionKey,
			&evt.WorkflowID, &evt.Payload, &evt.Headers, &evt.ScheduledAt,
			&evt.Status, &evt.CreatedAt, &releasedAt, &evt.RetryAt, &evt.RetryCount, &evt.LastError,
		); err != nil {
			return nil, fmt.Errorf("scan scheduled event: %w", err)
		}
		evt.ReleasedAt = releasedAt
		events = append(events, &evt)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate scheduled events: %w", err)
	}
	return events, nil
}
