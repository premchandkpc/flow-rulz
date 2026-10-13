package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/jackc/pgx/v5"
)

// InboxRepository persists event inbox records in PostgreSQL.
type InboxRepository struct {
	db Querier
}

// NewInboxRepository creates an inbox repository backed by a PostgreSQL query interface.
func NewInboxRepository(db Querier) *InboxRepository {
	return &InboxRepository{db: db}
}

// Insert stores an inbox entry and reports whether a new row was created.
func (r *InboxRepository) Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error) {
	tag, err := r.db.Exec(ctx, `
		INSERT INTO inbox (tenant_id, event_id, status, first_seen_at, partition_key, rule_set, payload, batch_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tenant_id, event_id) DO NOTHING
	`, entry.TenantID, entry.EventID, entry.Status, entry.FirstSeenAt,
		entry.PartitionKey, entry.RuleSet, entry.Payload, entry.BatchID)
	if err != nil {
		return false, fmt.Errorf("insert inbox: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// Get retrieves an inbox entry by tenant and event ID, or returns nil when it does not exist.
func (r *InboxRepository) Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	entry := &domain.InboxEntry{}
	err := r.db.QueryRow(ctx, `
		SELECT tenant_id, event_id, status, execution_id, first_seen_at, committed_at,
		       COALESCE(partition_key, ''), COALESCE(rule_set, ''), payload, COALESCE(batch_id, '')
		FROM inbox
		WHERE tenant_id = $1 AND event_id = $2
	`, tenantID, eventID).Scan(
		&entry.TenantID, &entry.EventID, &entry.Status, &entry.ExecutionID,
		&entry.FirstSeenAt, &entry.CommittedAt,
		&entry.PartitionKey, &entry.RuleSet, &entry.Payload, &entry.BatchID,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get inbox: %w", err)
	}
	return entry, nil
}

// MarkCommitted marks an inbox entry as committed and records its execution ID.
func (r *InboxRepository) MarkCommitted(ctx context.Context, tenantID string, eventID string, executionID string) error {
	now := time.Now().UTC()
	_, err := r.db.Exec(ctx, `
		UPDATE inbox SET status = 'committed', execution_id = $3, committed_at = $4
		WHERE tenant_id = $1 AND event_id = $2
	`, tenantID, eventID, executionID, now)
	if err != nil {
		return fmt.Errorf("mark committed: %w", err)
	}
	return nil
}

// ExecutionRepository persists execution records in PostgreSQL.
type ExecutionRepository struct {
	db Querier
}

// NewExecutionRepository creates an execution repository backed by a PostgreSQL query interface.
func NewExecutionRepository(db Querier) *ExecutionRepository {
	return &ExecutionRepository{db: db}
}

// Save inserts an execution record unless its execution ID already exists.
func (r *ExecutionRepository) Save(ctx context.Context, execution *domain.Execution) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO executions (execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (execution_id) DO NOTHING
	`, execution.ID, execution.EventID, execution.TenantID, execution.RuleSet, execution.Revision,
		execution.DecisionHash, execution.Status, execution.Error, execution.TraceID, execution.CreatedAt)
	if err != nil {
		return fmt.Errorf("save execution: %w", err)
	}
	return nil
}

// Get retrieves an execution by ID, or returns nil when it does not exist.
func (r *ExecutionRepository) Get(ctx context.Context, executionID string) (*domain.Execution, error) {
	exec := &domain.Execution{}
	err := r.db.QueryRow(ctx, `
		SELECT execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at, completed_at
		FROM executions
		WHERE execution_id = $1
	`, executionID).Scan(
		&exec.ID, &exec.EventID, &exec.TenantID, &exec.RuleSet, &exec.Revision,
		&exec.DecisionHash, &exec.Status, &exec.Error, &exec.TraceID, &exec.CreatedAt, &exec.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get execution: %w", err)
	}
	return exec, nil
}

// UpdateStatus changes an execution's status, error message, and completion timestamp.
func (r *ExecutionRepository) UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error {
	var completedAt *time.Time
	if status == domain.ExecutionStatusCompleted || status == domain.ExecutionStatusFailed || status == domain.ExecutionStatusQuarantined {
		now := time.Now().UTC()
		completedAt = &now
	}
	_, err := r.db.Exec(ctx, `
		UPDATE executions SET status = $2, error = $3, completed_at = $4
		WHERE execution_id = $1
	`, executionID, status, errMsg, completedAt)
	if err != nil {
		return fmt.Errorf("update execution status: %w", err)
	}
	return nil
}

// GetByEvent retrieves an execution by event ID, or returns nil when no execution exists.
func (r *ExecutionRepository) GetByEvent(ctx context.Context, eventID string) (*domain.Execution, error) {
	exec := &domain.Execution{}
	err := r.db.QueryRow(ctx, `
		SELECT execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at, completed_at
		FROM executions
		WHERE event_id = $1
	`, eventID).Scan(
		&exec.ID, &exec.EventID, &exec.TenantID, &exec.RuleSet, &exec.Revision,
		&exec.DecisionHash, &exec.Status, &exec.Error, &exec.TraceID, &exec.CreatedAt, &exec.CompletedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get execution by event: %w", err)
	}
	return exec, nil
}

// GetPending returns pending executions ordered by creation time, up to the requested limit.
func (r *ExecutionRepository) GetPending(ctx context.Context, limit int) ([]*domain.Execution, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.Query(ctx, `
		SELECT execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at, completed_at
		FROM executions
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending executions: %w", err)
	}
	defer rows.Close()

	var executions []*domain.Execution
	for rows.Next() {
		exec := &domain.Execution{}
		var completedAt *time.Time
		if err := rows.Scan(
			&exec.ID, &exec.EventID, &exec.TenantID, &exec.RuleSet, &exec.Revision,
			&exec.DecisionHash, &exec.Status, &exec.Error, &exec.TraceID, &exec.CreatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		exec.CompletedAt = completedAt
		executions = append(executions, exec)
	}
	return executions, nil
}
