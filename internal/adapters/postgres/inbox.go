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
	tag, err := r.db.Exec(ctx, inboxRepositoryInsertSQL, entry.TenantID, entry.EventID, entry.RuleSet, entry.Status, entry.FirstSeenAt,
		entry.PartitionKey, entry.Payload, entry.BatchID)
	if err != nil {
		return false, fmt.Errorf("insert inbox: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// Get retrieves an inbox entry by tenant and event ID, or returns nil when it does not exist.
// Note: This returns the first matching entry when multiple rulesets exist for the same event.
// Prefer GetByRuleSet for precise lookups.
func (r *InboxRepository) Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	entry, err := scanInbox(r.db.QueryRow(ctx, inboxRepositoryGetSQL, tenantID, eventID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get inbox: %w", err)
	}
	return entry, nil
}

// GetByRuleSet retrieves an inbox entry by tenant, event ID, and ruleset.
func (r *InboxRepository) GetByRuleSet(ctx context.Context, tenantID, eventID, ruleSet string) (*domain.InboxEntry, error) {
	entry, err := scanInbox(r.db.QueryRow(ctx, inboxRepositoryGetByRuleSetSQL, tenantID, eventID, ruleSet))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get inbox by ruleset: %w", err)
	}
	return entry, nil
}

// MarkCommitted transitions exactly one ruleset's processing entry to committed.
func (r *InboxRepository) MarkCommitted(ctx context.Context, tenantID, eventID, ruleSet, executionID string) error {
	now := time.Now().UTC()
	tag, err := r.db.Exec(ctx, inboxRepositoryMarkCommittedSQL, tenantID, eventID, ruleSet, executionID, now)
	if err != nil {
		return fmt.Errorf("mark committed: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("mark committed: %w for ruleset %q", domain.ErrInboxInvalidStatus, ruleSet)
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
	_, err := r.db.Exec(ctx, executionRepositorySaveSQL, execution.ID, execution.EventID, execution.TenantID, execution.RuleSet, execution.Revision,
		execution.DecisionHash, execution.Status, execution.Error, execution.TraceID, execution.CreatedAt)
	if err != nil {
		return fmt.Errorf("save execution: %w", err)
	}
	return nil
}

// Get retrieves an execution by ID, or returns nil when it does not exist.
func (r *ExecutionRepository) Get(ctx context.Context, executionID string) (*domain.Execution, error) {
	exec, err := scanExecution(r.db.QueryRow(ctx, executionRepositoryGetSQL, executionID))
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
	_, err := r.db.Exec(ctx, executionRepositoryUpdateStatusSQL, executionID, status, errMsg, completedAt)
	if err != nil {
		return fmt.Errorf("update execution status: %w", err)
	}
	return nil
}

// GetByEvent retrieves an execution by event ID, or returns nil when no execution exists.
func (r *ExecutionRepository) GetByEvent(ctx context.Context, eventID string) (*domain.Execution, error) {
	exec, err := scanExecution(r.db.QueryRow(ctx, executionRepositoryGetByEventSQL, eventID))
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
	rows, err := r.db.Query(ctx, executionRepositoryGetPendingSQL, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending executions: %w", err)
	}
	defer rows.Close()

	return scanExecutions(rows)
}

const inboxRepositoryInsertSQL = `
		INSERT INTO inbox (tenant_id, event_id, rule_set, status, first_seen_at, partition_key, payload, batch_id)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tenant_id, event_id, rule_set) DO NOTHING
	`

const inboxRepositoryGetSQL = `
		SELECT tenant_id, event_id, rule_set, status, COALESCE(execution_id, ''), first_seen_at, committed_at,
		       COALESCE(partition_key, ''), payload, COALESCE(batch_id, '')
		FROM inbox
		WHERE tenant_id = $1 AND event_id = $2
		LIMIT 1
	`

const inboxRepositoryGetByRuleSetSQL = `
		SELECT tenant_id, event_id, rule_set, status, COALESCE(execution_id, ''), first_seen_at, committed_at,
		       COALESCE(partition_key, ''), payload, COALESCE(batch_id, '')
		FROM inbox
		WHERE tenant_id = $1 AND event_id = $2 AND rule_set = $3
	`

const inboxRepositoryMarkCommittedSQL = `
		UPDATE inbox SET status = 'committed', execution_id = $4, committed_at = $5
		WHERE tenant_id = $1 AND event_id = $2 AND rule_set = $3 AND status = 'processing'
	`

const executionRepositorySaveSQL = `
		INSERT INTO executions (execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (execution_id) DO NOTHING
	`

const executionRepositoryGetSQL = `
		SELECT execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at, completed_at
		FROM executions
		WHERE execution_id = $1
	`

const executionRepositoryUpdateStatusSQL = `
		UPDATE executions SET status = $2, error = $3, completed_at = $4
		WHERE execution_id = $1
	`

const executionRepositoryGetByEventSQL = `
		SELECT execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at, completed_at
		FROM executions
		WHERE event_id = $1
	`

const executionRepositoryGetPendingSQL = `
		SELECT execution_id, event_id, tenant_id, rule_set, revision, decision_hash, status, error, trace_id, created_at, completed_at
		FROM executions
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT $1
	`

func scanInbox(row pgx.Row) (*domain.InboxEntry, error) {
	entry := &domain.InboxEntry{}
	err := row.Scan(&entry.TenantID, &entry.EventID, &entry.RuleSet, &entry.Status, &entry.ExecutionID,
		&entry.FirstSeenAt, &entry.CommittedAt, &entry.PartitionKey, &entry.Payload, &entry.BatchID)
	return entry, err
}

func scanExecution(row pgx.Row) (*domain.Execution, error) {
	execution := &domain.Execution{}
	err := row.Scan(&execution.ID, &execution.EventID, &execution.TenantID, &execution.RuleSet, &execution.Revision,
		&execution.DecisionHash, &execution.Status, &execution.Error, &execution.TraceID, &execution.CreatedAt, &execution.CompletedAt)
	return execution, err
}

func scanExecutions(rows pgx.Rows) ([]*domain.Execution, error) {
	var executions []*domain.Execution
	for rows.Next() {
		execution, err := scanExecution(rows)
		if err != nil {
			return nil, fmt.Errorf("scan execution: %w", err)
		}
		executions = append(executions, execution)
	}
	return executions, nil
}
