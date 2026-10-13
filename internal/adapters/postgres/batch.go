package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/jackc/pgx/v5"
)

// GetRunForTenant retrieves a batch run scoped to a tenant and batch ID.
func (r *BatchRepository) GetRunForTenant(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	run := &domain.BatchRun{}
	err := r.db.QueryRow(ctx, `
		SELECT batch_id, tenant_id, partition_key, rule_set, status, member_count,
		       execution_id, decision_hash, window_from, window_to, created_at
		FROM batch_runs
		WHERE tenant_id = $1 AND batch_id = $2
	`, tenantID, batchID).Scan(
		&run.BatchID, &run.TenantID, &run.PartitionKey, &run.RuleSet, &run.Status,
		&run.MemberCount, &run.ExecutionID, &run.DecisionHash,
		&run.WindowFrom, &run.WindowTo, &run.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get batch run for tenant: %w", err)
	}
	return run, nil
}

// ListRuns returns a paginated list of batch runs for a tenant, optionally filtered by rule set.
func (r *BatchRepository) ListRuns(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error) {
	if strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("tenant ID is required to list batch runs")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	query := `
		SELECT batch_id, tenant_id, partition_key, rule_set, status, member_count,
		       execution_id, decision_hash, window_from, window_to, created_at
		FROM batch_runs
		WHERE tenant_id = $1`
	args := []any{tenantID}
	if ruleSet != "" {
		query += " AND rule_set = $2"
		args = append(args, ruleSet)
	}
	query += fmt.Sprintf(" ORDER BY created_at DESC, batch_id DESC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list batch runs: %w", err)
	}
	defer rows.Close()
	var runs []*domain.BatchRun
	for rows.Next() {
		run := &domain.BatchRun{}
		if err := rows.Scan(
			&run.BatchID, &run.TenantID, &run.PartitionKey, &run.RuleSet, &run.Status,
			&run.MemberCount, &run.ExecutionID, &run.DecisionHash,
			&run.WindowFrom, &run.WindowTo, &run.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan batch run: %w", err)
		}
		runs = append(runs, run)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate batch runs: %w", err)
	}
	return runs, nil
}

// BatchRepository persists batch runs and inbox batch membership in PostgreSQL.
type BatchRepository struct {
	db Querier
}

// NewBatchRepository creates a batch repository backed by a PostgreSQL query interface.
func NewBatchRepository(db Querier) *BatchRepository {
	return &BatchRepository{db: db}
}

// ListUnbatched returns committed inbox entries that have not been assigned to a batch.
func (r *BatchRepository) ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error) {
	if limit <= 0 {
		limit = 400
	}
	rows, err := r.db.Query(ctx, `
		SELECT tenant_id, event_id, status, execution_id, first_seen_at, committed_at,
		       COALESCE(partition_key, ''), COALESCE(rule_set, ''), payload, COALESCE(batch_id, '')
		FROM inbox
		WHERE status = 'committed' AND batch_id IS NULL
		ORDER BY first_seen_at, tenant_id, event_id
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list unbatched: %w", err)
	}
	defer rows.Close()

	var entries []*domain.InboxEntry
	for rows.Next() {
		var e domain.InboxEntry
		if err := rows.Scan(
			&e.TenantID, &e.EventID, &e.Status, &e.ExecutionID,
			&e.FirstSeenAt, &e.CommittedAt,
			&e.PartitionKey, &e.RuleSet, &e.Payload, &e.BatchID,
		); err != nil {
			return nil, fmt.Errorf("scan unbatched: %w", err)
		}
		entries = append(entries, &e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate unbatched rows: %w", err)
	}
	return entries, nil
}

// ListUnbatchedByIDs returns only requested eligible events in one tenant partition.
func (r *BatchRepository) ListUnbatchedByIDs(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) ([]*domain.InboxEntry, error) {
	if len(eventIDs) == 0 {
		return []*domain.InboxEntry{}, nil
	}
	rows, err := r.db.Query(ctx, `
		SELECT tenant_id, event_id, status, execution_id, first_seen_at, committed_at,
		       COALESCE(partition_key, ''), COALESCE(rule_set, ''), payload, COALESCE(batch_id, '')
		FROM inbox
		WHERE tenant_id = $1
		  AND COALESCE(partition_key, '') = $2
		  AND COALESCE(rule_set, '') = $3
		  AND event_id = ANY($4::text[])
		  AND status = 'committed'
		  AND batch_id IS NULL
		ORDER BY first_seen_at, tenant_id, event_id
	`, tenantID, partitionKey, ruleSet, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("list requested unbatched events: %w", err)
	}
	defer rows.Close()

	var entries []*domain.InboxEntry
	for rows.Next() {
		var e domain.InboxEntry
		if err := rows.Scan(
			&e.TenantID, &e.EventID, &e.Status, &e.ExecutionID,
			&e.FirstSeenAt, &e.CommittedAt,
			&e.PartitionKey, &e.RuleSet, &e.Payload, &e.BatchID,
		); err != nil {
			return nil, fmt.Errorf("scan requested unbatched event: %w", err)
		}
		entries = append(entries, &e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate requested unbatched events: %w", err)
	}
	return entries, nil
}

// CompleteBatch writes the run and all inbox membership changes in one PostgreSQL
// transaction. A member may already belong to this same deterministic batch, but
// may never be stolen from another batch.
func (r *BatchRepository) CompleteBatch(ctx context.Context, run *domain.BatchRun, eventIDs []string) error {
	if err := validateBatchCompletion(run, eventIDs); err != nil {
		return err
	}
	tx, err := r.beginBatchCompletion(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := upsertBatchRun(ctx, tx, run); err != nil {
		return err
	}
	if err := associateBatchMembers(ctx, tx, run, eventIDs); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit batch completion: %w", err)
	}
	return nil
}

func validateBatchCompletion(run *domain.BatchRun, eventIDs []string) error {
	if run == nil || run.BatchID == "" || run.TenantID == "" || run.PartitionKey == "" || run.RuleSet == "" {
		return fmt.Errorf("batch run identity is required")
	}
	if len(eventIDs) == 0 || len(eventIDs) != run.MemberCount {
		return fmt.Errorf("batch member count does not match run")
	}
	seen := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		if id == "" {
			return fmt.Errorf("batch event ID must not be empty")
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate batch event ID %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func (r *BatchRepository) beginBatchCompletion(ctx context.Context) (pgx.Tx, error) {
	beginner, ok := r.db.(interface {
		Begin(context.Context) (pgx.Tx, error)
	})
	if !ok {
		return nil, fmt.Errorf("batch repository requires a transaction-capable PostgreSQL connection")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin batch completion: %w", err)
	}
	return tx, nil
}

func upsertBatchRun(ctx context.Context, tx pgx.Tx, run *domain.BatchRun) error {
	tag, err := tx.Exec(ctx, `
		INSERT INTO batch_runs (batch_id, tenant_id, partition_key, rule_set, status, member_count,
		                        execution_id, decision_hash, window_from, window_to, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (batch_id) DO UPDATE SET
			status = EXCLUDED.status, member_count = EXCLUDED.member_count,
			execution_id = EXCLUDED.execution_id, decision_hash = EXCLUDED.decision_hash,
			window_from = EXCLUDED.window_from, window_to = EXCLUDED.window_to
		WHERE batch_runs.tenant_id = EXCLUDED.tenant_id
		  AND batch_runs.partition_key = EXCLUDED.partition_key
		  AND batch_runs.rule_set = EXCLUDED.rule_set
	`, run.BatchID, run.TenantID, run.PartitionKey, run.RuleSet, run.Status,
		run.MemberCount, run.ExecutionID, run.DecisionHash, run.WindowFrom, run.WindowTo, run.CreatedAt)
	if err != nil {
		return fmt.Errorf("upsert batch run: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("batch ID %q already belongs to a different tenant or scope", run.BatchID)
	}
	return nil
}

func associateBatchMembers(ctx context.Context, tx pgx.Tx, run *domain.BatchRun, eventIDs []string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE inbox
		SET batch_id = $5
		WHERE tenant_id = $1
		  AND COALESCE(partition_key, '') = $2
		  AND COALESCE(rule_set, '') = $3
		  AND event_id = ANY($4::text[])
		  AND status = 'committed'
		  AND (batch_id IS NULL OR batch_id = $5)
	`, run.TenantID, run.PartitionKey, run.RuleSet, eventIDs, run.BatchID)
	if err != nil {
		return fmt.Errorf("associate batch members: %w", err)
	}
	if tag.RowsAffected() != int64(len(eventIDs)) {
		return fmt.Errorf("batch membership conflict: matched %d of %d requested events", tag.RowsAffected(), len(eventIDs))
	}
	return nil
}

// MarkBatched assigns an inbox event to a batch if it is not already assigned.
func (r *BatchRepository) MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE inbox SET batch_id = $3
		WHERE tenant_id = $1 AND event_id = $2 AND batch_id IS NULL
	`, tenantID, eventID, batchID)
	if err != nil {
		return fmt.Errorf("mark batched: %w", err)
	}
	return nil
}

// SaveRun inserts or updates a batch run.
func (r *BatchRepository) SaveRun(ctx context.Context, run *domain.BatchRun) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO batch_runs (batch_id, tenant_id, partition_key, rule_set, status, member_count,
		                        execution_id, decision_hash, window_from, window_to, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (batch_id) DO UPDATE SET
			status = EXCLUDED.status,
			member_count = EXCLUDED.member_count,
			execution_id = EXCLUDED.execution_id,
			decision_hash = EXCLUDED.decision_hash,
			window_from = EXCLUDED.window_from,
			window_to = EXCLUDED.window_to
	`, run.BatchID, run.TenantID, run.PartitionKey, run.RuleSet, run.Status,
		run.MemberCount, run.ExecutionID, run.DecisionHash,
		run.WindowFrom, run.WindowTo, run.CreatedAt)
	if err != nil {
		return fmt.Errorf("save batch run: %w", err)
	}
	return nil
}

// GetRun retrieves a batch run by its batch ID, or returns nil when it does not exist.
func (r *BatchRepository) GetRun(ctx context.Context, batchID string) (*domain.BatchRun, error) {
	run := &domain.BatchRun{}
	err := r.db.QueryRow(ctx, `
		SELECT batch_id, tenant_id, partition_key, rule_set, status, member_count,
		       execution_id, decision_hash, window_from, window_to, created_at
		FROM batch_runs
		WHERE batch_id = $1
	`, batchID).Scan(
		&run.BatchID, &run.TenantID, &run.PartitionKey, &run.RuleSet, &run.Status,
		&run.MemberCount, &run.ExecutionID, &run.DecisionHash,
		&run.WindowFrom, &run.WindowTo, &run.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get batch run: %w", err)
	}
	return run, nil
}
