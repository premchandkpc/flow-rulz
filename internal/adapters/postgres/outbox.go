// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// OutboxRepository is the PostgreSQL persistence adapter.
type OutboxRepository struct {
	db Querier
}

// NewOutboxRepository performs its documented repository operation.
func NewOutboxRepository(db Querier) *OutboxRepository {
	return &OutboxRepository{db: db}
}

// Insert performs its documented repository operation.
func (r *OutboxRepository) Insert(ctx context.Context, effects []domain.OutboxEffect) error {
	for _, ef := range effects {
		if ef.TenantID == "" {
			return fmt.Errorf("insert outbox: tenant_id is required")
		}
		_, err := r.db.Exec(ctx, `
			INSERT INTO outbox_effects (effect_id, execution_id, tenant_id, destination, name, payload, effect_type, status, attempts, max_attempts, available_at, last_error, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
			ON CONFLICT (effect_id) DO NOTHING
		`, ef.ID, ef.ExecutionID, ef.TenantID, ef.Destination, ef.Name, ef.Payload, ef.EffectType,
			ef.Status, ef.Attempts, ef.MaxAttempts, ef.AvailableAt, ef.LastError, ef.CreatedAt, ef.UpdatedAt)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}
	return nil
}

// ClaimPending performs its documented repository operation.
func (r *OutboxRepository) ClaimPending(ctx context.Context, batchSize int, owner string, claimTTL time.Duration) ([]domain.OutboxEffect, error) {
	if err := validateClaimArguments(batchSize, owner, claimTTL); err != nil {
		return nil, err
	}
	rows, err := r.db.Query(ctx, claimPendingOutboxSQL, batchSize, owner, claimTTL)
	if err != nil {
		return nil, fmt.Errorf("claim pending: %w", err)
	}
	defer rows.Close()
	return scanOutboxEffects(rows)
}

const claimPendingOutboxSQL = `
	WITH candidates AS (
		SELECT effect_id
		FROM outbox_effects
		WHERE (status = 'pending' AND available_at <= NOW())
		   OR (status = 'claimed' AND claim_expires_at <= NOW())
		ORDER BY available_at, created_at
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE outbox_effects AS o
	SET status = 'claimed',
		claimed_by = $2,
		claimed_at = NOW(),
		claim_expires_at = NOW() + $3,
		updated_at = NOW()
	FROM candidates
	WHERE o.effect_id = candidates.effect_id
	RETURNING o.effect_id, o.execution_id, o.tenant_id, o.destination, o.name, o.payload, o.effect_type,
	          o.status, o.attempts, o.max_attempts, o.available_at, o.last_error, o.created_at, o.updated_at
`

func scanOutboxEffects(rows pgx.Rows) ([]domain.OutboxEffect, error) {
	var effects []domain.OutboxEffect
	for rows.Next() {
		effect, err := scanOutboxEffect(rows)
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return effects, nil
}

func scanOutboxEffect(rows pgx.Rows) (domain.OutboxEffect, error) {
	var effect domain.OutboxEffect
	if err := rows.Scan(
		&effect.ID, &effect.ExecutionID, &effect.TenantID, &effect.Destination, &effect.Name, &effect.Payload, &effect.EffectType,
		&effect.Status, &effect.Attempts, &effect.MaxAttempts, &effect.AvailableAt, &effect.LastError, &effect.CreatedAt, &effect.UpdatedAt,
	); err != nil {
		return domain.OutboxEffect{}, fmt.Errorf("scan outbox: %w", err)
	}
	return effect, nil
}

// MarkDelivered performs its documented repository operation.
func (r *OutboxRepository) MarkDelivered(ctx context.Context, effectIDs []string, claimant string) error {
	if len(effectIDs) == 0 {
		return nil
	}
	// Build placeholders for the effect IDs; all settlement timestamps use the database clock.
	placeholders := make([]string, len(effectIDs))
	args := make([]any, 1, len(effectIDs)+1)
	args[0] = claimant
	for i, id := range effectIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, id)
	}

	query := fmt.Sprintf(`
		UPDATE outbox_effects
		SET status = 'delivered', claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = NOW()
		WHERE effect_id IN (%s) AND status = 'claimed' AND claimed_by = $1 AND claim_expires_at > NOW()
	`, strings.Join(placeholders, ","))

	tag, err := r.db.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("mark delivered: %w", err)
	}
	if int(tag.RowsAffected()) != len(effectIDs) {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

// ScheduleRetry performs its documented repository operation.
func (r *OutboxRepository) ScheduleRetry(ctx context.Context, effectID, claimant string, delay time.Duration, attempts int, errMsg string) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE outbox_effects
		SET status = 'pending', attempts = $3,
			available_at = NOW() + ($4 * INTERVAL '1 millisecond'), last_error = $5,
			claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = NOW()
		WHERE effect_id = $1 AND status = 'claimed' AND claimed_by = $2 AND claim_expires_at > NOW()
	`, effectID, claimant, attempts, delay.Milliseconds(), errMsg)
	if err != nil {
		return fmt.Errorf("schedule retry: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

// Quarantine performs its documented repository operation.
func (r *OutboxRepository) Quarantine(ctx context.Context, effectID, claimant, errMsg string) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE outbox_effects
		SET status = 'quarantined', last_error = $3,
			claimed_by = NULL, claimed_at = NULL, claim_expires_at = NULL, updated_at = NOW()
		WHERE effect_id = $1 AND status = 'claimed' AND claimed_by = $2 AND claim_expires_at > NOW()
	`, effectID, claimant, errMsg)
	if err != nil {
		return fmt.Errorf("quarantine effect: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrOutboxClaimLost
	}
	return nil
}

// GetPending performs its documented repository operation.
func (r *OutboxRepository) GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error) {
	limit = normalizeOutboxLimit(limit)
	rows, err := r.db.Query(ctx, `
		SELECT effect_id, execution_id, tenant_id, destination, name, payload, effect_type,
		       status, attempts, max_attempts, available_at, last_error, created_at, updated_at
		FROM outbox_effects
		WHERE status = 'pending' AND available_at <= NOW()
		ORDER BY available_at, created_at
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending: %w", err)
	}
	defer rows.Close()

	var effects []domain.OutboxEffect
	for rows.Next() {
		var ef domain.OutboxEffect
		if err := rows.Scan(
			&ef.ID, &ef.ExecutionID, &ef.TenantID, &ef.Destination, &ef.Name, &ef.Payload, &ef.EffectType,
			&ef.Status, &ef.Attempts, &ef.MaxAttempts, &ef.AvailableAt, &ef.LastError, &ef.CreatedAt, &ef.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		effects = append(effects, ef)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return effects, nil
}

// GetByExecution performs its documented repository operation.
func (r *OutboxRepository) GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error) {
	rows, err := r.db.Query(ctx, `
		SELECT effect_id, execution_id, tenant_id, destination, name, payload, effect_type,
		       status, attempts, max_attempts, available_at, last_error, created_at, updated_at
		FROM outbox_effects
		WHERE execution_id = $1
		ORDER BY created_at
	`, executionID)
	if err != nil {
		return nil, fmt.Errorf("get by execution: %w", err)
	}
	defer rows.Close()

	var effects []domain.OutboxEffect
	for rows.Next() {
		var ef domain.OutboxEffect
		if err := rows.Scan(
			&ef.ID, &ef.ExecutionID, &ef.TenantID, &ef.Destination, &ef.Name, &ef.Payload, &ef.EffectType,
			&ef.Status, &ef.Attempts, &ef.MaxAttempts, &ef.AvailableAt, &ef.LastError, &ef.CreatedAt, &ef.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan outbox: %w", err)
		}
		effects = append(effects, ef)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox rows: %w", err)
	}
	return effects, nil
}

func validateClaimArguments(batchSize int, owner string, claimTTL time.Duration) error {
	if batchSize <= 0 || batchSize > 1000 {
		return fmt.Errorf("claim batch size must be between 1 and 1000")
	}
	if strings.TrimSpace(owner) == "" {
		return fmt.Errorf("claim owner is required")
	}
	if claimTTL <= 0 {
		return fmt.Errorf("claim TTL must be > 0")
	}
	return nil
}

func normalizeOutboxLimit(limit int) int {
	if limit <= 0 {
		return 100
	}
	if limit > 500 {
		return 500
	}
	return limit
}
