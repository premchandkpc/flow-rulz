// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/jackc/pgx/v5"
)

// QuarantineRepository persists failure evidence. Replay records an operator
// action; source-specific republishing belongs to a dedicated replay workflow.
type QuarantineRepository struct {
	db Querier
}

// NewQuarantineRepository performs the new quarantine repository operation.
func NewQuarantineRepository(db Querier) *QuarantineRepository {
	return &QuarantineRepository{db: db}
}

// Save performs the save operation.
func (r *QuarantineRepository) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO quarantine (quarantine_id, source_type, source_id, error_class, payload_ref, event_id, tenant_id, error_message, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
		ON CONFLICT (quarantine_id) DO NOTHING
	`, entry.ID, entry.SourceType, entry.SourceID, entry.ErrorClass, entry.PayloadRef, entry.EventID, entry.TenantID, entry.Error, entry.CreatedAt)
	if err != nil {
		return fmt.Errorf("save quarantine entry: %w", err)
	}
	return nil
}

// Get performs the get operation.
func (r *QuarantineRepository) Get(ctx context.Context, tenantID, id string) (*domain.QuarantineEntry, error) {
	if tenantID == "" {
		return nil, nil
	}
	entry := &domain.QuarantineEntry{}
	err := r.db.QueryRow(ctx, `
		SELECT quarantine_id, source_type, source_id, error_class, COALESCE(payload_ref, ''),
		       COALESCE(event_id, ''), COALESCE(tenant_id, ''), COALESCE(error_message, ''), created_at
		FROM quarantine WHERE tenant_id = $1 AND quarantine_id = $2
	`, tenantID, id).Scan(&entry.ID, &entry.SourceType, &entry.SourceID, &entry.ErrorClass, &entry.PayloadRef,
		&entry.EventID, &entry.TenantID, &entry.Error, &entry.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get quarantine entry: %w", err)
	}
	return entry, nil
}

// Replay performs the replay operation.
func (r *QuarantineRepository) Replay(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	tag, err := r.db.Exec(ctx, `UPDATE quarantine SET updated_at = NOW() WHERE tenant_id = $1 AND quarantine_id = $2`, tenantID, id)
	if err != nil {
		return fmt.Errorf("record quarantine replay: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrQuarantineNotFound
	}
	return nil
}

// Delete performs the delete operation.
func (r *QuarantineRepository) Delete(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	tag, err := r.db.Exec(ctx, `DELETE FROM quarantine WHERE tenant_id = $1 AND quarantine_id = $2`, tenantID, id)
	if err != nil {
		return fmt.Errorf("delete quarantine entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrQuarantineNotFound
	}
	return nil
}

// List performs the list operation.
func (r *QuarantineRepository) List(ctx context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	if filter.TenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	filter.Limit, filter.Offset = normalizeQuarantinePagination(filter.Limit, filter.Offset)

	query := `
		SELECT quarantine_id, source_type, source_id, error_class, COALESCE(payload_ref, ''),
		       COALESCE(event_id, ''), COALESCE(tenant_id, ''), COALESCE(error_message, ''), created_at
		FROM quarantine WHERE 1=1
	`
	args := []interface{}{}
	argIdx := 1

	query += fmt.Sprintf(" AND tenant_id = $%d", argIdx)
	args = append(args, filter.TenantID)
	argIdx++
	if filter.SourceType != "" {
		query += fmt.Sprintf(" AND source_type = $%d", argIdx)
		args = append(args, filter.SourceType)
		argIdx++
	}
	if filter.ErrorClass != "" {
		query += fmt.Sprintf(" AND error_class = $%d", argIdx)
		args = append(args, filter.ErrorClass)
		argIdx++
	}
	if !filter.From.IsZero() {
		query += fmt.Sprintf(" AND created_at >= $%d", argIdx)
		args = append(args, filter.From)
		argIdx++
	}
	if !filter.To.IsZero() {
		query += fmt.Sprintf(" AND created_at <= $%d", argIdx)
		args = append(args, filter.To)
		argIdx++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, filter.Limit, filter.Offset)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list quarantine entries: %w", err)
	}
	defer rows.Close()

	var entries []*domain.QuarantineEntry
	for rows.Next() {
		entry := &domain.QuarantineEntry{}
		err := rows.Scan(&entry.ID, &entry.SourceType, &entry.SourceID, &entry.ErrorClass, &entry.PayloadRef,
			&entry.EventID, &entry.TenantID, &entry.Error, &entry.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("scan quarantine entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate quarantine entries: %w", err)
	}
	return entries, nil
}

// QuarantineFilter defines the quarantine filter operation.
type QuarantineFilter struct {
	TenantID   string
	SourceType string
	ErrorClass string
	From       time.Time
	To         time.Time
	Limit      int
	Offset     int
}

func normalizeQuarantinePagination(limit, offset int) (int, int) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}
	return limit, offset
}
