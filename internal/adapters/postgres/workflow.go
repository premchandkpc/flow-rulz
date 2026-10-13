// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/jackc/pgx/v5"
)

// WorkflowRepository is the PostgreSQL persistence adapter.
type WorkflowRepository struct {
	db Querier
}

// NewWorkflowRepository performs its documented repository operation.
func NewWorkflowRepository(db Querier) *WorkflowRepository {
	return &WorkflowRepository{db: db}
}

// Save performs its documented repository operation.
func (r *WorkflowRepository) Save(ctx context.Context, workflow *domain.WorkflowInstance) error {
	if workflow == nil {
		return fmt.Errorf("workflow is required")
	}
	tag, err := r.db.Exec(ctx, `
		INSERT INTO workflow_instances (workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (workflow_id) DO NOTHING
	`, workflow.WorkflowID, workflow.TenantID, workflow.WorkflowType, workflow.State, workflow.Version, workflow.CurrentRevision, workflow.Context, workflow.CreatedAt, workflow.UpdatedAt, workflow.CompletedAt)
	if err != nil {
		return fmt.Errorf("save workflow: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var existingTenantID, existingWorkflowType string
		lookupErr := r.db.QueryRow(ctx, `
			SELECT tenant_id, workflow_type
			FROM workflow_instances
			WHERE workflow_id = $1
		`, workflow.WorkflowID).Scan(&existingTenantID, &existingWorkflowType)
		if lookupErr != nil {
			return fmt.Errorf("verify duplicate workflow: %w", lookupErr)
		}
		if !sameWorkflowCreateIdentity(existingTenantID, existingWorkflowType, workflow) {
			return domain.ErrWorkflowConflict
		}
	}
	return nil
}

func sameWorkflowCreateIdentity(existingTenantID, existingWorkflowType string, requested *domain.WorkflowInstance) bool {
	return requested != nil &&
		existingTenantID == requested.TenantID &&
		existingWorkflowType == requested.WorkflowType
}

// Get performs its documented repository operation.
func (r *WorkflowRepository) Get(ctx context.Context, tenantID, workflowID string) (*domain.WorkflowInstance, error) {
	w := &domain.WorkflowInstance{}
	var completedAt *time.Time
	err := r.db.QueryRow(ctx, `
		SELECT workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at
		FROM workflow_instances
		WHERE tenant_id = $1 AND workflow_id = $2
	`, tenantID, workflowID).Scan(
		&w.WorkflowID, &w.TenantID, &w.WorkflowType, &w.State, &w.Version,
		&w.CurrentRevision, &w.Context, &w.CreatedAt, &w.UpdatedAt, &completedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow: %w", err)
	}
	w.CompletedAt = completedAt
	return w, nil
}

// Update performs its documented repository operation.
func (r *WorkflowRepository) Update(ctx context.Context, workflow *domain.WorkflowInstance) error {
	updatedAt := workflow.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	tag, err := r.db.Exec(ctx, `
		UPDATE workflow_instances
		SET state = $3, version = $4, current_revision = $5, context = $6, updated_at = $7, completed_at = $8
		WHERE tenant_id = $1 AND workflow_id = $2 AND version = $9
	`, workflow.TenantID, workflow.WorkflowID, workflow.State, workflow.Version, workflow.CurrentRevision, workflow.Context, updatedAt, workflow.CompletedAt, workflow.Version-1)
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrWorkflowConflict
	}
	return nil
}

// GetByTenantAndState performs its documented repository operation.
func (r *WorkflowRepository) GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error) {
	if limit <= 0 {
		limit = 100
	}
	query := `
		SELECT workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at
		FROM workflow_instances
		WHERE tenant_id = $1`
	args := []any{tenantID}
	if state != "" {
		query += " AND state = $2"
		args = append(args, state)
	}
	query += fmt.Sprintf(" ORDER BY updated_at LIMIT $%d", len(args)+1)
	args = append(args, limit)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get workflows by tenant and state: %w", err)
	}
	defer rows.Close()

	var workflows []*domain.WorkflowInstance
	for rows.Next() {
		w := &domain.WorkflowInstance{}
		var completedAt *time.Time
		if err := rows.Scan(
			&w.WorkflowID, &w.TenantID, &w.WorkflowType, &w.State, &w.Version,
			&w.CurrentRevision, &w.Context, &w.CreatedAt, &w.UpdatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		w.CompletedAt = completedAt
		workflows = append(workflows, w)
	}
	return workflows, nil
}

// GetByCorrelation performs its documented repository operation.
func (r *WorkflowRepository) GetByCorrelation(ctx context.Context, tenantID, correlationID string) ([]*domain.WorkflowInstance, error) {
	rows, err := r.db.Query(ctx, `
		SELECT workflow_id, tenant_id, workflow_type, state, version, current_revision, context, created_at, updated_at, completed_at
		FROM workflow_instances
		WHERE tenant_id = $1 AND (context->>'correlation_id') = $2
		ORDER BY updated_at
	`, tenantID, correlationID)
	if err != nil {
		return nil, fmt.Errorf("get workflows by correlation: %w", err)
	}
	defer rows.Close()

	var workflows []*domain.WorkflowInstance
	for rows.Next() {
		w := &domain.WorkflowInstance{}
		var completedAt *time.Time
		if err := rows.Scan(
			&w.WorkflowID, &w.TenantID, &w.WorkflowType, &w.State, &w.Version,
			&w.CurrentRevision, &w.Context, &w.CreatedAt, &w.UpdatedAt, &completedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		w.CompletedAt = completedAt
		workflows = append(workflows, w)
	}
	return workflows, nil
}

// WorkflowDefinitionRepository is the PostgreSQL persistence adapter.
type WorkflowDefinitionRepository struct {
	db Querier
}

// NewWorkflowDefinitionRepository performs its documented repository operation.
func NewWorkflowDefinitionRepository(db Querier) *WorkflowDefinitionRepository {
	return &WorkflowDefinitionRepository{db: db}
}

// Save performs its documented repository operation.
func (r *WorkflowDefinitionRepository) Save(ctx context.Context, def *domain.WorkflowDefinition) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO workflow_definitions (workflow_type, version, states, transitions, rules, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (workflow_type, version) DO UPDATE SET
			states = EXCLUDED.states,
			transitions = EXCLUDED.transitions,
			rules = EXCLUDED.rules,
			updated_at = EXCLUDED.updated_at
	`, def.WorkflowType, def.Version, def.States, def.Transitions, def.Rules, def.CreatedAt, def.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save workflow definition: %w", err)
	}
	return nil
}

// Get performs its documented repository operation.
func (r *WorkflowDefinitionRepository) Get(ctx context.Context, workflowType string, version int64) (*domain.WorkflowDefinition, error) {
	def := &domain.WorkflowDefinition{}
	err := r.db.QueryRow(ctx, `
		SELECT workflow_type, version, states, transitions, rules, created_at, updated_at
		FROM workflow_definitions
		WHERE workflow_type = $1 AND version = $2
	`, workflowType, version).Scan(
		&def.WorkflowType, &def.Version, &def.States, &def.Transitions, &def.Rules, &def.CreatedAt, &def.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get workflow definition: %w", err)
	}
	return def, nil
}

// List performs its documented repository operation.
func (r *WorkflowDefinitionRepository) List(ctx context.Context) ([]*domain.WorkflowDefinition, error) {
	rows, err := r.db.Query(ctx, `
		SELECT workflow_type, version, states, transitions, rules, created_at, updated_at
		FROM workflow_definitions
		ORDER BY workflow_type, version DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list workflow definitions: %w", err)
	}
	defer rows.Close()

	var defs []*domain.WorkflowDefinition
	for rows.Next() {
		def := &domain.WorkflowDefinition{}
		if err := rows.Scan(
			&def.WorkflowType, &def.Version, &def.States, &def.Transitions, &def.Rules, &def.CreatedAt, &def.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan workflow definition: %w", err)
		}
		defs = append(defs, def)
	}
	return defs, nil
}

// Delete performs its documented repository operation.
func (r *WorkflowDefinitionRepository) Delete(ctx context.Context, workflowType string, version int64) error {
	_, err := r.db.Exec(ctx, `
		DELETE FROM workflow_definitions WHERE workflow_type = $1 AND version = $2
	`, workflowType, version)
	if err != nil {
		return fmt.Errorf("delete workflow definition: %w", err)
	}
	return nil
}
