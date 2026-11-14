// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/jackc/pgx/v5"
)

// RuleRepository defines the rule repository operation.
type RuleRepository struct {
	db Querier
}

// NewRuleRepository performs the new rule repository operation.
func NewRuleRepository(db Querier) *RuleRepository {
	return &RuleRepository{db: db}
}

// GetActive performs the get active operation.
func (r *RuleRepository) GetActive(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleRevision, error) {
	return readRevision(r.db.QueryRow(ctx, ruleRepositoryGetActiveSQL, tenantScope, ruleSet), "query active revision")
}

// Save performs the save operation.
func (r *RuleRepository) Save(ctx context.Context, revision *domain.RuleRevision) error {
	compiled, inputContract, err := marshalRevision(revision)
	if err != nil {
		return err
	}
	_, err = r.db.Exec(ctx, ruleRepositorySaveSQL, revision.TenantScope, revision.RuleID, revision.Revision, revision.Source, compiled,
		revision.ContentHash, revision.CompilerVersion, revision.MatchMode, revision.CreatedAt, inputContract)
	if err != nil {
		return fmt.Errorf("save revision: %w", err)
	}
	return nil
}

// Get performs the get operation.
func (r *RuleRepository) Get(ctx context.Context, tenantScope string, ruleSet string, revision int64) (*domain.RuleRevision, error) {
	return readRevision(r.db.QueryRow(ctx, ruleRepositoryGetSQL, tenantScope, ruleSet, revision), "get revision")
}

// List performs the list operation.
func (r *RuleRepository) List(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error) {
	rows, err := r.db.Query(ctx, ruleRepositoryListSQL, tenantScope)
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	defer rows.Close()

	return scanRevisions(rows)
}

// Delete performs the delete operation.
func (r *RuleRepository) Delete(ctx context.Context, tenantScope string, ruleSet string, revision int64) error {
	_, err := r.db.Exec(ctx, ruleRepositoryDeleteSQL, tenantScope, ruleSet, revision)
	if err != nil {
		return fmt.Errorf("delete revision: %w", err)
	}
	return nil
}

// ActivationRepository defines the activation repository operation.
type ActivationRepository struct {
	db Querier
}

// NewActivationRepository performs the new activation repository operation.
func NewActivationRepository(db Querier) *ActivationRepository {
	return &ActivationRepository{db: db}
}

// Get performs the get operation.
func (a *ActivationRepository) Get(ctx context.Context, tenantScope string, ruleSet string) (*domain.RuleActivation, error) {
	act := &domain.RuleActivation{}
	err := a.db.QueryRow(ctx, activationRepositoryGetSQL, tenantScope, ruleSet).Scan(
		&act.TenantScope, &act.RuleSet, &act.Revision, &act.Version, &act.Actor, &act.ActivatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get activation: %w", err)
	}
	return act, nil
}

// Set performs the set operation.
func (a *ActivationRepository) Set(ctx context.Context, activation *domain.RuleActivation) error {
	_, err := a.db.Exec(ctx, activationRepositorySetSQL, activation.TenantScope, activation.RuleSet, activation.Revision, activation.Version,
		activation.Actor, activation.ActivatedAt)
	if err != nil {
		return fmt.Errorf("set activation: %w", err)
	}
	return nil
}

// SetWithVersion atomically sets the activation only if the current version matches expectedVersion.
// Returns error if version mismatch (optimistic concurrency conflict).
func (a *ActivationRepository) SetWithVersion(ctx context.Context, activation *domain.RuleActivation, expectedVersion int64) error {
	var version int64
	err := a.db.QueryRow(ctx, activationRepositorySetWithVersionSQL, activation.TenantScope, activation.RuleSet, activation.Revision, activation.Version,
		activation.Actor, activation.ActivatedAt, expectedVersion).Scan(&version)
	if err != nil {
		return fmt.Errorf("set activation with version: %w", err)
	}
	if version != expectedVersion+1 {
		return domain.ErrActivationConflict
	}
	return nil
}

// List performs the list operation.
func (a *ActivationRepository) List(ctx context.Context, tenantScope string) ([]*domain.RuleActivation, error) {
	rows, err := a.db.Query(ctx, activationRepositoryListSQL, tenantScope)
	if err != nil {
		return nil, fmt.Errorf("list activations: %w", err)
	}
	defer rows.Close()

	return scanActivations(rows)
}

// Delete performs the delete operation.
func (a *ActivationRepository) Delete(ctx context.Context, tenantScope string, ruleSet string) error {
	_, err := a.db.Exec(ctx, activationRepositoryDeleteSQL, tenantScope, ruleSet)
	if err != nil {
		return fmt.Errorf("delete activation: %w", err)
	}
	return nil
}

const ruleRepositoryGetActiveSQL = `
		SELECT r.tenant_scope, r.rule_id, r.revision, r.source, r.compiled, r.content_hash, 
		       r.compiler_version, r.match_mode, r.created_at, r.input_contract
		FROM rule_revisions r
		JOIN rule_activations a ON r.tenant_scope = a.tenant_scope AND r.rule_id = a.rule_set AND r.revision = a.revision
		WHERE a.tenant_scope = $1 AND a.rule_set = $2
	`

const ruleRepositorySaveSQL = `
		INSERT INTO rule_revisions (tenant_scope, rule_id, revision, source, compiled, content_hash, compiler_version, match_mode, created_at, input_contract)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (tenant_scope, rule_id, revision) DO NOTHING
	`

const ruleRepositoryGetSQL = `
		SELECT tenant_scope, rule_id, revision, source, compiled, content_hash, compiler_version, match_mode, created_at, input_contract
		FROM rule_revisions
		WHERE tenant_scope = $1 AND rule_id = $2 AND revision = $3
	`

const ruleRepositoryListSQL = `
		SELECT tenant_scope, rule_id, revision, source, compiled, content_hash, compiler_version, match_mode, created_at, input_contract
		FROM rule_revisions
		WHERE tenant_scope = $1
		ORDER BY rule_id, revision DESC
	`

const ruleRepositoryDeleteSQL = `
		DELETE FROM rule_revisions WHERE tenant_scope = $1 AND rule_id = $2 AND revision = $3
	`

const activationRepositoryGetSQL = `
		SELECT tenant_scope, rule_set, revision, version, actor, activated_at
		FROM rule_activations
		WHERE tenant_scope = $1 AND rule_set = $2
	`

const activationRepositorySetSQL = `
		INSERT INTO rule_activations (tenant_scope, rule_set, revision, version, actor, activated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_scope, rule_set) DO UPDATE SET
			revision = EXCLUDED.revision,
			version = rule_activations.version + 1,
			actor = EXCLUDED.actor,
			activated_at = EXCLUDED.activated_at
	`

const activationRepositorySetWithVersionSQL = `
		INSERT INTO rule_activations (tenant_scope, rule_set, revision, version, actor, activated_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (tenant_scope, rule_set) DO UPDATE SET
			revision = CASE WHEN rule_activations.version = $7 THEN EXCLUDED.revision ELSE rule_activations.revision END,
			version = CASE WHEN rule_activations.version = $7 THEN rule_activations.version + 1 ELSE rule_activations.version END,
			actor = CASE WHEN rule_activations.version = $7 THEN EXCLUDED.actor ELSE rule_activations.actor END,
			activated_at = CASE WHEN rule_activations.version = $7 THEN EXCLUDED.activated_at ELSE rule_activations.activated_at END
		WHERE rule_activations.version = $7 OR rule_activations.tenant_scope IS NULL
		RETURNING version
	`

const activationRepositoryListSQL = `
		SELECT tenant_scope, rule_set, revision, version, actor, activated_at
		FROM rule_activations
		WHERE tenant_scope = $1
		ORDER BY rule_set
	`

const activationRepositoryDeleteSQL = `
		DELETE FROM rule_activations WHERE tenant_scope = $1 AND rule_set = $2
	`

type revisionRow struct {
	revision                domain.RuleRevision
	compiled, inputContract []byte
}

func scanRevisionRow(row pgx.Row) (*revisionRow, error) {
	data := &revisionRow{}
	rev := &data.revision
	err := row.Scan(&rev.TenantScope, &rev.RuleID, &rev.Revision, &rev.Source, &data.compiled,
		&rev.ContentHash, &rev.CompilerVersion, &rev.MatchMode, &rev.CreatedAt, &data.inputContract)
	return data, err
}

func (data *revisionRow) decode() (*domain.RuleRevision, error) {
	if err := json.Unmarshal(data.compiled, &data.revision.Compiled); err != nil {
		return nil, fmt.Errorf("unmarshal compiled: %w", err)
	}
	if len(data.inputContract) > 0 {
		var ref domain.ContractRef
		if err := json.Unmarshal(data.inputContract, &ref); err != nil {
			return nil, fmt.Errorf("unmarshal input_contract: %w", err)
		}
		data.revision.InputContract = &ref
	}
	return &data.revision, nil
}

func readRevision(row pgx.Row, operation string) (*domain.RuleRevision, error) {
	data, err := scanRevisionRow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	return data.decode()
}

func scanRevisions(rows pgx.Rows) ([]*domain.RuleRevision, error) {
	var revisions []*domain.RuleRevision
	for rows.Next() {
		revision, err := readRevision(rows, "scan revision")
		if err != nil {
			return nil, err
		}
		revisions = append(revisions, revision)
	}
	return revisions, nil
}

func marshalRevision(revision *domain.RuleRevision) ([]byte, []byte, error) {
	compiled, err := json.Marshal(revision.Compiled)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal compiled: %w", err)
	}
	if revision.InputContract == nil {
		return compiled, nil, nil
	}
	inputContract, err := json.Marshal(revision.InputContract)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal input_contract: %w", err)
	}
	return compiled, inputContract, nil
}

func scanActivations(rows pgx.Rows) ([]*domain.RuleActivation, error) {
	var activations []*domain.RuleActivation
	for rows.Next() {
		act := &domain.RuleActivation{}
		if err := rows.Scan(&act.TenantScope, &act.RuleSet, &act.Revision, &act.Version, &act.Actor, &act.ActivatedAt); err != nil {
			return nil, fmt.Errorf("scan activation: %w", err)
		}
		activations = append(activations, act)
	}
	return activations, nil
}
