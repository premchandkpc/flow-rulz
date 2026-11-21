// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// BindingRepository provides PostgreSQL implementation of contract-to-ruleset bindings.
type BindingRepository struct {
	db Querier
}

// NewBindingRepository creates a new binding repository.
func NewBindingRepository(db Querier) *BindingRepository {
	return &BindingRepository{db: db}
}

// Save inserts or updates a binding.
func (r *BindingRepository) Save(ctx context.Context, binding *domain.ContractBinding) error {
	prepareBindingForSave(binding)
	_, err := r.db.Exec(ctx, bindingRepositorySaveSQL, binding.ID, binding.TenantID, binding.ContractName, binding.ContractVersion, binding.ContractHash,
		binding.RuleSet, binding.Priority, binding.ActivationPolicy, binding.CreatedAt, binding.UpdatedAt)
	if err != nil {
		return fmt.Errorf("save binding: %w", err)
	}
	return nil
}

// Get retrieves a binding by ID.
func (r *BindingRepository) Get(ctx context.Context, tenantID, bindingID string) (*domain.ContractBinding, error) {
	row := r.db.QueryRow(ctx, bindingRepositoryGetSQL, bindingID, tenantID)

	b, err := scanBinding(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get binding: %w", err)
	}
	return b, nil
}

// ListByContract returns all bindings for a specific contract.
func (r *BindingRepository) ListByContract(ctx context.Context, tenantID, contractName, contractVersion string) ([]*domain.ContractBinding, error) {
	rows, err := r.db.Query(ctx, bindingRepositoryListByContractSQL, tenantID, contractName, contractVersion)
	if err != nil {
		return nil, fmt.Errorf("list bindings by contract: %w", err)
	}
	defer rows.Close()

	return scanBindings(rows)
}

// ListByRuleSet returns all bindings for a specific ruleset.
func (r *BindingRepository) ListByRuleSet(ctx context.Context, tenantID, ruleSet string) ([]*domain.ContractBinding, error) {
	rows, err := r.db.Query(ctx, bindingRepositoryListByRuleSetSQL, tenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("list bindings by ruleset: %w", err)
	}
	defer rows.Close()

	return scanBindings(rows)
}

// Delete removes a binding by ID.
func (r *BindingRepository) Delete(ctx context.Context, tenantID, bindingID string) error {
	_, err := r.db.Exec(ctx, bindingRepositoryDeleteSQL, bindingID, tenantID)
	if err != nil {
		return fmt.Errorf("delete binding: %w", err)
	}
	return nil
}

// Compile-time interface check
var _ ports.BindingRepository = (*BindingRepository)(nil)

const bindingRepositorySaveSQL = `
		INSERT INTO contract_bindings (id, tenant_id, contract_name, contract_version, contract_hash, rule_set, priority, activation_policy, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO UPDATE SET
			tenant_id = EXCLUDED.tenant_id,
			contract_name = EXCLUDED.contract_name,
			contract_version = EXCLUDED.contract_version,
			contract_hash = EXCLUDED.contract_hash,
			rule_set = EXCLUDED.rule_set,
			priority = EXCLUDED.priority,
			activation_policy = EXCLUDED.activation_policy,
			updated_at = EXCLUDED.updated_at
	`

const bindingRepositoryGetSQL = `
		SELECT id, tenant_id, contract_name, contract_version, contract_hash, rule_set, priority, activation_policy, created_at, updated_at
		FROM contract_bindings
		WHERE id = $1 AND tenant_id = $2
	`

const bindingRepositoryListByContractSQL = `
		SELECT id, tenant_id, contract_name, contract_version, contract_hash, rule_set, priority, activation_policy, created_at, updated_at
		FROM contract_bindings
		WHERE tenant_id = $1 AND contract_name = $2 AND contract_version = $3
		ORDER BY priority DESC, created_at
	`

const bindingRepositoryListByRuleSetSQL = `
		SELECT id, tenant_id, contract_name, contract_version, contract_hash, rule_set, priority, activation_policy, created_at, updated_at
		FROM contract_bindings
		WHERE tenant_id = $1 AND rule_set = $2
		ORDER BY priority DESC, created_at
	`

const bindingRepositoryDeleteSQL = `
		DELETE FROM contract_bindings WHERE id = $1 AND tenant_id = $2
	`

func prepareBindingForSave(binding *domain.ContractBinding) {
	if binding.ID == "" {
		binding.ID = uuid.New().String()
	}
	binding.UpdatedAt = time.Now().UTC()
	if binding.CreatedAt.IsZero() {
		binding.CreatedAt = binding.UpdatedAt
	}
}

func scanBinding(row pgx.Row) (*domain.ContractBinding, error) {
	b := &domain.ContractBinding{}
	err := row.Scan(&b.ID, &b.TenantID, &b.ContractName, &b.ContractVersion, &b.ContractHash,
		&b.RuleSet, &b.Priority, &b.ActivationPolicy, &b.CreatedAt, &b.UpdatedAt)
	return b, err
}

func scanBindings(rows pgx.Rows) ([]*domain.ContractBinding, error) {
	var bindings []*domain.ContractBinding
	for rows.Next() {
		b, err := scanBinding(rows)
		if err != nil {
			return nil, fmt.Errorf("scan binding: %w", err)
		}
		bindings = append(bindings, b)
	}
	return bindings, nil
}
