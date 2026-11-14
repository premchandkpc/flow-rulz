// Package rules provides rule compilation.
package rules

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Service handles rule compilation, activation, and querying.
type Service struct {
	compiler    ports.RuleCompiler
	ruleRepo    ports.RuleRepository
	activations ports.ActivationRepository
	executions  ports.ExecutionRepository
	clock       ports.Clock
	txManager   ports.TransactionManager
	newTxRepos  ports.RuleTxRepositoryFactory
	contracts   ports.ContractRegistry
}

// NewService performs its documented repository operation.
func NewService(compiler ports.RuleCompiler, ruleRepo ports.RuleRepository, activations ports.ActivationRepository, executions ports.ExecutionRepository, clock ports.Clock, txManager ports.TransactionManager, newTxRepos ports.RuleTxRepositoryFactory) *Service {
	return &Service{
		compiler:    compiler,
		ruleRepo:    ruleRepo,
		activations: activations,
		executions:  executions,
		clock:       clock,
		txManager:   txManager,
		newTxRepos:  newTxRepos,
	}
}

// WithContractRegistry enables contract pinning during rule activation.
func (s *Service) WithContractRegistry(registry ports.ContractRegistry) *Service {
	s.contracts = registry
	return s
}

// Activate compiles and activates a rule revision atomically with CAS.
// The tenantScope should be derived from authenticated user context.
func (s *Service) Activate(ctx context.Context, tenantScope string, ruleSet string, source json.RawMessage, actor string) (*domain.RuleActivation, error) {
	revision, err := s.compiler.Compile(source)
	if err != nil {
		return nil, err
	}
	revision.RuleID = ruleSet
	revision.TenantScope = tenantScope
	if err := s.pinInputContract(ctx, revision); err != nil {
		return nil, err
	}
	return s.activateWithCAS(ctx, tenantScope, ruleSet, revision, actor)
}

// CompileAndActivate implements ports.RuleService.
func (s *Service) CompileAndActivate(ctx context.Context, tenantScope, ruleSet string, source []byte, actor string) (*domain.RuleActivation, error) {
	return s.Activate(ctx, tenantScope, ruleSet, json.RawMessage(source), actor)
}

// activateWithCAS performs the activation in a transaction with optimistic concurrency control.
func (s *Service) activateWithCAS(ctx context.Context, tenantScope, ruleSet string, revision *domain.RuleRevision, actor string) (*domain.RuleActivation, error) {
	tx, err := s.txManager.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	txRepos, err := s.newTxRepos(tx)
	if err != nil {
		return nil, fmt.Errorf("create tx repos: %w", err)
	}
	if txCtx := tx.Context(); txCtx != nil {
		ctx = txCtx //nolint:contextcheck // Transaction operations use the transaction's context.
	}
	return s.persistActivation(ctx, tx, txRepos, tenantScope, ruleSet, revision, actor)
}

// GetExecution retrieves an execution by ID.
func (s *Service) GetExecution(ctx context.Context, executionID string) (*domain.Execution, error) {
	return s.executions.Get(ctx, executionID)
}

// ListRevisions lists all rule revisions for a tenant scope.
func (s *Service) ListRevisions(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error) {
	return s.ruleRepo.List(ctx, tenantScope)
}

// GetRevision retrieves a specific rule revision.
func (s *Service) GetRevision(ctx context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error) {
	return s.ruleRepo.Get(ctx, tenantScope, ruleSet, revision)
}

// Deactivate deactivates a rule set by removing its activation.
func (s *Service) Deactivate(ctx context.Context, tenantScope, ruleSet string) error {
	// Get current activation to verify it exists
	_, err := s.activations.Get(ctx, tenantScope, ruleSet)
	if err != nil {
		return fmt.Errorf("get activation: %w", err)
	}
	// In this design, deactivation means setting activation to nil/zero revision
	// The activation record is deleted
	return s.activations.Delete(ctx, tenantScope, ruleSet)
}

// GetActivation retrieves the current activation for a rule set.
func (s *Service) GetActivation(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleActivation, error) {
	return s.activations.Get(ctx, tenantScope, ruleSet)
}

// ListActiveRules returns persisted active rule pointers for a tenant scope.
func (s *Service) ListActiveRules(ctx context.Context, tenantScope string) ([]*domain.RuleActivation, error) {
	return s.activations.List(ctx, tenantScope)
}

func (s *Service) pinInputContract(ctx context.Context, revision *domain.RuleRevision) error {
	if revision.InputContract == nil || s.contracts == nil {
		return nil
	}
	ref := revision.InputContract
	contract, err := s.contracts.Get(ctx, ref.Name, ref.Version)
	if err != nil {
		return fmt.Errorf("resolve input contract: %w", err)
	}
	if contract == nil {
		return fmt.Errorf("input contract %s@%s not found", ref.Name, ref.Version)
	}
	return pinContractHash(ref, contract)
}

func pinContractHash(ref *domain.ContractRef, contract *domain.ContractSchema) error {
	if contract.Deprecated {
		return fmt.Errorf("input contract %s@%s is deprecated", contract.Name, contract.Version)
	}
	if err := contract.NormalizeAndValidate(); err != nil {
		return fmt.Errorf("invalid input contract: %w", err)
	}
	if ref.SchemaHash != "" && ref.SchemaHash != contract.SchemaHash {
		return fmt.Errorf("input contract schema hash mismatch for %s@%s", contract.Name, contract.Version)
	}
	ref.SchemaHash = contract.SchemaHash
	return nil
}

func (s *Service) persistActivation(ctx context.Context, tx ports.Transaction, repos ports.RuleTxRepos, tenant, ruleSet string, revision *domain.RuleRevision, actor string) (*domain.RuleActivation, error) {
	if err := repos.RuleRepo.Save(ctx, revision); err != nil {
		return nil, fmt.Errorf("save revision: %w", err)
	}
	current, err := repos.Activations.Get(ctx, tenant, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get activation: %w", err)
	}
	activation := s.nextActivation(tenant, ruleSet, revision.Revision, actor, current)
	if err := commitActivation(ctx, tx, repos.Activations, activation); err != nil {
		return nil, err
	}
	return activation, nil
}

func (s *Service) nextActivation(tenant, ruleSet string, revision int64, actor string, current *domain.RuleActivation) *domain.RuleActivation {
	version := int64(1)
	if current != nil {
		version += current.Version
	}
	return &domain.RuleActivation{
		TenantScope: tenant, RuleSet: ruleSet, Revision: revision,
		Version: version, Actor: actor, ActivatedAt: s.clock.Now(),
	}
}

func commitActivation(ctx context.Context, tx ports.Transaction, repository ports.ActivationRepository, activation *domain.RuleActivation) error {
	if err := repository.SetWithVersion(ctx, activation, activation.Version-1); err != nil {
		return fmt.Errorf("set activation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}
