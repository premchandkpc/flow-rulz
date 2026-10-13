// Package workflow provides workflow instance management use cases.
package workflow

import (
	"context"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Service manages workflow instances.
type Service struct {
	repo  ports.WorkflowRepository
	clock ports.Clock
}

// NewService creates a new workflow service.
func NewService(repo ports.WorkflowRepository, clock ports.Clock) *Service {
	return &Service{
		repo:  repo,
		clock: clock,
	}
}

// Create persists a new workflow instance.
func (s *Service) Create(ctx context.Context, workflow *domain.WorkflowInstance) error {
	if workflow.WorkflowID == "" {
		workflow.WorkflowID = domain.NewID()
	}
	if workflow.CreatedAt.IsZero() {
		workflow.CreatedAt = s.clock.Now()
	}
	workflow.UpdatedAt = s.clock.Now()
	workflow.Version = 1
	return s.repo.Save(ctx, workflow)
}

// Get retrieves a workflow instance by tenant and workflow ID.
func (s *Service) Get(ctx context.Context, tenantID, workflowID string) (*domain.WorkflowInstance, error) {
	if tenantID == "" {
		return nil, domain.ErrWorkflowNotFound
	}
	return s.repo.Get(ctx, tenantID, workflowID)
}

// UpdateState updates a workflow's state with optimistic concurrency control.
func (s *Service) UpdateState(ctx context.Context, tenantID, workflowID, newState string, revision int64) error {
	return s.transition(ctx, tenantID, workflowID, "", newState, &revision)
}

// Transition performs a workflow state transition.
func (s *Service) Transition(ctx context.Context, tenantID, workflowID, fromState, toState string) error {
	return s.transition(ctx, tenantID, workflowID, fromState, toState, nil)
}

// transition is the single state-transition path for both API forms. It scopes
// the read by tenant, validates the expected state and domain transition, then
// persists using the repository's optimistic version check.
func (s *Service) transition(ctx context.Context, tenantID, workflowID, expectedFrom, toState string, revision *int64) error {
	if tenantID == "" {
		return domain.ErrWorkflowNotFound
	}

	workflow, err := s.repo.Get(ctx, tenantID, workflowID)
	if err != nil {
		return fmt.Errorf("get workflow: %w", err)
	}
	if workflow == nil {
		return domain.ErrWorkflowNotFound
	}
	if expectedFrom != "" && workflow.State != expectedFrom {
		return fmt.Errorf("workflow in state %s, expected %s", workflow.State, expectedFrom)
	}
	if !workflow.CanTransition(workflow.State, toState) {
		return domain.ErrWorkflowInvalidTransition
	}
	if err := workflow.Transition(toState); err != nil {
		return err
	}
	if revision != nil {
		workflow.CurrentRevision = *revision
	}

	now := s.clock.Now()
	workflow.UpdatedAt = now
	if toState == domain.WorkflowStatusCompleted || toState == domain.WorkflowStatusFailed {
		workflow.CompletedAt = &now
	}
	return s.repo.Update(ctx, workflow)
}

// GetByTenantAndState retrieves workflows by tenant and state.
func (s *Service) GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error) {
	if limit <= 0 {
		limit = 100
	}
	return s.repo.GetByTenantAndState(ctx, tenantID, state, limit)
}

// GetByCorrelation retrieves workflows by tenant and correlation ID.
func (s *Service) GetByCorrelation(ctx context.Context, tenantID, correlationID string) ([]*domain.WorkflowInstance, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	return s.repo.GetByCorrelation(ctx, tenantID, correlationID)
}
