package events

import (
	"context"
	"fmt"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"time"
)

func (s *Service) persistExecutionAndOutboxForRuleset(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, revision *domain.RuleRevision, ruleSet string, decision *domain.Decision, routeSubscribers bool) (*domain.Execution, error) {
	now := s.clock.Now()
	execution := newExecution(envelope, revision, ruleSet, decision, now)
	if err := repos.Executions.Save(ctx, execution); err != nil {
		return nil, fmt.Errorf("save execution: %w", err)
	}
	effects, err := s.executionEffects(ctx, repos.Subscribers, envelope, execution, decision, routeSubscribers)
	if err != nil {
		return nil, err
	}
	if err := commitExecutionEffects(ctx, repos, envelope, execution.ID, effects); err != nil {
		return nil, err
	}
	return execution, nil
}

func newExecution(envelope *domain.EventEnvelope, revision *domain.RuleRevision, ruleSet string, decision *domain.Decision, now time.Time) *domain.Execution {
	return &domain.Execution{
		ID:           domain.ComputeExecutionID(envelope.TenantID, envelope.ID, ruleSet, revision.Revision),
		EventID:      envelope.ID,
		TenantID:     envelope.TenantID,
		RuleSet:      ruleSet,
		Revision:     revision.Revision,
		DecisionHash: decision.Hash,
		Status:       domain.ExecutionStatusCompleted,
		CreatedAt:    now,
	}
}

func (s *Service) executionEffects(ctx context.Context, subscribers ports.SubscriberRepository, envelope *domain.EventEnvelope, execution *domain.Execution, decision *domain.Decision, routeSubscribers bool) ([]domain.OutboxEffect, error) {
	effects := make([]domain.OutboxEffect, 0, len(decision.Effects))
	for _, effect := range decision.Effects {
		effects = append(effects, newExecutionEffect(effect, execution))
	}
	if !routeSubscribers {
		return effects, nil
	}
	routed, err := s.subscriberRouter.Route(ctx, subscribers, envelope, execution.ID, execution.CreatedAt)
	if err != nil {
		return nil, err
	}
	return append(effects, routed...), nil
}

func newExecutionEffect(effect domain.Effect, execution *domain.Execution) domain.OutboxEffect {
	return domain.OutboxEffect{
		ID: effect.ID, ExecutionID: execution.ID, TenantID: execution.TenantID,
		Destination: effect.Destination,
		Name:        effect.Name,
		Payload:     effect.Payload,
		EffectType:  effect.EffectType,
		Status:      domain.OutboxStatusPending,
		MaxAttempts: 5,
		AvailableAt: execution.CreatedAt,
		CreatedAt:   execution.CreatedAt,
		UpdatedAt:   execution.CreatedAt,
	}
}

func commitExecutionEffects(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, executionID string, effects []domain.OutboxEffect) error {
	if err := repos.Outbox.Insert(ctx, effects); err != nil {
		return fmt.Errorf("insert outbox: %w", err)
	}
	if err := repos.Inbox.MarkCommitted(ctx, envelope.TenantID, envelope.ID, executionID); err != nil {
		return fmt.Errorf("mark inbox committed: %w", err)
	}
	return nil
}
