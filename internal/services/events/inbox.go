package events

import (
	"context"
	"fmt"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// deduplicateEventForRuleset preserves inbox conflict and redelivery semantics.
func (s *Service) deduplicateEventForRuleset(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, ruleSet string) (*domain.Execution, error) {
	execution, err := findInboxExecution(ctx, repos, envelope, ruleSet, "check inbox", "get existing execution")
	if err != nil || execution != nil {
		return execution, err
	}
	inserted, err := repos.Inbox.Insert(ctx, s.newInboxEntry(envelope, ruleSet))
	if err != nil {
		return nil, fmt.Errorf("insert inbox: %w", err)
	}
	if inserted {
		return nil, nil
	}
	return resolveInboxConflict(ctx, repos, envelope, ruleSet)
}

func findInboxExecution(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, ruleSet, inboxOperation, executionOperation string) (*domain.Execution, error) {
	entry, err := repos.Inbox.Get(ctx, envelope.TenantID, envelope.ID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", inboxOperation, err)
	}
	if entry == nil || entry.RuleSet != ruleSet {
		return nil, nil
	}
	execution, err := repos.Executions.Get(ctx, entry.ExecutionID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", executionOperation, err)
	}
	return execution, nil
}

func resolveInboxConflict(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, ruleSet string) (*domain.Execution, error) {
	execution, err := findInboxExecution(ctx, repos, envelope, ruleSet, "get inbox after conflict", "get execution after conflict")
	if err != nil || execution != nil {
		return execution, err
	}
	return nil, domain.ErrInboxDuplicate
}

func (s *Service) newInboxEntry(envelope *domain.EventEnvelope, ruleSet string) *domain.InboxEntry {
	return &domain.InboxEntry{
		TenantID:     envelope.TenantID,
		EventID:      envelope.ID,
		Status:       domain.InboxStatusProcessing,
		PartitionKey: envelope.PartitionKey,
		RuleSet:      ruleSet,
		Payload:      envelope.Data,
		FirstSeenAt:  s.clock.Now(),
	}
}
