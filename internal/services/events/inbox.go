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
	if err := rejectUnresolvedLegacyInbox(ctx, repos, envelope); err != nil {
		return nil, err
	}
	return s.insertInboxOrResolveConflict(ctx, repos, envelope, ruleSet)
}

func (s *Service) insertInboxOrResolveConflict(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, ruleSet string) (*domain.Execution, error) {
	inserted, err := repos.Inbox.Insert(ctx, s.newInboxEntry(envelope, ruleSet))
	if err != nil {
		return nil, fmt.Errorf("insert inbox: %w", err)
	}
	if !inserted {
		return resolveInboxConflict(ctx, repos, envelope, ruleSet)
	}
	return nil, nil
}

func findInboxExecution(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, ruleSet, inboxOperation, executionOperation string) (*domain.Execution, error) {
	entry, err := repos.Inbox.GetByRuleSet(ctx, envelope.TenantID, envelope.ID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", inboxOperation, err)
	}
	if entry == nil {
		return nil, nil
	}
	if !validCommittedInbox(entry, envelope, ruleSet) {
		return nil, domain.ErrInboxDuplicate
	}
	return loadInboxExecution(ctx, repos.Executions, entry, envelope, ruleSet, executionOperation)
}

func loadInboxExecution(ctx context.Context, repository ports.ExecutionRepository, entry *domain.InboxEntry, envelope *domain.EventEnvelope, ruleSet, operation string) (*domain.Execution, error) {
	execution, err := repository.Get(ctx, entry.ExecutionID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", operation, err)
	}
	if !matchingInboxExecution(execution, entry, envelope, ruleSet) {
		return nil, domain.ErrInboxDuplicate
	}
	return execution, nil
}

func validCommittedInbox(entry *domain.InboxEntry, envelope *domain.EventEnvelope, ruleSet string) bool {
	return entry.TenantID == envelope.TenantID && entry.EventID == envelope.ID && entry.RuleSet == ruleSet && entry.Status == domain.InboxStatusCommitted && entry.ExecutionID != ""
}

func matchingInboxExecution(execution *domain.Execution, entry *domain.InboxEntry, envelope *domain.EventEnvelope, ruleSet string) bool {
	return execution != nil && execution.ID == entry.ExecutionID && execution.TenantID == envelope.TenantID && execution.EventID == envelope.ID && execution.RuleSet == ruleSet
}

// Legacy rows without a ruleset cannot safely be attributed to a new execution.
func rejectUnresolvedLegacyInbox(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) error {
	entry, err := repos.Inbox.GetByRuleSet(ctx, envelope.TenantID, envelope.ID, "")
	if err != nil {
		return fmt.Errorf("check legacy inbox: %w", err)
	}
	if entry != nil {
		return fmt.Errorf("unresolved legacy inbox entry: %w", domain.ErrInboxDuplicate)
	}
	return nil
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
