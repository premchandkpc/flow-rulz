// Package events provides event processing services.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Service handles event processing, inbox dedup, rule evaluation, and execution creation.
type Service struct {
	compiler   ports.RuleCompiler
	evaluator  ports.RuleEvaluator
	quarantine ports.QuarantineRepository
	clock      ports.Clock
	beginTx    ports.TransactionManager
	newRepos   ports.EventTxRepositoryFactory
	contracts  ports.ContractRegistry
}

// NewService constructs the event-processing service.
func NewService(
	compiler ports.RuleCompiler,
	evaluator ports.RuleEvaluator,
	quarantine ports.QuarantineRepository,
	clock ports.Clock,
	beginTx ports.TransactionManager,
	newRepos ports.EventTxRepositoryFactory,
) *Service {
	return &Service{
		compiler:   compiler,
		evaluator:  evaluator,
		quarantine: quarantine,
		clock:      clock,
		beginTx:    beginTx,
		newRepos:   newRepos,
	}
}

// WithContractRegistry enables runtime payload validation for versioned events.
func (s *Service) WithContractRegistry(registry ports.ContractRegistry) *Service {
	s.contracts = registry
	return s
}

// Process processes an incoming event through the rules engine.
func (s *Service) Process(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error) {
	if err := envelope.Validate(); err != nil {
		return nil, err
	}
	if envelope.Contract != nil {
		if s.contracts == nil {
			return nil, fmt.Errorf("event declares contract %s@%s but contract registry is not configured", envelope.Contract.Name, envelope.Contract.Version)
		}
		schema, err := s.contracts.Get(ctx, envelope.Contract.Name, envelope.Contract.Version)
		if err != nil {
			return nil, fmt.Errorf("resolve event contract: %w", err)
		}
		if schema == nil {
			return nil, fmt.Errorf("event contract %s@%s not found", envelope.Contract.Name, envelope.Contract.Version)
		}
		if schema.Deprecated {
			return nil, fmt.Errorf("event contract %s@%s is deprecated", schema.Name, schema.Version)
		}
		if err := schema.NormalizeAndValidate(); err != nil {
			return nil, fmt.Errorf("invalid event contract: %w", err)
		}
		if envelope.Contract.SchemaHash == "" || envelope.Contract.SchemaHash != schema.SchemaHash {
			return nil, fmt.Errorf("event contract schema hash mismatch for %s@%s", schema.Name, schema.Version)
		}
		if schema.Kind != domain.ContractKindEvent {
			return nil, fmt.Errorf("contract %s@%s is not an event contract", schema.Name, schema.Version)
		}
		if err := schema.ValidatePayload(envelope.Data); err != nil {
			return nil, fmt.Errorf("event payload violates contract %s@%s: %w", schema.Name, schema.Version, err)
		}
	}

	// F1: Reject invalid fencing token (<= 0) for all shards including 0
	if fencingToken <= 0 {
		return nil, domain.ErrFencingTokenMismatch
	}

	return s.processInTransaction(ctx, envelope, fencingToken, shard, workerID)
}

// resolveRulesets determines which rulesets should process this event.
// Returns a list of (ruleset, revision) pairs to process.
func (s *Service) resolveRulesets(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) ([]rulesetRevision, error) {
	// If event has a contract, use bindings-based routing
	if envelope.Contract != nil {
		bindings, err := repos.Bindings.ListByContract(ctx, envelope.TenantID, envelope.Contract.Name, envelope.Contract.Version)
		if err != nil {
			return nil, fmt.Errorf("list bindings: %w", err)
		}

		var result []rulesetRevision
		for _, binding := range bindings {
			// Verify contract hash matches
			if binding.ContractHash != envelope.Contract.SchemaHash {
				continue // Hash mismatch, skip this binding
			}

			// Check if ruleset has an active revision
			activation, err := repos.Activations.Get(ctx, envelope.TenantID, binding.RuleSet)
			if err != nil {
				return nil, fmt.Errorf("get activation for ruleset %s: %w", binding.RuleSet, err)
			}
			if activation == nil {
				continue // No activation for this ruleset yet
			}

			revision, err := repos.RuleRepo.GetActive(ctx, envelope.TenantID, binding.RuleSet)
			if err != nil {
				return nil, fmt.Errorf("get active revision for ruleset %s: %w", binding.RuleSet, err)
			}
			if revision == nil {
				continue // No active revision
			}

			// Enforce contract binding for this revision
			if revision.InputContract != nil {
				if envelope.Contract.Name != revision.InputContract.Name ||
					envelope.Contract.Version != revision.InputContract.Version ||
					envelope.Contract.SchemaHash != revision.InputContract.SchemaHash {
					continue // Contract mismatch, skip
				}
			}

			result = append(result, rulesetRevision{
				RuleSet:  binding.RuleSet,
				Revision: revision,
				Priority: binding.Priority,
			})
		}

		if len(result) > 0 {
			// Sort by priority (highest first)
			sort.Slice(result, func(i, j int) bool {
				return result[i].Priority > result[j].Priority
			})
			return result, nil
		}
		// No bindings matched - fall through to legacy routing
	}

	// Legacy routing: event type directly maps to ruleset
	ruleSet := envelope.Type
	activation, err := repos.Activations.Get(ctx, envelope.TenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get activation: %w", err)
	}
	if activation == nil {
		return nil, domain.ErrRuleNotReady
	}

	revision, err := repos.RuleRepo.GetActive(ctx, envelope.TenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get active revision: %w", err)
	}
	if revision == nil {
		return nil, domain.ErrRuleNotFound
	}

	// Enforce contract binding
	if revision.InputContract != nil {
		if envelope.Contract == nil {
			return nil, fmt.Errorf("%w: rule revision %s requires contract %s@%s (hash: %s)",
				domain.ErrContractRequired, revision.RuleID, revision.InputContract.Name, revision.InputContract.Version, revision.InputContract.SchemaHash)
		}
		if envelope.Contract.Name != revision.InputContract.Name ||
			envelope.Contract.Version != revision.InputContract.Version ||
			envelope.Contract.SchemaHash != revision.InputContract.SchemaHash {
			return nil, fmt.Errorf("%w: event contract %s@%s (hash: %s) does not match rule revision %s required contract %s@%s (hash: %s)",
				domain.ErrContractMismatch,
				envelope.Contract.Name, envelope.Contract.Version, envelope.Contract.SchemaHash,
				revision.RuleID,
				revision.InputContract.Name, revision.InputContract.Version, revision.InputContract.SchemaHash)
		}
	}

	return []rulesetRevision{{RuleSet: ruleSet, Revision: revision, Priority: 0}}, nil
}

type rulesetRevision struct {
	RuleSet  string
	Revision *domain.RuleRevision
	Priority int
}

// processInTransaction executes the event processing within a transaction.
// Supports fan-out to multiple rulesets via contract bindings.
func (s *Service) processInTransaction(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error) {
	tx, err := s.beginTx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// F1: Set fencing token for validation
	tx.SetFencingToken(fencingToken)

	repos, err := s.newRepos(tx)
	if err != nil {
		return nil, fmt.Errorf("create transaction repositories: %w", err)
	}
	if txCtx := tx.Context(); txCtx != nil {
		ctx = txCtx //nolint:contextcheck // tx context is intentionally used for transaction-scoped operations
	}

	// F3: Validate fencing token FIRST with row lock (AssertHeld)
	if err := repos.ShardLeases.AssertHeld(ctx, shard, workerID, fencingToken); err != nil {
		return nil, fmt.Errorf("fencing token validation failed: %w", err)
	}

	// Resolve all rulesets that should process this event
	rulesets, err := s.resolveRulesets(ctx, repos, envelope)
	if err != nil {
		return nil, err
	}
	if len(rulesets) == 0 {
		return nil, domain.ErrRuleNotReady
	}

	var firstExecution *domain.Execution
	processedAny := false

	// Process each ruleset
	for _, rr := range rulesets {
		// Deduplicate per ruleset (inbox is keyed by tenantID, eventID, RuleSet)
		existingExec, err := s.deduplicateEventForRuleset(ctx, repos, envelope, rr.RuleSet)
		if err != nil {
			return nil, err
		}
		if existingExec != nil {
			if firstExecution == nil {
				firstExecution = existingExec
			}
			continue // Already processed for this ruleset
		}

		// Evaluate rules
		decision, err := s.evaluator.Evaluate(rr.Revision, envelope, nil)
		if err != nil {
			return nil, fmt.Errorf("evaluate ruleset %s: %w", rr.RuleSet, err)
		}

		// Persist execution and outbox effects
		execution, err := s.persistExecutionAndOutboxForRuleset(ctx, repos, envelope, rr.Revision, rr.RuleSet, decision)
		if err != nil {
			return nil, err
		}

		if firstExecution == nil {
			firstExecution = execution
		}
		processedAny = true
	}

	// If all rulesets were duplicates, rollback (don't commit empty transaction)
	if !processedAny {
		return firstExecution, nil
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}

	return firstExecution, nil
}

// deduplicateEvent checks inbox for duplicate and inserts new entry.
// Returns existing execution if duplicate, nil if new event.
func (s *Service) deduplicateEvent(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) (*domain.Execution, error) {
	entry, err := repos.Inbox.Get(ctx, envelope.TenantID, envelope.ID)
	if err != nil {
		return nil, fmt.Errorf("check inbox: %w", err)
	}
	if entry != nil {
		exec, err := repos.Executions.Get(ctx, entry.ExecutionID)
		if err != nil {
			return nil, fmt.Errorf("get existing execution: %w", err)
		}
		if exec != nil {
			return exec, nil
		}
	}

	inboxEntry := &domain.InboxEntry{
		TenantID:     envelope.TenantID,
		EventID:      envelope.ID,
		Status:       domain.InboxStatusProcessing,
		PartitionKey: envelope.PartitionKey,
		RuleSet:      envelope.Type,
		Payload:      envelope.Data,
		FirstSeenAt:  s.clock.Now(),
	}
	inserted, err := repos.Inbox.Insert(ctx, inboxEntry)
	if err != nil {
		return nil, fmt.Errorf("insert inbox: %w", err)
	}
	// F2: Dedup fall-through - if not inserted and no execution found, it's a conflict
	if !inserted {
		entry, err = repos.Inbox.Get(ctx, envelope.TenantID, envelope.ID)
		if err != nil {
			return nil, fmt.Errorf("get inbox after conflict: %w", err)
		}
		if entry != nil {
			exec, err := repos.Executions.Get(ctx, entry.ExecutionID)
			if err != nil {
				return nil, fmt.Errorf("get execution after conflict: %w", err)
			}
			if exec != nil {
				return exec, nil
			}
		}
		// F2: Fall-through with nil execution = conflict
		return nil, domain.ErrInboxDuplicate
	}
	return nil, nil
}

// deduplicateEventForRuleset checks inbox for duplicate for a specific ruleset.
// Returns existing execution if duplicate, nil if new event for this ruleset.
func (s *Service) deduplicateEventForRuleset(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, ruleSet string) (*domain.Execution, error) {
	entry, err := repos.Inbox.Get(ctx, envelope.TenantID, envelope.ID)
	if err != nil {
		return nil, fmt.Errorf("check inbox: %w", err)
	}
	if entry != nil && entry.RuleSet == ruleSet {
		exec, err := repos.Executions.Get(ctx, entry.ExecutionID)
		if err != nil {
			return nil, fmt.Errorf("get existing execution: %w", err)
		}
		if exec != nil {
			return exec, nil
		}
	}

	inboxEntry := &domain.InboxEntry{
		TenantID:     envelope.TenantID,
		EventID:      envelope.ID,
		Status:       domain.InboxStatusProcessing,
		PartitionKey: envelope.PartitionKey,
		RuleSet:      ruleSet,
		Payload:      envelope.Data,
		FirstSeenAt:  s.clock.Now(),
	}
	inserted, err := repos.Inbox.Insert(ctx, inboxEntry)
	if err != nil {
		return nil, fmt.Errorf("insert inbox: %w", err)
	}
	// F2: Dedup fall-through - if not inserted and no execution found, it's a conflict
	if !inserted {
		entry, err = repos.Inbox.Get(ctx, envelope.TenantID, envelope.ID)
		if err != nil {
			return nil, fmt.Errorf("get inbox after conflict: %w", err)
		}
		if entry != nil && entry.RuleSet == ruleSet {
			exec, err := repos.Executions.Get(ctx, entry.ExecutionID)
			if err != nil {
				return nil, fmt.Errorf("get execution after conflict: %w", err)
			}
			if exec != nil {
				return exec, nil
			}
		}
		// F2: Fall-through with nil execution = conflict
		return nil, domain.ErrInboxDuplicate
	}
	return nil, nil
}

// persistExecutionAndOutboxForRuleset creates execution record and outbox effects for a specific ruleset.
func (s *Service) persistExecutionAndOutboxForRuleset(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, revision *domain.RuleRevision, ruleSet string, decision *domain.Decision) (*domain.Execution, error) {
	executionID := domain.ComputeExecutionID(envelope.TenantID, envelope.ID, ruleSet, revision.Revision)
	now := s.clock.Now()

	execution := &domain.Execution{
		ID:           executionID,
		EventID:      envelope.ID,
		TenantID:     envelope.TenantID,
		RuleSet:      ruleSet,
		Revision:     revision.Revision,
		DecisionHash: decision.Hash,
		Status:       domain.ExecutionStatusCompleted,
		CreatedAt:    now,
	}

	if err := repos.Executions.Save(ctx, execution); err != nil {
		return nil, fmt.Errorf("save execution: %w", err)
	}

	outboxEffects := make([]domain.OutboxEffect, 0, len(decision.Effects))
	for _, ef := range decision.Effects {
		outboxEffects = append(outboxEffects, domain.OutboxEffect{
			ID:          ef.ID,
			ExecutionID: executionID,
			TenantID:    envelope.TenantID,
			Destination: ef.Destination,
			Name:        ef.Name,
			Payload:     ef.Payload,
			EffectType:  ef.EffectType,
			Status:      domain.OutboxStatusPending,
			Attempts:    0,
			MaxAttempts: 5,
			AvailableAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	}
	if repos.DomainServices == nil {
		return nil, fmt.Errorf("tenant domain service repository is not configured")
	}
	subscribers, err := repos.DomainServices.ListSubscribersForEvent(ctx, envelope.TenantID, envelope.Type)
	if err != nil {
		return nil, fmt.Errorf("get tenant domain subscribers: %w", err)
	}
	if len(subscribers) > 100 {
		return nil, fmt.Errorf("event %q exceeds the 100-service fan-out limit", envelope.Type)
	}
	eventPayload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal subscribed event: %w", err)
	}
	for _, subscriber := range subscribers {
		outboxEffects = append(outboxEffects, domain.OutboxEffect{
			ID:          domain.ComputeEffectID(envelope.TenantID, envelope.ID, "service-subscription", 1, subscriber.ID, 0),
			ExecutionID: executionID,
			TenantID:    envelope.TenantID,
			Destination: subscriber.ID,
			Name:        envelope.Type,
			Payload:     eventPayload,
			EffectType:  domain.EffectTypeServiceEvent,
			Status:      domain.OutboxStatusPending,
			MaxAttempts: 5,
			AvailableAt: now,
			CreatedAt:   now,
			UpdatedAt:   now,
		})
	}

	if err := repos.Outbox.Insert(ctx, outboxEffects); err != nil {
		return nil, fmt.Errorf("insert outbox: %w", err)
	}

	if err := repos.Inbox.MarkCommitted(ctx, envelope.TenantID, envelope.ID, executionID); err != nil {
		return nil, fmt.Errorf("mark inbox committed: %w", err)
	}

	return execution, nil
}

// loadActiveRevision loads the active rule revision for the event (legacy single-ruleset path).
// Deprecated: Use resolveRulesets for bindings-based routing.
func (s *Service) loadActiveRevision(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) (*domain.RuleRevision, error) {
	ruleSet := envelope.Type
	activation, err := repos.Activations.Get(ctx, envelope.TenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get activation: %w", err)
	}
	if activation == nil {
		// Rule not yet activated for this event type. This is transient:
		// the rule may be deployed shortly after the event arrives.
		// NATS MaxDeliver + backoff bounds the retry window.
		return nil, domain.ErrRuleNotReady
	}

	revision, err := repos.RuleRepo.GetActive(ctx, envelope.TenantID, ruleSet)
	if err != nil {
		return nil, fmt.Errorf("get active revision: %w", err)
	}
	if revision == nil {
		return nil, domain.ErrRuleNotFound
	}

	// Enforce contract binding: if rule revision declares an InputContract,
	// the event MUST declare a matching contract.
	if revision.InputContract != nil {
		if envelope.Contract == nil {
			return nil, fmt.Errorf("%w: rule revision %s requires contract %s@%s (hash: %s)",
				domain.ErrContractRequired, revision.RuleID, revision.InputContract.Name, revision.InputContract.Version, revision.InputContract.SchemaHash)
		}
		if envelope.Contract.Name != revision.InputContract.Name ||
			envelope.Contract.Version != revision.InputContract.Version ||
			envelope.Contract.SchemaHash != revision.InputContract.SchemaHash {
			return nil, fmt.Errorf("%w: event contract %s@%s (hash: %s) does not match rule revision %s required contract %s@%s (hash: %s)",
				domain.ErrContractMismatch,
				envelope.Contract.Name, envelope.Contract.Version, envelope.Contract.SchemaHash,
				revision.RuleID,
				revision.InputContract.Name, revision.InputContract.Version, revision.InputContract.SchemaHash)
		}
	}
	// If revision doesn't require a contract, event may or may not have one (backward compatible)

	return revision, nil
}

// QuarantineEvent records a failed event for later inspection.
func (s *Service) QuarantineEvent(ctx context.Context, sourceID string, eventID string, tenantID string, errClass domain.ErrorClass, errMsg string) error {
	rawID := fmt.Sprintf("event-quarantine|%d:%s|%d:%s|%d:%s|%s",
		len(tenantID), tenantID, len(sourceID), sourceID, len(eventID), eventID, errClass)
	quarantineID := domain.ComputeSourceHash([]byte(rawID))
	return s.quarantine.Save(ctx, &domain.QuarantineEntry{
		ID:         quarantineID,
		SourceType: "event",
		SourceID:   sourceID,
		EventID:    eventID,
		TenantID:   tenantID,
		ErrorClass: string(errClass),
		PayloadRef: "jetstream:flowrulz",
		Error:      errMsg,
		CreatedAt:  s.clock.Now(),
	})
}
