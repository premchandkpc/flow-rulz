package events

import (
	"context"
	"fmt"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// processInTransaction owns the transaction lifecycle for all matching rulesets.
func (s *Service) processInTransaction(ctx context.Context, envelope *domain.EventEnvelope, token int64, shard uint32, workerID string) (*domain.Execution, error) {
	tx, err := s.beginFencedTransaction(ctx, token)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	repos, err := s.newRepos(tx)
	if err != nil {
		return nil, fmt.Errorf("create transaction repositories: %w", err)
	}
	if txCtx := tx.Context(); txCtx != nil {
		ctx = txCtx //nolint:contextcheck // Transaction operations use the transaction's context.
	}
	return s.processTransaction(ctx, tx, repos, envelope, shard, workerID, token)
}

func (s *Service) beginFencedTransaction(ctx context.Context, token int64) (ports.Transaction, error) {
	tx, err := s.beginTx.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	tx.SetFencingToken(token)
	return tx, nil
}

func (s *Service) processTransaction(ctx context.Context, tx ports.Transaction, repos ports.EventTxRepos, envelope *domain.EventEnvelope, shard uint32, workerID string, token int64) (*domain.Execution, error) {
	result, err := s.processFencedEvent(ctx, repos, envelope, shard, workerID, token)
	if err != nil || !result.processed {
		return result.first, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit tx: %w", err)
	}
	return result.first, nil
}

func (s *Service) processFencedEvent(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, shard uint32, workerID string, token int64) (processingResult, error) {
	if err := repos.ShardLeases.AssertHeld(ctx, shard, workerID, token); err != nil {
		return processingResult{}, fmt.Errorf("fencing token validation failed: %w", err)
	}
	rulesets, err := s.resolveRulesets(ctx, repos, envelope)
	if err != nil {
		return processingResult{}, err
	}
	if len(rulesets) == 0 {
		return processingResult{}, domain.ErrRuleNotReady
	}
	return s.processRulesets(ctx, repos, envelope, rulesets)
}

type processingResult struct {
	first     *domain.Execution
	processed bool
}

func (s *Service) processRulesets(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, rulesets []rulesetRevision) (processingResult, error) {
	var result processingResult
	for _, revision := range rulesets {
		execution, processed, err := s.processRuleset(ctx, repos, envelope, revision, !result.processed)
		if err != nil {
			return processingResult{}, err
		}
		if result.first == nil {
			result.first = execution
		}
		result.processed = result.processed || processed
	}
	return result, nil
}

func (s *Service) processRuleset(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, revision rulesetRevision, routeSubscribers bool) (*domain.Execution, bool, error) {
	existing, err := s.deduplicateEventForRuleset(ctx, repos, envelope, revision.RuleSet)
	if err != nil || existing != nil {
		return existing, false, err
	}
	decision, err := s.evaluator.Evaluate(revision.Revision, envelope, nil)
	if err != nil {
		return nil, false, fmt.Errorf("evaluate ruleset %s: %w", revision.RuleSet, err)
	}
	execution, err := s.persistExecutionAndOutboxForRuleset(ctx, repos, envelope, revision.Revision, revision.RuleSet, decision, routeSubscribers)
	return execution, err == nil, err
}
