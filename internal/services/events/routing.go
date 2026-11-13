package events

import (
	"context"
	"fmt"
	"sort"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type rulesetRevision struct {
	RuleSet  string
	Revision *domain.RuleRevision
	Priority int
}

// resolveRulesets prefers contract bindings and falls back to event-type routing.
func (s *Service) resolveRulesets(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) ([]rulesetRevision, error) {
	if envelope.Contract != nil {
		revisions, err := s.resolveBoundRulesets(ctx, repos, envelope)
		if err != nil || len(revisions) > 0 {
			return revisions, err
		}
	}
	return s.resolveLegacyRuleset(ctx, repos, envelope)
}

func (s *Service) resolveBoundRulesets(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) ([]rulesetRevision, error) {
	bindings, err := repos.Bindings.ListByContract(ctx, domain.DomainConfigurationScope, envelope.Contract.Name, envelope.Contract.Version)
	if err != nil {
		return nil, fmt.Errorf("list bindings: %w", err)
	}
	return s.loadBoundRevisions(ctx, repos, envelope, bindings)
}

func (s *Service) loadBoundRevisions(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, bindings []*domain.ContractBinding) ([]rulesetRevision, error) {
	var result []rulesetRevision
	for _, binding := range bindings {
		revision, err := s.resolveBinding(ctx, repos, envelope, binding)
		if err != nil {
			return nil, err
		}
		if revision != nil {
			result = append(result, rulesetRevision{RuleSet: binding.RuleSet, Revision: revision, Priority: binding.Priority})
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Priority > result[j].Priority })
	return result, nil
}

func (s *Service) resolveBinding(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope, binding *domain.ContractBinding) (*domain.RuleRevision, error) {
	if binding.ContractHash != envelope.Contract.SchemaHash {
		return nil, nil
	}
	activation, err := repos.Activations.Get(ctx, domain.DomainConfigurationScope, binding.RuleSet)
	if err != nil {
		return nil, fmt.Errorf("get activation for ruleset %s: %w", binding.RuleSet, err)
	}
	if activation == nil {
		return nil, nil
	}
	return s.loadBindingRevision(ctx, repos.RuleRepo, envelope, binding.RuleSet, activation.Revision)
}

func (s *Service) loadBindingRevision(ctx context.Context, repository ports.RuleRepository, envelope *domain.EventEnvelope, ruleSet string, number int64) (*domain.RuleRevision, error) {
	revision, err := s.getRevision(ctx, repository, domain.DomainConfigurationScope, ruleSet, number)
	if err != nil {
		return nil, fmt.Errorf("get active revision for ruleset %s: %w", ruleSet, err)
	}
	if revision == nil || !matchesInputContract(revision.InputContract, envelope.Contract) {
		return nil, nil
	}
	return revision, nil
}

func (s *Service) resolveLegacyRuleset(ctx context.Context, repos ports.EventTxRepos, envelope *domain.EventEnvelope) ([]rulesetRevision, error) {
	activation, err := repos.Activations.Get(ctx, domain.DomainConfigurationScope, envelope.Type)
	if err != nil {
		return nil, fmt.Errorf("get activation: %w", err)
	}
	if activation == nil {
		return nil, domain.ErrRuleNotReady
	}
	return s.loadLegacyRevision(ctx, repos.RuleRepo, envelope, activation.Revision)
}

func (s *Service) loadLegacyRevision(ctx context.Context, repository ports.RuleRepository, envelope *domain.EventEnvelope, number int64) ([]rulesetRevision, error) {
	revision, err := s.getRevision(ctx, repository, domain.DomainConfigurationScope, envelope.Type, number)
	if err != nil {
		return nil, fmt.Errorf("get active revision: %w", err)
	}
	if revision == nil {
		return nil, domain.ErrRuleNotFound
	}
	if err := validateInputContract(revision, envelope.Contract); err != nil {
		return nil, err
	}
	return []rulesetRevision{{RuleSet: envelope.Type, Revision: revision}}, nil
}

func matchesInputContract(required, actual *domain.ContractRef) bool {
	return required == nil || (actual != nil && required.Name == actual.Name && required.Version == actual.Version && required.SchemaHash == actual.SchemaHash)
}

func validateInputContract(revision *domain.RuleRevision, actual *domain.ContractRef) error {
	required := revision.InputContract
	if matchesInputContract(required, actual) {
		return nil
	}
	if actual == nil {
		return fmt.Errorf("%w: rule revision %s requires contract %s@%s (hash: %s)", domain.ErrContractRequired, revision.RuleID, required.Name, required.Version, required.SchemaHash)
	}
	return fmt.Errorf("%w: event contract %s@%s (hash: %s) does not match rule revision %s required contract %s@%s (hash: %s)",
		domain.ErrContractMismatch, actual.Name, actual.Version, actual.SchemaHash,
		revision.RuleID, required.Name, required.Version, required.SchemaHash)
}
