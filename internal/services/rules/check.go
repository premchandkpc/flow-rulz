package rules

import (
	"context"
	"encoding/json"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	rulesengine "github.com/flowrulz/flow-rulz/internal/rules"
)

var _ ports.RuleChecker = (*Service)(nil)

// CheckRule compiles source and, when sampleData is present, evaluates it against a
// synthetic event. Nothing is persisted or activated.
func (s *Service) CheckRule(_ context.Context, tenantScope, eventType string, source []byte, sampleData json.RawMessage) *ports.RuleCheckResult {
	revision, err := s.compiler.Compile(source)
	if err != nil {
		return &ports.RuleCheckResult{Error: err.Error()}
	}
	result := &ports.RuleCheckResult{
		Valid:       true,
		Revision:    revision.Revision,
		MatchMode:   string(revision.MatchMode),
		ContentHash: revision.ContentHash,
		RuleCount:   len(revision.Compiled),
	}
	var meta struct {
		RuleSet string `json:"rule_set"`
	}
	if json.Unmarshal(source, &meta) == nil {
		result.RuleSet = meta.RuleSet
	}
	if len(sampleData) == 0 {
		return result
	}
	if eventType == "" {
		eventType = result.RuleSet
	}
	event := &domain.EventEnvelope{
		ID:           "check-event",
		Type:         eventType,
		TenantID:     tenantScope,
		PartitionKey: "check",
		OccurredAt:   s.clock.Now(),
		Data:         sampleData,
	}
	decision, err := rulesengine.NewEvaluator(s.clock).Evaluate(revision, event, nil)
	if err != nil {
		result.EvalError = err.Error()
		return result
	}
	result.Decision = decision
	return result
}
