// Package events provides event processing services.
package events

import (
	"context"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Service handles event processing, inbox dedup, rule evaluation, and execution creation.
type Service struct {
	compiler         ports.RuleCompiler
	evaluator        ports.RuleEvaluator
	quarantine       ports.QuarantineRepository
	clock            ports.Clock
	beginTx          ports.TransactionManager
	newRepos         ports.EventTxRepositoryFactory
	contracts        ports.ContractRegistry
	subscriberRouter SubscriberRouter
	revisions        *immutableRevisionCache
}

// NewService constructs the event-processing service.
func NewService(compiler ports.RuleCompiler, evaluator ports.RuleEvaluator, quarantine ports.QuarantineRepository, clock ports.Clock, beginTx ports.TransactionManager, newRepos ports.EventTxRepositoryFactory) *Service {
	return &Service{
		compiler: compiler, evaluator: evaluator, quarantine: quarantine,
		clock: clock, beginTx: beginTx, newRepos: newRepos,
		revisions: newImmutableRevisionCache(4096),
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
	if err := s.validateEventContract(ctx, envelope); err != nil {
		return nil, err
	}
	// F1: Reject invalid fencing token (<= 0) for all shards including 0
	if fencingToken <= 0 {
		return nil, domain.ErrFencingTokenMismatch
	}
	return s.processInTransaction(ctx, envelope, fencingToken, shard, workerID)
}

// validateEventContract enforces the declared event contract when the envelope
// carries one; envelopes without a contract pass unchanged.
func (s *Service) validateEventContract(ctx context.Context, envelope *domain.EventEnvelope) error {
	if envelope.Contract == nil {
		return nil
	}
	schema, err := s.resolveEventContractSchema(ctx, envelope)
	if err != nil {
		return err
	}
	return validateResolvedContract(schema, envelope)
}

func validateResolvedContract(schema *domain.ContractSchema, envelope *domain.EventEnvelope) error {
	if schema.Deprecated {
		return fmt.Errorf("event contract %s@%s is deprecated", schema.Name, schema.Version)
	}
	if err := schema.NormalizeAndValidate(); err != nil {
		return fmt.Errorf("invalid event contract: %w", err)
	}
	return validateEventPayload(schema, envelope)
}

// resolveEventContractSchema resolves the declared contract from the registry
// and rejects a missing registry, a failed lookup or an absent schema.
func (s *Service) resolveEventContractSchema(ctx context.Context, envelope *domain.EventEnvelope) (*domain.ContractSchema, error) {
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
	return schema, nil
}

// validateEventPayload enforces the schema hash, event-kind and payload
// conformance of the envelope against the resolved contract schema.
func validateEventPayload(schema *domain.ContractSchema, envelope *domain.EventEnvelope) error {
	if envelope.Contract.SchemaHash == "" || envelope.Contract.SchemaHash != schema.SchemaHash {
		return fmt.Errorf("event contract schema hash mismatch for %s@%s", schema.Name, schema.Version)
	}
	if schema.Kind != domain.ContractKindEvent {
		return fmt.Errorf("contract %s@%s is not an event contract", schema.Name, schema.Version)
	}
	if err := schema.ValidatePayload(envelope.Data); err != nil {
		return fmt.Errorf("event payload violates contract %s@%s: %w", schema.Name, schema.Version, err)
	}
	return nil
}

// QuarantineEvent records a failed event for later inspection.
func (s *Service) QuarantineEvent(ctx context.Context, sourceID string, eventID string, tenantID string, errClass domain.ErrorClass, errMsg string) error {
	return s.quarantine.Save(ctx, &domain.QuarantineEntry{
		ID:         eventQuarantineID(tenantID, sourceID, eventID, errClass),
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

func eventQuarantineID(tenantID, sourceID, eventID string, errClass domain.ErrorClass) string {
	rawID := fmt.Sprintf("event-quarantine|%d:%s|%d:%s|%d:%s|%s",
		len(tenantID), tenantID, len(sourceID), sourceID, len(eventID), eventID, errClass)
	return domain.ComputeSourceHash([]byte(rawID))
}
