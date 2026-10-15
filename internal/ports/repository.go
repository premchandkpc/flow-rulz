package ports

import (
	"context"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// Repository interfaces

// EventRepository handles event storage and retrieval
type EventRepository interface {
	Save(ctx context.Context, event *domain.EventEnvelope) error
	Get(ctx context.Context, tenantID, eventID string) (*domain.EventEnvelope, error)
	Exists(ctx context.Context, tenantID, eventID string) (bool, error)
	MarkProcessed(ctx context.Context, tenantID, eventID, executionID string) error
	GetUnprocessed(ctx context.Context, tenantID string, limit int) ([]*domain.EventEnvelope, error)
}

// InboxRepository handles idempotency via inbox pattern
type InboxRepository interface {
	Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error)
	Get(ctx context.Context, tenantID, eventID string) (*domain.InboxEntry, error)
	GetByRuleSet(ctx context.Context, tenantID, eventID, ruleSet string) (*domain.InboxEntry, error)
	MarkCommitted(ctx context.Context, tenantID, eventID, executionID string) error
}

// ExecutionRepository handles execution records
type ExecutionRepository interface {
	Save(ctx context.Context, execution *domain.Execution) error
	Get(ctx context.Context, executionID string) (*domain.Execution, error)
	UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error
}

// OutboxWriter persists outgoing effects. Event processing receives this narrow
// interface so inserts can share the same transaction as inbox and execution writes.
type OutboxWriter interface {
	Insert(ctx context.Context, effects []domain.OutboxEffect) error
}

// OutboxDispatcher owns claiming and durable settlement of effects.
type OutboxDispatcher interface {
	ClaimPending(ctx context.Context, batchSize int, claimant string, claimTTL time.Duration) ([]domain.OutboxEffect, error)
	MarkDelivered(ctx context.Context, effectIDs []string, claimant string) error
	ScheduleRetry(ctx context.Context, effectID, claimant string, delay time.Duration, attempts int, errMsg string) error
	Quarantine(ctx context.Context, effectID, claimant, errMsg string) error
}

// OutboxReader exposes read-only operational queries.
type OutboxReader interface {
	GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error)
	GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error)
}

// OutboxDeliveryStore is the runtime publisher's minimal persistence boundary.
type OutboxDeliveryStore interface {
	OutboxDispatcher
	OutboxReader
}

// OutboxRepository is the full adapter contract retained for storage wiring and
// compatibility. Application services should depend on the narrowest interface.
type OutboxRepository interface {
	OutboxWriter
	OutboxDeliveryStore
}

// RuleRepository handles rule storage
type RuleRepository interface {
	Save(ctx context.Context, revision *domain.RuleRevision) error
	GetActive(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleRevision, error)
	Get(ctx context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error)
	List(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error)
	Delete(ctx context.Context, tenantScope, ruleSet string, revision int64) error
}

// ActivationRepository handles active rule pointers
type ActivationRepository interface {
	Get(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleActivation, error)
	Set(ctx context.Context, activation *domain.RuleActivation) error
	// SetWithVersion atomically sets the activation only if the current version matches expectedVersion.
	// Returns error if version mismatch (optimistic concurrency conflict).
	SetWithVersion(ctx context.Context, activation *domain.RuleActivation, expectedVersion int64) error
	Delete(ctx context.Context, tenantScope, ruleSet string) error
	List(ctx context.Context, tenantScope string) ([]*domain.RuleActivation, error)
}

// WorkflowRepository handles workflow instances
type WorkflowRepository interface {
	Save(ctx context.Context, workflow *domain.WorkflowInstance) error
	Get(ctx context.Context, tenantID, workflowID string) (*domain.WorkflowInstance, error)
	Update(ctx context.Context, workflow *domain.WorkflowInstance) error
	GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error)
	GetByCorrelation(ctx context.Context, tenantID, correlationID string) ([]*domain.WorkflowInstance, error)
}

// WorkflowDefinitionRepository handles workflow definitions (templates)
type WorkflowDefinitionRepository interface {
	Save(ctx context.Context, def *domain.WorkflowDefinition) error
	Get(ctx context.Context, workflowType string, version int64) (*domain.WorkflowDefinition, error)
	List(ctx context.Context) ([]*domain.WorkflowDefinition, error)
	Delete(ctx context.Context, workflowType string, version int64) error
}

// ScheduledEventRepository handles scheduled/delayed events. Public read and
// cancel operations are tenant-scoped; scheduler settlement is fenced by the
// unique claim token returned to the claimant's call path.
type ScheduledEventRepository interface {
	Save(ctx context.Context, event *domain.ScheduledEvent) error
	GetDue(ctx context.Context, before time.Time, limit int) ([]*domain.ScheduledEvent, error)
	Claim(ctx context.Context, eventIDs []string, claimant string, leaseTTL time.Duration) ([]*domain.ScheduledEvent, error)
	MarkReleased(ctx context.Context, eventID, claimant string, releasedAt time.Time) error
	ScheduleRetry(ctx context.Context, eventID, claimant string, retryAt time.Time, errMsg string) error
	MarkFailed(ctx context.Context, eventID, claimant, errMsg string) error
	Get(ctx context.Context, tenantID, eventID string) (*domain.ScheduledEvent, error)
	List(ctx context.Context, tenantID string, limit, offset int) ([]*domain.ScheduledEvent, error)
	Cancel(ctx context.Context, tenantID, eventID string) error
}

// BatchRepository handles batch processing
type BatchRepository interface {
	SaveRun(ctx context.Context, run *domain.BatchRun) error
	// CompleteBatch atomically persists the completed run and associates every member.
	// It must reject missing members and members already assigned to another batch.
	CompleteBatch(ctx context.Context, run *domain.BatchRun, eventIDs []string) error
	GetRun(ctx context.Context, batchID string) (*domain.BatchRun, error)
	GetRunForTenant(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error)
	ListRuns(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error)
	ListUnbatched(ctx context.Context, limit int) ([]*domain.InboxEntry, error)
	ListUnbatchedByIDs(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) ([]*domain.InboxEntry, error)
	MarkBatched(ctx context.Context, tenantID, eventID, batchID string) error
}

// ShardLeaseRepository handles shard ownership
type ShardLeaseRepository interface {
	Acquire(ctx context.Context, shard uint32, owner string, ttl time.Duration) (*domain.ShardLease, error)
	Renew(ctx context.Context, shard uint32, owner string, fencingToken int64, ttl time.Duration) (*domain.ShardLease, error)
	Release(ctx context.Context, shard uint32, owner string) error
	GetOwner(ctx context.Context, shard uint32) (*domain.ShardLease, error)
	ValidateFencingToken(ctx context.Context, shard uint32, owner string, fencingToken int64) error
	// AssertHeld validates the lease is held by the owner with the given fencing token.
	// Uses row lock (FOR SHARE) to prevent stale holders from proceeding. Fails closed.
	AssertHeld(ctx context.Context, shard uint32, owner string, fencingToken int64) error
}

// QuarantineRepository handles failed items
type QuarantineRepository interface {
	Save(ctx context.Context, entry *domain.QuarantineEntry) error
	Get(ctx context.Context, tenantID, id string) (*domain.QuarantineEntry, error)
	List(ctx context.Context, filter QuarantineFilter) ([]*domain.QuarantineEntry, error)
	Replay(ctx context.Context, tenantID, id string) error
	Delete(ctx context.Context, tenantID, id string) error
}

// QuarantineFilter for filtering quarantine entries
type QuarantineFilter struct {
	TenantID   string
	SourceType string
	ErrorClass string
	From       time.Time
	To         time.Time
	Limit      int
	Offset     int
}

// ContractRegistry handles schema contracts
type ContractRegistry interface {
	Register(ctx context.Context, schema *domain.ContractSchema) error
	Get(ctx context.Context, name, version string) (*domain.ContractSchema, error)
	List(ctx context.Context, name string) ([]*domain.ContractSchema, error)
	Delete(ctx context.Context, name, version string) error
}

// BindingRepository manages contract-to-ruleset bindings for explicit routing.
type BindingRepository interface {
	Save(ctx context.Context, binding *domain.ContractBinding) error
	Get(ctx context.Context, tenantID, bindingID string) (*domain.ContractBinding, error)
	ListByContract(ctx context.Context, tenantID, contractName, contractVersion string) ([]*domain.ContractBinding, error)
	ListByRuleSet(ctx context.Context, tenantID, ruleSet string) ([]*domain.ContractBinding, error)
	Delete(ctx context.Context, tenantID, bindingID string) error
}

// SubscriberRepository resolves active subscribers for a tenant event type. It is intentionally narrower than the domain-management repository.
type SubscriberRepository interface {
	ListSubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error)
}

// DomainServiceRepository manages tenant domains, callback services, and matching subscriptions.
type DomainServiceRepository interface {
	SaveDomain(ctx context.Context, item *domain.TenantDomain) error
	GetDomain(ctx context.Context, tenantID, domainID string) (*domain.TenantDomain, error)
	ListDomains(ctx context.Context, tenantID string) ([]*domain.TenantDomain, error)
	DeleteDomain(ctx context.Context, tenantID, domainID string) error
	SaveService(ctx context.Context, item *domain.DomainService) error
	GetService(ctx context.Context, tenantID, serviceID string) (*domain.DomainService, error)
	ListServices(ctx context.Context, tenantID string) ([]*domain.DomainService, error)
	DeleteService(ctx context.Context, tenantID, serviceID string) error
	ListSubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error)
}
