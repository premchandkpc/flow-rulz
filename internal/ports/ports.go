package ports

import (
	"context"
	"encoding/json"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// Transaction represents a database transaction
type Transaction interface {
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Context() context.Context
	FencingToken() int64
	SetFencingToken(int64)
}

// TransactionManager manages database transactions
type TransactionManager interface {
	Begin(ctx context.Context) (Transaction, error)
	WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// EventTxRepos contains only the repositories required to process one event
// atomically. Keeping this bundle narrow prevents unrelated persistence concerns
// from leaking into the event-processing use case.
type EventTxRepos struct {
	Inbox          InboxRepository
	Activations    ActivationRepository
	RuleRepo       RuleRepository
	Executions     ExecutionRepository
	Outbox         OutboxWriter
	ShardLeases    ShardLeaseRepository
	DomainServices DomainServiceRepository
	Bindings       BindingRepository
}

// RuleTxRepos contains only the repositories needed to activate a rule revision.
type RuleTxRepos struct {
	RuleRepo    RuleRepository
	Activations ActivationRepository
}

// TransactionFactory creates a new transaction.
type TransactionFactory func(ctx context.Context) (Transaction, error)

// EventTxRepositoryFactory builds event-processing repositories scoped to a transaction.
type EventTxRepositoryFactory func(Transaction) (EventTxRepos, error)

// RuleTxRepositoryFactory builds rule-activation repositories scoped to a transaction.
type RuleTxRepositoryFactory func(Transaction) (RuleTxRepos, error)

// RuleCompiler compiles raw rule source into an immutable revision.
type RuleCompiler interface {
	Compile(source json.RawMessage) (*domain.RuleRevision, error)
}

// RuleEvaluator evaluates a compiled revision against an event.
type RuleEvaluator interface {
	Evaluate(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage) (*domain.Decision, error)
}

// DomainServiceUseCase manages tenant domains and their HTTP consumers.
type DomainServiceUseCase interface {
	SaveDomain(ctx context.Context, tenantID string, item *domain.TenantDomain) (*domain.TenantDomain, error)
	GetDomain(ctx context.Context, tenantID, domainID string) (*domain.TenantDomain, error)
	ListDomains(ctx context.Context, tenantID string) ([]*domain.TenantDomain, error)
	DeleteDomain(ctx context.Context, tenantID, domainID string) error
	SaveService(ctx context.Context, tenantID string, item *domain.DomainService) (*domain.DomainService, error)
	GetService(ctx context.Context, tenantID, serviceID string) (*domain.DomainService, error)
	ListServices(ctx context.Context, tenantID string) ([]*domain.DomainService, error)
	DeleteService(ctx context.Context, tenantID, serviceID string) error
	SubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error)
}

// EventProcessor processes incoming events through the rules engine.
type EventProcessor interface {
	Process(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error)
	QuarantineEvent(ctx context.Context, sourceID, eventID, tenantID string, errClass domain.ErrorClass, errMsg string) error
}

// EffectPublisher publishes pending effects to their destinations.
type EffectPublisher interface {
	PublishBatch(ctx context.Context, batchSize int) error
}

// EffectSender sends effects to external destinations
type EffectSender interface {
	Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error
}

// Clock provides time abstraction
type Clock interface {
	Now() time.Time
}

// Logger interface for structured logging
type Logger interface {
	Printf(format string, args ...any)
}
