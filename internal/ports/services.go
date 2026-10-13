package ports

import (
	"context"
	"encoding/json"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// EventService processes incoming events through the rules engine
type EventService interface {
	Process(ctx context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error)
	QuarantineEvent(ctx context.Context, sourceID, eventID, tenantID string, errClass domain.ErrorClass, errMsg string) error
}

// RuleService manages rule deployment and activation
type RuleService interface {
	CompileAndActivate(ctx context.Context, tenantScope, ruleSet string, source []byte, actor string) (*domain.RuleActivation, error)
	GetActivation(ctx context.Context, tenantScope, ruleSet string) (*domain.RuleActivation, error)
	ListRevisions(ctx context.Context, tenantScope string) ([]*domain.RuleRevision, error)
	GetRevision(ctx context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error)
	Deactivate(ctx context.Context, tenantScope, ruleSet string) error
	ListActiveRules(ctx context.Context, tenantScope string) ([]*domain.RuleActivation, error)
}

// EffectService manages effect dispatching
type EffectService interface {
	Dispatch(ctx context.Context, effect *domain.Effect) error
	PublishBatch(ctx context.Context, batchSize int) error
	GetPending(ctx context.Context, limit int) ([]domain.OutboxEffect, error)
	GetByExecution(ctx context.Context, executionID string) ([]domain.OutboxEffect, error)
}

// WorkflowService manages long-running workflows
type WorkflowService interface {
	Get(ctx context.Context, tenantID, workflowID string) (*domain.WorkflowInstance, error)
	Create(ctx context.Context, workflow *domain.WorkflowInstance) error
	UpdateState(ctx context.Context, tenantID, workflowID, newState string, revision int64) error
	Transition(ctx context.Context, tenantID, workflowID, fromState, toState string) error
	GetByTenantAndState(ctx context.Context, tenantID, state string, limit int) ([]*domain.WorkflowInstance, error)
	GetByCorrelation(ctx context.Context, tenantID, correlationID string) ([]*domain.WorkflowInstance, error)
}

// BatchService manages batch processing
type BatchService interface {
	Tick(ctx context.Context) (int, error)
	CreateBatch(ctx context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) (*domain.BatchRun, error)
	GetBatch(ctx context.Context, tenantID, batchID string) (*domain.BatchRun, error)
	ListBatches(ctx context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error)
}

// SchedulerService manages scheduled/delayed events
type SchedulerService interface {
	Schedule(ctx context.Context, event *domain.ScheduledEvent) error
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// QuarantineService manages failed items
type QuarantineService interface {
	Save(ctx context.Context, entry *domain.QuarantineEntry) error
	Get(ctx context.Context, tenantID, id string) (*domain.QuarantineEntry, error)
	List(ctx context.Context, filter QuarantineFilter) ([]*domain.QuarantineEntry, error)
	Replay(ctx context.Context, tenantID, id string) error
	Delete(ctx context.Context, tenantID, id string) error
	ReplayAndDelete(ctx context.Context, tenantID, id string) error
}

// RuleCheckResult is the outcome of a dry-run compile and optional evaluation.
type RuleCheckResult struct {
	Valid       bool             `json:"valid"`
	Error       string           `json:"error,omitempty"`
	RuleSet     string           `json:"rule_set,omitempty"`
	Revision    int64            `json:"revision,omitempty"`
	MatchMode   string           `json:"match_mode,omitempty"`
	ContentHash string           `json:"content_hash,omitempty"`
	RuleCount   int              `json:"rule_count"`
	Decision    *domain.Decision `json:"decision,omitempty"`
	EvalError   string           `json:"evaluation_error,omitempty"`
}

// RuleChecker compiles a rule source and optionally evaluates it against sample
// event data without saving or activating anything.
type RuleChecker interface {
	CheckRule(ctx context.Context, tenantScope, eventType string, source []byte, sampleData json.RawMessage) *RuleCheckResult
}
