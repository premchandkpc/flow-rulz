package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// SystemClock represents the system clock.
type SystemClock struct{}

// Now provides the now.
func (SystemClock) Now() time.Time { return time.Now().UTC() }

// EventEnvelope represents the event envelope.
type EventEnvelope struct {
	ID            string            `json:"id"`
	Type          string            `json:"type"`
	TenantID      string            `json:"tenant_id"`
	PartitionKey  string            `json:"partition_key"`
	WorkflowID    string            `json:"workflow_id,omitempty"`
	OccurredAt    time.Time         `json:"occurred_at"`
	Data          json.RawMessage   `json:"data"`
	Contract      *ContractRef      `json:"contract,omitempty"`
	MessageKind   string            `json:"message_kind,omitempty"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	CausationID   string            `json:"causation_id,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
}

// VirtualShard provides the virtual shard.
func (e *EventEnvelope) VirtualShard(modulus uint32) uint32 {
	key := e.TenantID + ":" + e.PartitionKey
	return ShardHash(key) % modulus
}

// ShardHash returns the stable 32-bit FNV-1a hash used for shard routing.
func ShardHash(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}

// Validate provides the validate.
func (e *EventEnvelope) Validate() error {
	if e.ID == "" {
		return ErrEventIDRequired
	}
	if e.Type == "" {
		return ErrEventTypeRequired
	}
	if e.TenantID == "" {
		return ErrTenantIDRequired
	}
	if e.PartitionKey == "" {
		return ErrPartitionKeyRequired
	}
	if e.OccurredAt.IsZero() {
		return ErrOccurredAtRequired
	}
	return nil
}

// MatchMode represents the match mode.
type MatchMode string

const (
	// MatchModeFirstMatch identifies the match mode first match value.
	MatchModeFirstMatch MatchMode = "first_match"
	// MatchModeAllMatches identifies the match mode all matches value.
	MatchModeAllMatches MatchMode = "all_matches"
)

// RuleRevision represents the rule revision.
type RuleRevision struct {
	TenantScope     string          `json:"tenant_scope"`
	RuleID          string          `json:"rule_id"`
	Revision        int64           `json:"revision"`
	ContentHash     string          `json:"content_hash"`
	MatchMode       MatchMode       `json:"match_mode"`
	Compiled        []CompiledRule  `json:"compiled"`
	Source          json.RawMessage `json:"source"`
	CompilerVersion string          `json:"compiler_version"`
	InputContract   *ContractRef    `json:"input_contract,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
}

// ContractRef represents the contract ref.
type ContractRef struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	SchemaHash string `json:"schema_hash,omitempty"`
}

// ContractSchema represents the contract schema.
type ContractSchema struct {
	Name            string                   `json:"name"`
	Version         string                   `json:"version"`
	Namespace       string                   `json:"namespace,omitempty"`
	Description     string                   `json:"description,omitempty"`
	Fields          map[string]ContractField `json:"fields"`
	Kind            ContractKind             `json:"kind,omitempty"`
	Parent          *ContractRef             `json:"parent,omitempty"`
	AllowedChildren []ContractRef            `json:"allowed_children,omitempty"`
	SchemaHash      string                   `json:"schema_hash,omitempty"`
	Compatibility   string                   `json:"compatibility,omitempty"`
	Owner           string                   `json:"owner,omitempty"`
	Deprecated      bool                     `json:"deprecated,omitempty"`
	CreatedAt       time.Time                `json:"created_at,omitempty"`
	UpdatedAt       time.Time                `json:"updated_at,omitempty"`
}

// TenantDomain is persisted domain configuration. Runtime event identity and routing keys are not configuration.
type TenantDomain struct {
	ID          string              `json:"id"`
	TenantID    string              `json:"-"`
	Name        string              `json:"name"`
	EventTypes  []string            `json:"-"`
	Deployments []ServiceDeployment `json:"services"`
	CreatedAt   time.Time           `json:"created_at"`
	UpdatedAt   time.Time           `json:"updated_at"`
}

// ServiceDeployment declares the ruleset revisions assigned to one service worker pool.
type ServiceDeployment struct {
	ServiceID     string              `json:"service_id"`
	Name          string              `json:"name"`
	Revision      int64               `json:"revision"`
	Active        bool                `json:"active"`
	Rulesets      []RulesetAssignment `json:"rulesets"`
	WorkerProfile WorkerProfile       `json:"worker_profile,omitempty"`
}

// RulesetAssignment pins a deployment to one immutable ruleset revision.
type RulesetAssignment struct {
	RulesetID string       `json:"ruleset_id"`
	Revision  int64        `json:"revision"`
}

// WorkerProfile contains service-level worker scaling and concurrency hints.
type WorkerProfile struct {
	MinReplicas int `json:"min_replicas,omitempty"`
	MaxReplicas int `json:"max_replicas,omitempty"`
	Concurrency int `json:"concurrency,omitempty"`
}

// DomainService is a generic event subscription. It is separate from execution Services nested under a Domain.
// nolint:revive // type name stutters with package name but is a domain concept
// DomainService represents the domain service.
type DomainService struct {
	ID          string    `json:"id"`
	TenantID    string    `json:"-"`
	Name        string    `json:"name"`
	NATSSubject string    `json:"nats_subject"`
	EndpointURL string    `json:"-"` // Deprecated legacy field; never used for delivery.
	EventTypes  []string  `json:"event_types"`
	DomainIDs   []string  `json:"-"` // Legacy internal domain subscriptions; new subscriptions use event_types.
	Active      bool      `json:"active"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// ServiceSubject returns a stable, tenant-isolated NATS subject for a service.
// Hashing identifiers keeps subjects valid and prevents tenant/service names from
// leaking into broker monitoring while ensuring each subscriber gets its own copy.
func ServiceSubject(tenantID, serviceID string) string {
	tenantHash := sha256.Sum256([]byte(tenantID))
	serviceHash := sha256.Sum256([]byte(serviceID))
	return "services." + hex.EncodeToString(tenantHash[:12]) + "." + hex.EncodeToString(serviceHash[:12])
}

// ContractField represents the contract field.
type ContractField struct {
	Type        string                   `json:"type"`
	Description string                   `json:"description,omitempty"`
	Required    bool                     `json:"required,omitempty"`
	Default     interface{}              `json:"default,omitempty"`
	Enum        []interface{}            `json:"enum,omitempty"`
	Format      string                   `json:"format,omitempty"`
	Items       *ContractField           `json:"items,omitempty"`
	Properties  map[string]ContractField `json:"properties,omitempty"`
}

// CompiledRule represents the compiled rule.
type CompiledRule struct {
	ID          string    `json:"id"`
	Name        string    `json:"name,omitempty"`
	Description string    `json:"description,omitempty"`
	Tags        []string  `json:"tags,omitempty"`
	Priority    int       `json:"priority"`
	When        Predicate `json:"when"`
	Then        []Action  `json:"then"`
	Otherwise   []Action  `json:"otherwise,omitempty"`
}

// Predicate represents the predicate.
type Predicate struct {
	All   []*Predicate `json:"all,omitempty"`
	Any   []*Predicate `json:"any,omitempty"`
	Not   *Predicate   `json:"not,omitempty"`
	Path  string       `json:"path,omitempty"`
	Op    Operator     `json:"op,omitempty"`
	Value interface{}  `json:"value,omitempty"`
}

// Operator represents the operator.
type Operator string

const (
	// OpEq identifies the op eq value.
	OpEq Operator = "eq"
	// OpNeq identifies the op neq value.
	OpNeq Operator = "neq"
	// OpGt identifies the op gt value.
	OpGt Operator = "gt"
	// OpGte identifies the op gte value.
	OpGte Operator = "gte"
	// OpLt identifies the op lt value.
	OpLt Operator = "lt"
	// OpLte identifies the op lte value.
	OpLte Operator = "lte"
	// OpIn identifies the op in value.
	OpIn Operator = "in"
	// OpNotIn identifies the op not_in value.
	OpNotIn Operator = "not_in"
	// OpExists identifies the op exists value.
	OpExists Operator = "exists"
	// OpNotExists identifies the op not_exists value.
	OpNotExists Operator = "not_exists"
)

// ValidOp provides the valid op.
func ValidOp(op Operator) bool {
	switch op {
	case OpEq, OpNeq, OpGt, OpGte, OpLt, OpLte, OpIn, OpNotIn, OpExists, OpNotExists:
		return true
	default:
		return false
	}
}

// Action represents the action.
type Action struct {
	Emit          *EmitAction          `json:"emit,omitempty"`
	EmitEvent     *EmitEventAction     `json:"emit_event,omitempty"`
	ScheduleEvent *ScheduleEventAction `json:"schedule_event,omitempty"`
	Command       *CommandAction       `json:"command,omitempty"`
}

// EmitAction represents the emit action.
type EmitAction struct {
	Topic string          `json:"topic"`
	Data  json.RawMessage `json:"data"`
}

// PartitionKeyPolicy defines how to derive the partition key for child events.
type PartitionKeyPolicy string

const (
	// PartitionKeyPolicyExplicit identifies the partition key policy explicit value.
	PartitionKeyPolicyExplicit PartitionKeyPolicy = "explicit" // Use literal PartitionKey value
	// PartitionKeyPolicyInherit inherits the partition key from the parent event.
	PartitionKeyPolicyInherit PartitionKeyPolicy = "inherit" // Inherit from parent event
	// PartitionKeyPolicyFromData derives the partition key from JSON path in parent data.
	PartitionKeyPolicyFromData PartitionKeyPolicy = "from_data" // Derive from JSON path in parent data
)

// EmitEventAction creates a new FlowRulz event after the parent execution
// commits. Supports explicit, inherit, and from_data partition key policies.
// EmitEventAction represents the emit event action.
type EmitEventAction struct {
	Type               string             `json:"type"`
	PartitionKey       string             `json:"partition_key,omitempty"`
	PartitionKeyPolicy PartitionKeyPolicy `json:"partition_key_policy,omitempty"`
	PartitionKeyPath   string             `json:"partition_key_path,omitempty"` // JSON path for from_data policy
	Data               json.RawMessage    `json:"data"`
}

// MaxScheduledDelaySeconds bounds a rule's scheduled-event delay to one year.
const MaxScheduledDelaySeconds int64 = 31_536_000

// ScheduleEventAction durably schedules a child event after the configured delay.
type ScheduleEventAction struct {
	Type               string             `json:"type"`
	DelaySeconds       int64              `json:"delay_seconds"`
	PartitionKey       string             `json:"partition_key,omitempty"`
	PartitionKeyPolicy PartitionKeyPolicy `json:"partition_key_policy,omitempty"`
	PartitionKeyPath   string             `json:"partition_key_path,omitempty"`
	Data               json.RawMessage    `json:"data"`
}

// CommandAction represents the command action.
type CommandAction struct {
	Destination string          `json:"destination"`
	Name        string          `json:"name"`
	Data        json.RawMessage `json:"data"`
}

// Effect represents the effect.
type Effect struct {
	ID             string          `json:"id"`
	ExecutionID    string          `json:"execution_id"`
	TenantID       string          `json:"tenant_id,omitempty"`
	Destination    string          `json:"destination"`
	Name           string          `json:"name"`
	Payload        json.RawMessage `json:"payload"`
	EffectType     EffectType      `json:"effect_type"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
}

// EffectType represents the effect type.
type EffectType string

const (
	// EffectTypeEmit identifies the effect type emit value.
	EffectTypeEmit EffectType = "emit"
	// EffectTypeEmitEvent identifies the effect type emit_event value.
	EffectTypeEmitEvent EffectType = "emit_event"
	// EffectTypeScheduleEvent identifies a durable scheduled-event intent.
	EffectTypeScheduleEvent EffectType = "schedule_event"
	// EffectTypeCommand identifies the effect type command value.
	EffectTypeCommand EffectType = "command"
	// EffectTypeServiceEvent identifies the effect type service_event value.
	EffectTypeServiceEvent EffectType = "service_event"
)

// Decision represents the decision.
type Decision struct {
	MatchedRules []string          `json:"matched_rules"`
	Effects      []Effect          `json:"effects"`
	Hash         string            `json:"hash"`
	Explanations []RuleExplanation `json:"explanations,omitempty"`
}

// RuleExplanation represents the rule explanation.
type RuleExplanation struct {
	RuleID   string   `json:"rule_id"`
	RuleName string   `json:"rule_name,omitempty"`
	Matched  bool     `json:"matched"`
	Priority int      `json:"priority"`
	Reason   string   `json:"reason"`
	Actions  []string `json:"actions,omitempty"`
}

// Execution represents the execution.
type Execution struct {
	ID           string          `json:"id"`
	EventID      string          `json:"event_id"`
	TenantID     string          `json:"tenant_id"`
	RuleSet      string          `json:"rule_set"`
	Revision     int64           `json:"revision"`
	DecisionHash string          `json:"decision_hash"`
	Status       ExecutionStatus `json:"status"`
	Error        string          `json:"error,omitempty"`
	TraceID      string          `json:"trace_id,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
}

// ExecutionStatus represents the execution status.
type ExecutionStatus string

const (
	// ExecutionStatusPending identifies the execution status pending value.
	ExecutionStatusPending ExecutionStatus = "pending"
	// ExecutionStatusCompleted identifies the execution status completed value.
	ExecutionStatusCompleted ExecutionStatus = "completed"
	// ExecutionStatusFailed identifies the execution status failed value.
	ExecutionStatusFailed ExecutionStatus = "failed"
	// ExecutionStatusQuarantined identifies the execution status quarantined value.
	ExecutionStatusQuarantined ExecutionStatus = "quarantined"
)

// Complete transitions execution to completed state. Only valid from pending.
func (e *Execution) Complete(now time.Time) error {
	if e.Status != ExecutionStatusPending {
		return fmt.Errorf("cannot complete execution in state %s", e.Status)
	}
	e.Status = ExecutionStatusCompleted
	e.CompletedAt = &now
	return nil
}

// Fail transitions execution to failed state. Only valid from pending.
func (e *Execution) Fail(now time.Time, errMsg string) error {
	if e.Status != ExecutionStatusPending {
		return fmt.Errorf("cannot fail execution in state %s", e.Status)
	}
	e.Status = ExecutionStatusFailed
	e.Error = errMsg
	e.CompletedAt = &now
	return nil
}

// Quarantine transitions execution to quarantined state. Only valid from pending.
func (e *Execution) Quarantine(now time.Time, errMsg string) error {
	if e.Status != ExecutionStatusPending {
		return fmt.Errorf("cannot quarantine execution in state %s", e.Status)
	}
	e.Status = ExecutionStatusQuarantined
	e.Error = errMsg
	e.CompletedAt = &now
	return nil
}

// OutboxEffect represents the outbox effect.
type OutboxEffect struct {
	ID             string          `json:"id"`
	ExecutionID    string          `json:"execution_id"`
	TenantID       string          `json:"tenant_id"`
	Destination    string          `json:"destination"`
	Name           string          `json:"name"`
	Payload        json.RawMessage `json:"payload"`
	EffectType     EffectType      `json:"effect_type"`
	Status         OutboxStatus    `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	AvailableAt    time.Time       `json:"available_at"`
	ClaimedBy      string          `json:"claimed_by,omitempty"`
	ClaimedAt      *time.Time      `json:"claimed_at,omitempty"`
	ClaimExpiresAt *time.Time      `json:"claim_expires_at,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// OutboxStatus represents the outbox status.
type OutboxStatus string

const (
	// OutboxStatusPending identifies the outbox status pending value.
	OutboxStatusPending OutboxStatus = "pending"
	// OutboxStatusClaimed identifies the outbox status claimed value.
	OutboxStatusClaimed OutboxStatus = "claimed"
	// OutboxStatusDelivered identifies the outbox status delivered value.
	OutboxStatusDelivered OutboxStatus = "delivered"
	// OutboxStatusFailed identifies the outbox status failed value.
	OutboxStatusFailed OutboxStatus = "failed"
	// OutboxStatusQuarantined identifies the outbox status quarantined value.
	OutboxStatusQuarantined OutboxStatus = "quarantined"
)

// Claim transitions effect from pending to claimed. Only valid from pending.
func (e *OutboxEffect) Claim(_ string, now time.Time) error {
	if e.Status != OutboxStatusPending {
		return fmt.Errorf("cannot claim effect in state %s", e.Status)
	}
	e.Status = OutboxStatusClaimed
	e.UpdatedAt = now
	return nil
}

// Deliver transitions effect to delivered. Only valid from claimed.
func (e *OutboxEffect) Deliver(now time.Time) error {
	if e.Status != OutboxStatusClaimed {
		return fmt.Errorf("cannot deliver effect in state %s", e.Status)
	}
	e.Status = OutboxStatusDelivered
	e.UpdatedAt = now
	return nil
}

// Retry transitions effect back to pending with updated attempts.
func (e *OutboxEffect) Retry(now time.Time, delay time.Duration, attempts int, errMsg string) error {
	if e.Status != OutboxStatusClaimed {
		return fmt.Errorf("cannot retry effect in state %s", e.Status)
	}
	e.Status = OutboxStatusPending
	e.Attempts = attempts
	e.AvailableAt = now.Add(delay)
	e.LastError = errMsg
	e.UpdatedAt = now
	return nil
}

// Quarantine transitions effect to quarantined. Only valid from claimed.
func (e *OutboxEffect) Quarantine(now time.Time, errMsg string) error {
	if e.Status != OutboxStatusClaimed {
		return fmt.Errorf("cannot quarantine effect in state %s", e.Status)
	}
	e.Status = OutboxStatusQuarantined
	e.LastError = errMsg
	e.UpdatedAt = now
	return nil
}

// InboxEntry represents the inbox entry.
type InboxEntry struct {
	TenantID     string          `json:"tenant_id"`
	EventID      string          `json:"event_id"`
	Status       InboxStatus     `json:"status"`
	ExecutionID  string          `json:"execution_id,omitempty"`
	PartitionKey string          `json:"partition_key,omitempty"`
	RuleSet      string          `json:"rule_set,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	BatchID      string          `json:"batch_id,omitempty"`
	FirstSeenAt  time.Time       `json:"first_seen_at"`
	CommittedAt  *time.Time      `json:"committed_at,omitempty"`
}

// InboxStatus represents the inbox status.
type InboxStatus string

const (
	// InboxStatusProcessing identifies the inbox status processing value.
	InboxStatusProcessing InboxStatus = "processing"
	// InboxStatusCommitted identifies the inbox status committed value.
	InboxStatusCommitted InboxStatus = "committed"
)

// Commit transitions inbox entry to committed state. Only valid from processing.
func (e *InboxEntry) Commit(executionID string, now time.Time) error {
	if e.Status != InboxStatusProcessing {
		return fmt.Errorf("cannot commit inbox entry in state %s", e.Status)
	}
	e.Status = InboxStatusCommitted
	e.ExecutionID = executionID
	e.CommittedAt = &now
	return nil
}

// IsDuplicate returns true if this event was already processed.
func (e *InboxEntry) IsDuplicate() bool {
	return e.Status == InboxStatusCommitted && e.ExecutionID != ""
}

// ShardLease represents the shard lease.
type ShardLease struct {
	VirtualShard uint32    `json:"virtual_shard"`
	Owner        string    `json:"owner"`
	FencingToken int64     `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	RoutingEpoch int64     `json:"routing_epoch"`
}

// QuarantineEntry represents the quarantine entry.
type QuarantineEntry struct {
	ID         string    `json:"id"`
	SourceType string    `json:"source_type"`
	SourceID   string    `json:"source_id"`
	ErrorClass string    `json:"error_class"`
	PayloadRef string    `json:"payload_ref"`
	EventID    string    `json:"event_id,omitempty"`
	TenantID   string    `json:"tenant_id,omitempty"`
	Error      string    `json:"error,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// RuleActivation represents the rule activation.
type RuleActivation struct {
	TenantScope string    `json:"tenant_scope"`
	RuleSet     string    `json:"rule_set"`
	Revision    int64     `json:"revision"`
	Version     int64     `json:"version"`
	Actor       string    `json:"actor"`
	ActivatedAt time.Time `json:"activated_at"`
}

// NewID provides the new id.
func NewID() string { return uuid.New().String() }

// ComputeExecutionID returns a stable identity for an event evaluated against a rule revision.
// Re-evaluating the same event under a different revision intentionally yields a different ID.
// ComputeExecutionID provides the compute execution id.
func ComputeExecutionID(tenantID, eventID, ruleSet string, revision int64) string {
	raw := fmt.Sprintf("execution|%d:%s|%d:%s|%d:%s|%d", len(tenantID), tenantID, len(eventID), eventID, len(ruleSet), ruleSet, revision)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)
}

// ComputeEffectID provides the compute effect id.
func ComputeEffectID(tenantID, eventID, ruleSet string, revision int64, ruleID string, actionIndex int) string {
	raw := fmt.Sprintf("%s|%s|%s|%d|%s|%d", tenantID, eventID, ruleSet, revision, ruleID, actionIndex)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)
}

// ComputeDecisionHash provides the compute decision hash.
func ComputeDecisionHash(revisionHash, eventID, factsHash string, matched []string, effects []Effect) string {
	for _, id := range matched {
		revisionHash += id
	}
	for _, ef := range effects {
		revisionHash += ef.ID
	}
	h := sha256.Sum256([]byte(revisionHash + eventID + factsHash))
	return fmt.Sprintf("%x", h)
}

// ComputeSourceHash provides the compute source hash.
func ComputeSourceHash(source json.RawMessage) string {
	h := sha256.Sum256(source)
	return fmt.Sprintf("%x", h)
}

// ComputeChildEventID returns a deterministic child event ID scoped to its tenant.
// Length-prefixing prevents delimiter ambiguity and tenant inclusion prevents cross-tenant collisions.
func ComputeChildEventID(tenantID, parentEventID, ruleID string, actionIndex int) string {
	raw := fmt.Sprintf("child|%d:%s|%d:%s|%d:%s|%d",
		len(tenantID), tenantID, len(parentEventID), parentEventID, len(ruleID), ruleID, actionIndex)
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)
}

// ScheduledEvent represents the scheduled event.
type ScheduledEvent struct {
	EventID      string               `json:"event_id"`
	TenantID     string               `json:"tenant_id"`
	EventType    string               `json:"event_type"`
	PartitionKey string               `json:"partition_key"`
	WorkflowID   string               `json:"workflow_id,omitempty"`
	Payload      json.RawMessage      `json:"payload"`
	Headers      map[string]string    `json:"headers,omitempty"`
	ScheduledAt  time.Time            `json:"scheduled_at"`
	Status       ScheduledEventStatus `json:"status"`
	RetryAt      *time.Time           `json:"retry_at,omitempty"`
	RetryCount   int                  `json:"retry_count,omitempty"`
	LastError    string               `json:"last_error,omitempty"`
	CreatedAt    time.Time            `json:"created_at"`
	ReleasedAt   *time.Time           `json:"released_at,omitempty"`
}

// WorkflowInstance represents the workflow instance.
type WorkflowInstance struct {
	WorkflowID      string          `json:"workflow_id"`
	TenantID        string          `json:"tenant_id"`
	WorkflowType    string          `json:"workflow_type"`
	State           string          `json:"state"`
	Version         int64           `json:"version"`
	CurrentRevision int64           `json:"current_revision,omitempty"`
	Context         json.RawMessage `json:"context,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	CompletedAt     *time.Time      `json:"completed_at,omitempty"`
}

const (
	// WorkflowStatusPending identifies the workflow status pending value.
	WorkflowStatusPending = "pending"
	// WorkflowStatusRunning identifies the workflow status running value.
	WorkflowStatusRunning = "running"
	// WorkflowStatusCompleted identifies the workflow status completed value.
	WorkflowStatusCompleted = "completed"
	// WorkflowStatusFailed identifies the workflow status failed value.
	WorkflowStatusFailed = "failed"
	// WorkflowStatusPaused identifies the workflow status paused value.
	WorkflowStatusPaused = "paused"
)

// CanTransition provides the can transition.
func (w *WorkflowInstance) CanTransition(from, to string) bool {
	// Basic state machine - can be extended
	validTransitions := map[string][]string{
		WorkflowStatusPending:   {WorkflowStatusRunning, WorkflowStatusFailed},
		WorkflowStatusRunning:   {WorkflowStatusCompleted, WorkflowStatusFailed, WorkflowStatusPaused},
		WorkflowStatusPaused:    {WorkflowStatusRunning, WorkflowStatusFailed},
		WorkflowStatusCompleted: {},
		WorkflowStatusFailed:    {},
	}
	if next, ok := validTransitions[from]; ok {
		for _, s := range next {
			if s == to {
				return true
			}
		}
	}
	return false
}

// Transition provides the transition.
func (w *WorkflowInstance) Transition(to string) error {
	if !w.CanTransition(w.State, to) {
		return fmt.Errorf("invalid workflow transition: %s -> %s", w.State, to)
	}
	w.State = to
	w.Version++
	now := SystemClock{}.Now()
	w.UpdatedAt = now
	if to == WorkflowStatusCompleted || to == WorkflowStatusFailed {
		w.CompletedAt = &now
	}
	return nil
}

// WorkflowStateType represents the workflow state type.
type WorkflowStateType string

const (
	// WorkflowStateTypeStart identifies the workflow state type start value.
	WorkflowStateTypeStart WorkflowStateType = "start"
	// WorkflowStateTypeEnd identifies the workflow state type end value.
	WorkflowStateTypeEnd WorkflowStateType = "end"
	// WorkflowStateTypeIntermediate identifies the workflow state type intermediate value.
	WorkflowStateTypeIntermediate WorkflowStateType = "intermediate"
)

// WorkflowState represents the workflow state.
type WorkflowState struct {
	Name     string            `json:"name"`
	Type     WorkflowStateType `json:"type"`
	RuleSets []string          `json:"rule_sets,omitempty"`
	OnEnter  []Action          `json:"on_enter,omitempty"`
	OnExit   []Action          `json:"on_exit,omitempty"`
}

// WorkflowTransition represents the workflow transition.
type WorkflowTransition struct {
	From      string     `json:"from"`
	To        string     `json:"to"`
	EventType string     `json:"event_type"`
	Condition *Predicate `json:"condition,omitempty"`
}

// WorkflowDefinition represents the workflow definition.
type WorkflowDefinition struct {
	WorkflowType string               `json:"workflow_type"`
	Version      int64                `json:"version"`
	States       []WorkflowState      `json:"states"`
	Transitions  []WorkflowTransition `json:"transitions"`
	Rules        map[string]string    `json:"rules,omitempty"`
	CreatedAt    time.Time            `json:"created_at"`
	UpdatedAt    time.Time            `json:"updated_at"`
}
