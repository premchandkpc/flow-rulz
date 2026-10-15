// Package logfirst contains a client-neutral sketch of the Kafka transactional worker.
// A production adapter must map these interfaces to a maintained Kafka client and
// must enforce consumer-group assignment and transactional offset semantics.
package logfirst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrUnsupportedSchema indicates an envelope version this worker cannot process.
	ErrUnsupportedSchema = errors.New("unsupported event schema")
	// ErrHopLimit indicates an event exceeded the configured hop limit.
	ErrHopLimit           = errors.New("event hop limit reached")
	// ErrMissingRuleset indicates the pinned active ruleset revision is unavailable.
	ErrMissingRuleset     = errors.New("active ruleset revision is unavailable")
)

// EventEnvelope is the versioned, tenant-scoped event consumed by the worker.
type EventEnvelope struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"id"`
	TenantID      string          `json:"tenant_id"`
	EventType     string          `json:"event_type"`
	PartitionKey  string          `json:"partition_key"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Payload       json.RawMessage `json:"payload"`
	Metadata      EventMetadata   `json:"metadata"`
}

// EventMetadata carries correlation and event-chain metadata.
type EventMetadata struct {
	CorrelationID       string `json:"correlation_id,omitempty"`
	CausationID         string `json:"causation_id,omitempty"`
	BatchID             string `json:"batch_id,omitempty"`
	Hop                 int    `json:"hop"`
	RequestedRulesetID  string `json:"requested_ruleset_id,omitempty"`
}

// Record is an input record with its source stream position.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Offset identifies the next source offset to consume after a batch.
type Offset struct {
	Topic     string
	Partition int32
	Next      int64
}

// RulesetRef pins a tenant-scoped ruleset to an immutable revision.
type RulesetRef struct {
	TenantID  string
	RulesetID string
	Revision  int64
}

// CompiledRuleset evaluates one event without performing external side effects.
type CompiledRuleset interface {
	Evaluate(context.Context, EventEnvelope) ([]Effect, error)
}

// Effect is a deterministic, generic output produced by rule evaluation or routing.
type Effect struct {
	ID             string
	IdempotencyKey string
	TenantID       string
	ExecutionID    string
	Type           string
	Destination    string
	Name           string
	PartitionKey   string
	CausationID    string
	Hop            int
	Payload        json.RawMessage
}

// ExecutionAudit records which immutable ruleset revision processed an event.
type ExecutionAudit struct {
	TenantID     string
	EventID      string
	ExecutionID  string
	RulesetID    string
	Revision     int64
	ContentHash  string
	DecisionHash string
	OccurredAt   time.Time
}

// Resolver reads rebuildable projections populated from compacted config topics.
// Resolve must return a pinned immutable revision, never a mutable "latest" object.
type Resolver interface {
	Resolve(context.Context, EventEnvelope) ([]RulesetRef, error)
	GetRevision(context.Context, RulesetRef) (CompiledRuleset, string, error)
}

// SubscriberRouter is intentionally separate from the rule evaluator. It returns
// generic delivery intents for active, tenant-scoped subscribers.
type SubscriberRouter interface {
	Route(context.Context, EventEnvelope, string) ([]Effect, error)
}

// Evaluator turns an event into generic effects and audit records.
type Evaluator interface {
	Evaluate(context.Context, EventEnvelope) ([]Effect, []ExecutionAudit, error)
}

// RuleEvaluator resolves pinned rulesets and delegates subscriber fan-out.
type RuleEvaluator struct {
	Resolver  Resolver
	Router    SubscriberRouter
	MaxHop    int
	Now       func() time.Time
}

// Evaluate is pure with respect to external side effects: it builds outputs in
// memory. The transaction adapter persists these outputs atomically with offsets.
func (e *RuleEvaluator) Evaluate(ctx context.Context, env EventEnvelope) ([]Effect, []ExecutionAudit, error) {
	if e == nil || e.Resolver == nil {
		return nil, nil, errors.New("ruleset resolver is not configured")
	}
	if env.SchemaVersion != 1 {
		return nil, nil, ErrUnsupportedSchema
	}
	if env.ID == "" || env.TenantID == "" || env.EventType == "" || env.PartitionKey == "" {
		return nil, nil, errors.New("event id, tenant, type, and partition key are required")
	}
	if e.MaxHop > 0 && env.Metadata.Hop >= e.MaxHop {
		return nil, nil, ErrHopLimit
	}
	refs, err := e.Resolver.Resolve(ctx, env)
	if err != nil {
		return nil, nil, err
	}
	if len(refs) == 0 {
		// Keep policy explicit: no active ruleset means no subscriber fan-out.
		return nil, nil, nil
	}

	now := time.Now
	if e.Now != nil {
		now = e.Now
	}
	var effects []Effect
	var audits []ExecutionAudit
	for _, ref := range refs {
		if ref.TenantID != env.TenantID || ref.RulesetID == "" || ref.Revision <= 0 {
			return nil, nil, errors.New("resolver returned an invalid or cross-tenant ruleset reference")
		}
		ruleset, contentHash, err := e.Resolver.GetRevision(ctx, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %s/%s@%d: %w", ErrMissingRuleset, ref.TenantID, ref.RulesetID, ref.Revision, err)
		}
		if ruleset == nil {
			return nil, nil, fmt.Errorf("%w: %s/%s@%d: resolver returned a nil compiled ruleset", ErrMissingRuleset, ref.TenantID, ref.RulesetID, ref.Revision)
		}
		ruleEffects, err := ruleset.Evaluate(ctx, env)
		if err != nil {
			return nil, nil, err
		}
		for i := range ruleEffects {
			ruleEffects[i].TenantID = env.TenantID
			ruleEffects[i].CausationID = env.ID
			ruleEffects[i].Hop = env.Metadata.Hop + 1
			if ruleEffects[i].IdempotencyKey == "" {
				return nil, nil, errors.New("effect idempotency key must be deterministic and non-empty")
			}
			if ruleEffects[i].Destination == "" || ruleEffects[i].PartitionKey == "" {
				return nil, nil, errors.New("effect destination and partition key are required")
			}
			if ruleEffects[i].ID == "" {
				ruleEffects[i].ID = ruleEffects[i].IdempotencyKey
			}
		}
		effects = append(effects, ruleEffects...)
		audits = append(audits, ExecutionAudit{
			TenantID: env.TenantID, EventID: env.ID,
			RulesetID: ref.RulesetID, Revision: ref.Revision,
			ContentHash: contentHash, OccurredAt: now().UTC(),
		})
	}

	// Subscriber fan-out occurs once per input event, outside the per-ruleset loop.
	// The router must generate deterministic IDs and tenant-filter every subscription.
	if e.Router != nil {
		subscriptionEffects, err := e.Router.Route(ctx, env, env.ID)
		if err != nil {
			return nil, nil, err
		}
		for i := range subscriptionEffects {
			subscriptionEffects[i].TenantID = env.TenantID
			subscriptionEffects[i].CausationID = env.ID
			if subscriptionEffects[i].IdempotencyKey == "" {
				return nil, nil, errors.New("subscriber effect idempotency key is required")
			}
			if subscriptionEffects[i].Destination == "" || subscriptionEffects[i].PartitionKey == "" {
				return nil, nil, errors.New("subscriber destination and partition key are required")
			}
			if subscriptionEffects[i].ID == "" {
				subscriptionEffects[i].ID = subscriptionEffects[i].IdempotencyKey
			}
		}
		effects = append(effects, subscriptionEffects...)
	}
	return effects, audits, nil
}

// Codec encodes and decodes backend-neutral event, effect, and audit envelopes.
type Codec interface {
	DecodeEvent([]byte) (EventEnvelope, error)
	EncodeEffect(Effect) ([]byte, error)
	EncodeAudit(ExecutionAudit) ([]byte, error)
}

// Worker owns no shard lease or correctness-critical key queue. Kafka consumer-group
// assignment and stable record keys define partition ownership and per-partition order.
type Worker struct {
	GroupID       string
	InputTopic    string
	EffectsTopic  string
	AuditTopic    string
	Store         LogStore
	Evaluator     Evaluator
	Codec         Codec
}

// Batch contains input records and the next offsets to checkpoint atomically.
type Batch struct {
	Records []Record
	Offsets []Offset
}

// ProcessBatch atomically appends outputs and advances input checkpoints.
func (w *Worker) ProcessBatch(ctx context.Context, batch Batch) error {
	if len(batch.Records) == 0 {
		return nil
	}
	if w.GroupID == "" || w.Store == nil || w.Evaluator == nil || w.Codec == nil {
		return errors.New("worker dependencies are not configured")
	}
	if len(batch.Offsets) == 0 {
		return errors.New("input offsets are required for transactional processing")
	}
	for _, offset := range batch.Offsets {
		if offset.Topic == "" || offset.Partition < 0 || offset.Next < 0 {
			return errors.New("input offset contains an invalid topic, partition, or sequence")
		}
	}

	tx, err := w.Store.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			abortCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = tx.Abort(abortCtx)
		}
	}()

	for _, record := range batch.Records {
		env, err := w.Codec.DecodeEvent(record.Value)
		if err != nil {
			return fmt.Errorf("decode %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
		}
		effects, audits, err := w.Evaluator.Evaluate(ctx, env)
		if err != nil {
			// Production code must classify permanent errors into a DLQ transaction.
			// Returning an error aborts this batch; do not silently skip poison records.
			return fmt.Errorf("evaluate event %s: %w", env.ID, err)
		}
		for _, effect := range effects {
			value, err := w.Codec.EncodeEffect(effect)
			if err != nil {
				return err
			}
			if err := tx.Append(ctx, LogRecord{ID: effect.ID, TenantID: effect.TenantID, PartitionKey: effect.PartitionKey, Kind: effect.Destination, Key: []byte(effect.PartitionKey), Value: value}); err != nil {
				return err
			}
		}
		for _, audit := range audits {
			value, err := w.Codec.EncodeAudit(audit)
			if err != nil {
				return err
			}
			if err := tx.Append(ctx, LogRecord{ID: audit.TenantID + ":" + audit.EventID + ":" + audit.RulesetID + ":" + fmt.Sprint(audit.Revision), TenantID: audit.TenantID, PartitionKey: audit.TenantID + ":" + audit.EventID, Kind: w.AuditTopic, Key: []byte(audit.TenantID + ":" + audit.EventID), Value: value}); err != nil {
				return err
			}
		}
	}
	positions := make([]InputCheckpoint, 0, len(batch.Offsets))
	for _, offset := range batch.Offsets {
		positions = append(positions, InputCheckpoint{Stream: offset.Topic, Partition: fmt.Sprint(offset.Partition), NextSequence: offset.Next})
	}
	if err := tx.CommitInputPosition(ctx, w.GroupID, positions); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}
