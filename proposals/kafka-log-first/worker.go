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
	ErrHopLimit = errors.New("event hop limit reached")
	// ErrMissingRuleset indicates the pinned active ruleset revision is unavailable.
	ErrMissingRuleset = errors.New("active ruleset revision is unavailable")
)

// EventEnvelope uses the same stable public wire shape as the API and production workers.
// SchemaVersion is an internal compatibility guard carried in meta when explicitly set.
type EventEnvelope struct {
	SchemaVersion int             `json:"-"`
	ID            string          `json:"-"`
	TenantID      string          `json:"-"`
	EventType     string          `json:"-"`
	PartitionKey  string          `json:"-"`
	OccurredAt    time.Time       `json:"-"`
	Payload       json.RawMessage `json:"-"`
	Contract      *EventContract  `json:"-"`
	Metadata      EventMetadata   `json:"-"`
}

// EventContract identifies the immutable payload contract used by an event.
type EventContract struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	SchemaHash string `json:"schema_hash,omitempty"`
}

// EventMetadata contains known runtime fields and preserves future keys in Extra.
type EventMetadata struct {
	TenantID           string                     `json:"tenant_id,omitempty"`
	PartitionKey       string                     `json:"partition_key,omitempty"`
	WorkflowID         string                     `json:"workflow_id,omitempty"`
	MessageKind        string                     `json:"message_kind,omitempty"`
	CorrelationID      string                     `json:"correlation_id,omitempty"`
	CausationID        string                     `json:"causation_id,omitempty"`
	BatchID            string                     `json:"batch_id,omitempty"`
	Hop                int                        `json:"hop"`
	RequestedRulesetID string                     `json:"requested_ruleset_id,omitempty"`
	TargetServiceID    string                     `json:"flow.service_id,omitempty"`
	DeploymentRevision int64                      `json:"flow.deployment_revision,omitempty"`
	SchemaVersion      int                        `json:"-"`
	Traceparent        string                     `json:"traceparent,omitempty"`
	Headers            map[string]string          `json:"headers,omitempty"`
	Extra              map[string]json.RawMessage `json:"-"`
}

type eventMetadataAlias EventMetadata

type eventEnvelopeWire struct {
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
	Contract   *EventContract  `json:"contract,omitempty"`
	Meta       EventMetadata   `json:"meta"`
}

// MarshalJSON writes known and extension metadata into one extensible JSON object.
func (m EventMetadata) MarshalJSON() ([]byte, error) {
	encoded, err := json.Marshal(eventMetadataAlias(m))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		return nil, err
	}
	if m.SchemaVersion > 0 {
		fields["schema_version"], _ = json.Marshal(m.SchemaVersion)
	}
	for key, value := range m.Extra {
		if _, known := fields[key]; !known {
			fields[key] = append(json.RawMessage(nil), value...)
		}
	}
	return json.Marshal(fields)
}

// UnmarshalJSON decodes known metadata and retains all unknown keys in Extra.
func (m *EventMetadata) UnmarshalJSON(data []byte) error {
	var known eventMetadataAlias
	if err := json.Unmarshal(data, &known); err != nil {
		return err
	}
	var all map[string]json.RawMessage
	if err := json.Unmarshal(data, &all); err != nil {
		return err
	}
	if raw, exists := all["schema_version"]; exists {
		var version int
		if err := json.Unmarshal(raw, &version); err == nil && version > 0 {
			known.SchemaVersion = version
			delete(all, "schema_version")
		}
	}
	for _, key := range []string{"tenant_id", "partition_key", "workflow_id", "message_kind", "correlation_id", "causation_id", "batch_id", "hop", "requested_ruleset_id", "flow.service_id", "flow.deployment_revision", "traceparent", "headers"} {
		delete(all, key)
	}
	*m = EventMetadata(known)
	m.Extra = all
	return nil
}

// MarshalJSON serializes the stable envelope and nests runtime metadata under meta.
func (e EventEnvelope) MarshalJSON() ([]byte, error) {
	meta := e.Metadata
	if e.TenantID != "" {
		meta.TenantID = e.TenantID
	}
	if e.PartitionKey != "" {
		meta.PartitionKey = e.PartitionKey
	}
	if e.SchemaVersion > 0 && meta.Extra["schema_version"] == nil {
		meta.SchemaVersion = e.SchemaVersion
	}
	return json.Marshal(eventEnvelopeWire{ID: e.ID, Type: e.EventType, OccurredAt: e.OccurredAt, Data: e.Payload, Contract: e.Contract, Meta: meta})
}

// UnmarshalJSON reads the stable envelope and rejects top-level runtime metadata.
func (e *EventEnvelope) UnmarshalJSON(data []byte) error {
	var top map[string]json.RawMessage
	if err := json.Unmarshal(data, &top); err != nil {
		return err
	}
	allowed := map[string]bool{"id": true, "type": true, "occurred_at": true, "data": true, "contract": true, "meta": true}
	for key := range top {
		if !allowed[key] {
			return fmt.Errorf("unknown event envelope field %q; runtime metadata belongs inside meta", key)
		}
	}
	var wire eventEnvelopeWire
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if len(wire.Meta.TenantID) == 0 || len(wire.Meta.PartitionKey) == 0 {
		return errors.New("meta.tenant_id and meta.partition_key are required")
	}
	version := wire.Meta.SchemaVersion
	if version == 0 {
		version = 1
	}
	*e = EventEnvelope{SchemaVersion: version, ID: wire.ID, TenantID: wire.Meta.TenantID, EventType: wire.Type,
		PartitionKey: wire.Meta.PartitionKey, OccurredAt: wire.OccurredAt, Payload: wire.Data, Contract: wire.Contract, Metadata: wire.Meta}
	return nil
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

// RulesetRef pins a globally configured ruleset to an immutable revision.
type RulesetRef struct {
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
	Resolve(context.Context, string, int64, EventEnvelope) ([]RulesetRef, error)
	GetRevision(context.Context, RulesetRef) (CompiledRuleset, string, error)
}

// SubscriberRouter is intentionally separate from the rule evaluator. It returns
// generic delivery intents for active subscriptions matching the event type.
type SubscriberRouter interface {
	Route(context.Context, EventEnvelope, string) ([]Effect, error)
}

// Evaluator turns an event into generic effects and audit records.
type Evaluator interface {
	Evaluate(context.Context, string, int64, EventEnvelope) ([]Effect, []ExecutionAudit, error)
}

// RuleEvaluator resolves pinned rulesets and delegates subscriber fan-out.
type RuleEvaluator struct {
	Resolver Resolver
	Router   SubscriberRouter
	MaxHop   int
	Now      func() time.Time
}

// Evaluate is pure with respect to external side effects: it builds outputs in
// memory. The transaction adapter persists these outputs atomically with offsets.
func (e *RuleEvaluator) Evaluate(ctx context.Context, deploymentID string, deploymentRevision int64, env EventEnvelope) ([]Effect, []ExecutionAudit, error) {
	if e == nil || e.Resolver == nil {
		return nil, nil, errors.New("ruleset resolver is not configured")
	}
	if deploymentID == "" || deploymentRevision <= 0 {
		return nil, nil, errors.New("worker service deployment identity and revision are required")
	}
	if env.SchemaVersion != 1 {
		return nil, nil, ErrUnsupportedSchema
	}
	if env.ID == "" || env.TenantID == "" || env.EventType == "" || env.PartitionKey == "" {
		return nil, nil, errors.New("event id, tenant, type, and partition key are required")
	}
	if env.Metadata.TargetServiceID != deploymentID || env.Metadata.DeploymentRevision != deploymentRevision {
		return nil, nil, errors.New("event target does not match this worker deployment")
	}
	if e.MaxHop > 0 && env.Metadata.Hop >= e.MaxHop {
		return nil, nil, ErrHopLimit
	}
	refs, err := e.Resolver.Resolve(ctx, deploymentID, deploymentRevision, env)
	if err != nil {
		return nil, nil, err
	}
	now := time.Now
	if e.Now != nil {
		now = e.Now
	}
	var effects []Effect
	var audits []ExecutionAudit
	for _, ref := range refs {
		if ref.RulesetID == "" || ref.Revision <= 0 {
			return nil, nil, errors.New("resolver returned an invalid ruleset reference")
		}
		ruleset, contentHash, err := e.Resolver.GetRevision(ctx, ref)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: %s@%d: %w", ErrMissingRuleset, ref.RulesetID, ref.Revision, err)
		}
		if ruleset == nil {
			return nil, nil, fmt.Errorf("%w: %s@%d: resolver returned a nil compiled ruleset", ErrMissingRuleset, ref.RulesetID, ref.Revision)
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

	// Subscriber fan-out occurs once per input event, even when this deployment has
	// no assigned rulesets. It is a separate routing stage, never nested inside rule evaluation.
	// The router must generate deterministic IDs and retain the event tenant on each delivery intent.
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
	// DeploymentID identifies the Domain/Service worker deployment this process serves.
	DeploymentID       string
	DeploymentRevision int64
	GroupID            string
	InputTopic         string
	EffectsTopic       string
	AuditTopic         string
	Store              LogStore
	Evaluator          Evaluator
	Codec              Codec
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
	if w.DeploymentID == "" || w.DeploymentRevision <= 0 || w.GroupID == "" || w.Store == nil || w.Evaluator == nil || w.Codec == nil {
		return errors.New("worker deployment identity or dependencies are not configured")
	}
	if len(batch.Offsets) == 0 {
		return errors.New("input offsets are required for transactional processing")
	}
	offsetsByPartition := make(map[string]int64, len(batch.Offsets))
	for _, offset := range batch.Offsets {
		if offset.Topic == "" || offset.Partition < 0 || offset.Next < 0 {
			return errors.New("input offset contains an invalid topic, partition, or sequence")
		}
		key := offset.Topic + "\x00" + fmt.Sprint(offset.Partition)
		if _, exists := offsetsByPartition[key]; exists {
			return errors.New("duplicate input checkpoint for topic and partition")
		}
		offsetsByPartition[key] = offset.Next
	}
	maxOffsetByPartition := make(map[string]int64, len(batch.Offsets))
	for _, record := range batch.Records {
		if record.Topic == "" || record.Partition < 0 || record.Offset < 0 {
			return errors.New("input record contains an invalid topic, partition, or offset")
		}
		key := record.Topic + "\x00" + fmt.Sprint(record.Partition)
		next, exists := offsetsByPartition[key]
		if !exists || next <= record.Offset {
			return fmt.Errorf("input checkpoint does not cover %s[%d]@%d", record.Topic, record.Partition, record.Offset)
		}
		if previous, found := maxOffsetByPartition[key]; !found || record.Offset > previous {
			maxOffsetByPartition[key] = record.Offset
		}
	}
	if len(maxOffsetByPartition) != len(offsetsByPartition) {
		return errors.New("input checkpoints must correspond exactly to partitions present in the batch")
	}
	for key, maxOffset := range maxOffsetByPartition {
		if offsetsByPartition[key] != maxOffset+1 {
			return fmt.Errorf("input checkpoint for %q must be exactly one past the highest processed offset", key)
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
		if env.Metadata.TargetServiceID != w.DeploymentID || env.Metadata.DeploymentRevision != w.DeploymentRevision {
			return fmt.Errorf("event %q target does not match worker deployment %q revision %d", env.ID, w.DeploymentID, w.DeploymentRevision)
		}
		effects, audits, err := w.Evaluator.Evaluate(ctx, w.DeploymentID, w.DeploymentRevision, env)
		if err != nil {
			// Production code must classify permanent errors into a DLQ transaction.
			// Returning an error aborts this batch; do not silently skip poison records.
			return fmt.Errorf("evaluate event %s: %w", env.ID, err)
		}
		for _, effect := range effects {
			if effect.TenantID != env.TenantID {
				return fmt.Errorf("effect %q tenant %q does not match event tenant %q", effect.ID, effect.TenantID, env.TenantID)
			}
			if effect.ID == "" || effect.IdempotencyKey == "" || effect.Destination == "" || effect.PartitionKey == "" {
				return errors.New("effect id, idempotency key, destination, and partition key are required")
			}
			value, err := w.Codec.EncodeEffect(effect)
			if err != nil {
				return err
			}
			if err := tx.Append(ctx, LogRecord{ID: effect.ID, TenantID: effect.TenantID, PartitionKey: effect.PartitionKey, Kind: effect.Destination, Key: []byte(effect.PartitionKey), Value: value}); err != nil {
				return err
			}
		}
		for _, audit := range audits {
			if audit.TenantID != env.TenantID || audit.EventID != env.ID || audit.RulesetID == "" || audit.Revision <= 0 {
				return fmt.Errorf("audit for event %q has invalid tenant, event, or ruleset identity", env.ID)
			}
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
