package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// LogContractRef pins a contract to an immutable version and content hash.
type LogContractRef struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	ContentHash string `json:"content_hash"`
}

// LogRuleRevisionRef pins the exact rule revision and compiler used by an execution.
type LogRuleRevisionRef struct {
	TenantID        string `json:"tenant_id"`
	RuleSet         string `json:"rule_set"`
	Revision        int64  `json:"revision"`
	ContentHash     string `json:"content_hash"`
	CompilerVersion string `json:"compiler_version"`
}

// LogOutputRoute is a tagged target. Exactly one target field appropriate to Kind
// must be populated, so an effect cannot be ambiguously routed.
type LogOutputRoute struct {
	Kind          string `json:"kind"`
	Destination   string `json:"destination,omitempty"`
	RuleSet       string `json:"rule_set,omitempty"`
	WorkflowID    string `json:"workflow_id,omitempty"`
	ScheduleID    string `json:"schedule_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	Processor     string `json:"processor,omitempty"`
}

// Validate checks that the route has one supported kind and exactly one target.
func (r LogOutputRoute) Validate() error {
	targets := map[string]string{
		"consumer": r.Destination,
		"rule_set": r.RuleSet,
		"workflow": r.WorkflowID,
		"schedule": r.ScheduleID,
		"reply":    r.CorrelationID,
		"internal": r.Processor,
	}
	target, ok := targets[r.Kind]
	if !ok {
		return fmt.Errorf("unsupported output route kind %q", r.Kind)
	}
	if strings.TrimSpace(target) == "" {
		return fmt.Errorf("output route %q requires its target", r.Kind)
	}
	for kind, value := range targets {
		if kind != r.Kind && value != "" {
			return fmt.Errorf("output route %q contains an unrelated %s target", r.Kind, kind)
		}
	}
	return nil
}

// LogEffectIntent is durably embedded inside ExecutionCommittedPayload. The
// dispatch index may be rebuilt from this intent after a crash.
type LogEffectIntent struct {
	EffectID       string          `json:"effect_id"`
	EffectKey      string          `json:"effect_key"`
	TenantID       string          `json:"tenant_id"`
	SourceEventID  string          `json:"source_event_id"`
	ExecutionID    string          `json:"execution_id"`
	OutputContract LogContractRef  `json:"output_contract"`
	Route          LogOutputRoute  `json:"route"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	PayloadSHA256  string          `json:"payload_sha256"`
	PartitionKey   string          `json:"partition_key"`
}

// ExecutionCommittedPayload is the atomic result of evaluating one event.
// Effects must be present even when empty, encoded as [] rather than omitted or
// null. This makes it possible to distinguish "no effects" from an incomplete
// commit written by an old or buggy producer.
type ExecutionCommittedPayload struct {
	ExecutionID    string               `json:"execution_id"`
	Outcome        string               `json:"outcome"`
	OutcomeDetails json.RawMessage      `json:"outcome_details,omitempty"`
	DecisionHash   string               `json:"decision_hash"`
	RuleRevisions  []LogRuleRevisionRef `json:"rule_revisions"`
	MatchedRuleIDs []string             `json:"matched_rule_ids"`
	ResultPayload  json.RawMessage      `json:"result_payload"`
	Effects        []LogEffectIntent    `json:"effects"`
	FencingEpoch   uint64               `json:"fencing_epoch"`
}

// Validate enforces the execution/effect atomicity contract before a commit can
// be appended. The enclosing record supplies the authoritative tenant and event.
func (p ExecutionCommittedPayload) Validate(tenantID, eventID string) error {
	if strings.TrimSpace(p.ExecutionID) == "" {
		return errors.New("execution.committed requires execution_id")
	}
	switch p.Outcome {
	case "completed", "no_match", "quarantined":
	default:
		return fmt.Errorf("unsupported execution outcome %q", p.Outcome)
	}
	if len(p.OutcomeDetails) > 0 && !json.Valid(p.OutcomeDetails) {
		return errors.New("execution.committed outcome_details must be valid JSON")
	}
	if !isSHA256Hex(p.DecisionHash) {
		return errors.New("execution.committed requires a SHA-256 decision_hash")
	}
	if len(p.ResultPayload) == 0 || !json.Valid(p.ResultPayload) {
		return errors.New("execution.committed requires a valid result_payload")
	}
	if p.RuleRevisions == nil {
		return errors.New("execution.committed requires rule_revisions array")
	}
	if p.MatchedRuleIDs == nil {
		return errors.New("execution.committed requires matched_rule_ids array")
	}
	if p.Effects == nil {
		return errors.New("execution.committed requires an explicit effects array")
	}
	if p.FencingEpoch == 0 {
		return errors.New("execution.committed requires a positive fencing_epoch")
	}
	if (p.Outcome == "quarantined" || p.Outcome == "no_match") && len(p.Effects) != 0 {
		return fmt.Errorf("%s execution cannot emit effects", p.Outcome)
	}
	for i, revision := range p.RuleRevisions {
		if strings.TrimSpace(revision.TenantID) == "" || revision.TenantID != tenantID {
			return fmt.Errorf("rule_revisions[%d] tenant_id does not match event tenant", i)
		}
		if strings.TrimSpace(revision.RuleSet) == "" || revision.Revision <= 0 {
			return fmt.Errorf("rule_revisions[%d] requires rule_set and positive revision", i)
		}
		if !isSHA256Hex(revision.ContentHash) || strings.TrimSpace(revision.CompilerVersion) == "" {
			return fmt.Errorf("rule_revisions[%d] requires content_hash and compiler_version", i)
		}
	}
	seenEffectIDs := make(map[string]struct{}, len(p.Effects))
	for i, effect := range p.Effects {
		if _, exists := seenEffectIDs[effect.EffectID]; exists {
			return fmt.Errorf("effects[%d] duplicates effect_id %q", i, effect.EffectID)
		}
		seenEffectIDs[effect.EffectID] = struct{}{}
		if strings.TrimSpace(effect.EffectID) == "" || strings.TrimSpace(effect.EffectKey) == "" || strings.TrimSpace(effect.ExecutionID) == "" {
			return fmt.Errorf("effects[%d] requires effect_id, effect_key and execution_id", i)
		}
		expectedEffectID := DeterministicID("effect", tenantID, effect.PartitionKey, eventID, p.ExecutionID, effect.EffectKey)
		if effect.EffectID != expectedEffectID {
			return fmt.Errorf("effects[%d] effect_id does not match its deterministic identity", i)
		}
		if effect.ExecutionID != p.ExecutionID {
			return fmt.Errorf("effects[%d] belongs to a different execution", i)
		}
		if effect.TenantID != tenantID || effect.SourceEventID != eventID {
			return fmt.Errorf("effects[%d] tenant or source event does not match enclosing record", i)
		}
		if strings.TrimSpace(effect.PartitionKey) == "" {
			return fmt.Errorf("effects[%d] requires partition_key", i)
		}
		if err := validateLogContractRef(effect.OutputContract); err != nil {
			return fmt.Errorf("effects[%d] output_contract: %w", i, err)
		}
		if err := effect.Route.Validate(); err != nil {
			return fmt.Errorf("effects[%d] route: %w", i, err)
		}
		switch effect.Kind {
		case "publish_event", "invoke_command", "reply", "schedule":
		default:
			return fmt.Errorf("effects[%d] has unsupported kind %q", i, effect.Kind)
		}
		if len(effect.Payload) == 0 || !json.Valid(effect.Payload) {
			return fmt.Errorf("effects[%d] requires a valid JSON payload", i)
		}
		if !isSHA256Hex(effect.PayloadSHA256) || payloadSHA256(effect.Payload) != effect.PayloadSHA256 {
			return fmt.Errorf("effects[%d] payload_sha256 does not match payload", i)
		}
	}
	return nil
}

func validateLogContractRef(ref LogContractRef) error {
	if strings.TrimSpace(ref.Name) == "" || strings.TrimSpace(ref.Version) == "" || !isSHA256Hex(ref.ContentHash) {
		return errors.New("name, version and SHA-256 content_hash are required")
	}
	return nil
}

func isSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

// payloadSHA256 hashes canonical JSON so Go and Rust agree regardless of
// insignificant whitespace or object-key order.
func payloadSHA256(payload json.RawMessage) string {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		sum := sha256.Sum256(payload)
		return hex.EncodeToString(sum[:])
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		sum := sha256.Sum256(payload)
		return hex.EncodeToString(sum[:])
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}
