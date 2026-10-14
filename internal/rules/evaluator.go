package rules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

var _ ports.RuleEvaluator = (*Evaluator)(nil)

// Evaluator evaluates compiled rules against events.
type Evaluator struct {
	clock ports.Clock
}

// NewEvaluator creates a new Evaluator with the given clock.
func NewEvaluator(clock ports.Clock) *Evaluator {
	return &Evaluator{clock: clock}
}

// Evaluate runs the rule evaluation against an event and returns a Decision.
func (e *Evaluator) Evaluate(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage) (*domain.Decision, error) {
	matched, effects, explanations, err := e.evaluateRules(revision, event, facts)
	if err != nil {
		return nil, err
	}

	hash := domain.ComputeDecisionHash(revision.ContentHash, event.ID, "", matched, effects)

	return &domain.Decision{
		MatchedRules: matched,
		Effects:      effects,
		Hash:         hash,
		Explanations: explanations,
	}, nil
}

// evaluateRules iterates through compiled rules, evaluates predicates, and builds effects/explanations.
func (e *Evaluator) evaluateRules(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage) ([]string, []domain.Effect, []domain.RuleExplanation, error) {
	matched := make([]string, 0)
	effects := make([]domain.Effect, 0)
	explanations := make([]domain.RuleExplanation, 0, len(revision.Compiled))

	for _, rule := range revision.Compiled {
		ok, err := e.evalPredicate(rule.When, event.Data, facts)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("rule %s: %w: %w", rule.ID, domain.ErrEvaluationError, err)
		}

		matched, effects, explanations, err = e.processRuleResult(rule, ok, matched, effects, explanations, revision, event)
		if err != nil {
			return nil, nil, nil, err
		}
		if revision.MatchMode == domain.MatchModeFirstMatch && ok {
			break
		}
	}
	return matched, effects, explanations, nil
}

// processRuleResult handles the result of a rule evaluation (matched or not).
func (e *Evaluator) processRuleResult(rule domain.CompiledRule, ok bool, matched []string, effects []domain.Effect, explanations []domain.RuleExplanation, revision *domain.RuleRevision, event *domain.EventEnvelope) ([]string, []domain.Effect, []domain.RuleExplanation, error) {
	explanation := domain.RuleExplanation{
		RuleID:   rule.ID,
		RuleName: rule.Name,
		Matched:  ok,
		Priority: rule.Priority,
	}

	if ok {
		matched = append(matched, rule.ID)
		actions := make([]string, 0, len(rule.Then))
		for i, action := range rule.Then {
			effect, err := e.buildEffect(revision, event, rule.ID, i, action)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("rule %s action %d: %w: %w", rule.ID, i, domain.ErrEvaluationError, err)
			}
			effects = append(effects, effect)
			actions = append(actions, describeAction(action))
		}
		explanation.Reason = "predicate matched"
		explanation.Actions = actions
	} else {
		actions := make([]string, 0, len(rule.Otherwise))
		for i, action := range rule.Otherwise {
			effect, err := e.buildEffect(revision, event, rule.ID, i, action)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("rule %s otherwise action %d: %w: %w", rule.ID, i, domain.ErrEvaluationError, err)
			}
			effects = append(effects, effect)
			actions = append(actions, describeAction(action))
		}
		explanation.Reason = "predicate did not match"
		explanation.Actions = actions
	}
	explanations = append(explanations, explanation)
	return matched, effects, explanations, nil
}

func (e *Evaluator) evalPredicate(p domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	if p.All != nil {
		return e.evalAll(p.All, data, facts)
	}
	if p.Any != nil {
		return e.evalAny(p.Any, data, facts)
	}
	if p.Not != nil {
		return e.evalNot(p.Not, data, facts)
	}
	return e.evalLeaf(p, data, facts)
}

func (e *Evaluator) evalAll(predicates []*domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	for _, predicate := range predicates {
		if predicate == nil {
			return false, domain.ErrInvalidPredicate
		}
		matched, err := e.evalPredicate(*predicate, data, facts)
		if err != nil {
			return false, err
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

func (e *Evaluator) evalAny(predicates []*domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	for _, predicate := range predicates {
		if predicate == nil {
			continue
		}
		matched, err := e.evalPredicate(*predicate, data, facts)
		if err != nil {
			return false, err
		}
		if matched {
			return true, nil
		}
	}
	return false, nil
}

func (e *Evaluator) evalNot(predicate *domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	if predicate == nil {
		return false, domain.ErrInvalidPredicate
	}
	matched, err := e.evalPredicate(*predicate, data, facts)
	if err != nil {
		return false, err
	}
	return !matched, nil
}

func (e *Evaluator) evalLeaf(p domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	value, err := resolvePath(p.Path, data, facts)
	if err != nil {
		return false, fmt.Errorf("%w: resolve predicate path %q: %w", domain.ErrEvaluationError, p.Path, err)
	}
	if p.Op == domain.OpExists {
		return value != nil, nil
	}
	if p.Op == domain.OpNotExists {
		return value == nil, nil
	}
	// Missing field semantics:
	// - eq: false (missing != anything)
	// - neq: true (missing != expected)
	// - in: false (missing not in list)
	// - not_in: true (missing not in list)
	// - gt/gte/lt/lte: false (cannot compare missing)
	if value == nil {
		switch p.Op {
		case domain.OpNeq, domain.OpNotIn:
			return true, nil
		default:
			return false, nil
		}
	}
	return evaluateLeafValue(p.Op, value, p.Value)
}

func evaluateLeafValue(op domain.Operator, actual, expected interface{}) (bool, error) {
	switch op {
	case domain.OpEq:
		return compareEq(actual, expected), nil
	case domain.OpNeq:
		return !compareEq(actual, expected), nil
	case domain.OpGt, domain.OpGte, domain.OpLt, domain.OpLte:
		return compareNumericLeaf(op, actual, expected)
	case domain.OpIn:
		return compareIn(actual, expected), nil
	case domain.OpNotIn:
		return !compareIn(actual, expected), nil
	default:
		return false, domain.ErrInvalidOperator
	}
}

func compareNumericLeaf(op domain.Operator, actual, expected interface{}) (bool, error) {
	comparison := compareCmp(actual, expected)
	if comparison == 2 {
		return false, fmt.Errorf("cannot compare non-numeric values for %s: %v vs %v", op, actual, expected)
	}
	switch op {
	case domain.OpGt:
		return comparison > 0, nil
	case domain.OpGte:
		return comparison >= 0, nil
	case domain.OpLt:
		return comparison < 0, nil
	case domain.OpLte:
		return comparison <= 0, nil
	default:
		return false, domain.ErrInvalidOperator
	}
}

func resolvePath(path string, data json.RawMessage, facts map[string]json.RawMessage) (interface{}, error) {
	if path == "" || !strings.HasPrefix(path, "$.") {
		return nil, domain.ErrInvalidFactPath
	}
	parts := strings.Split(path[2:], ".")
	if len(parts) > 0 && parts[0] == "facts" {
		return resolveFactPath(parts[1:], facts)
	}
	return resolveEventDataPath(data, parts)
}

func resolveFactPath(parts []string, facts map[string]json.RawMessage) (interface{}, error) {
	if len(parts) < 1 {
		return nil, nil
	}
	factKey := parts[0]
	factData, exists := facts[factKey]
	if !exists {
		return nil, nil
	}
	var value interface{}
	dec := json.NewDecoder(bytes.NewReader(factData))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("unmarshal fact %s: %w", factKey, err)
	}
	return walkObjectPath(value, parts[1:]), nil
}

func resolveEventDataPath(data json.RawMessage, parts []string) (interface{}, error) {
	var value interface{}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&value); err != nil {
		return nil, fmt.Errorf("unmarshal data: %w", err)
	}
	return walkObjectPath(value, parts), nil
}

func walkObjectPath(value interface{}, parts []string) interface{} {
	current := value
	for _, part := range parts {
		if part == "" {
			continue
		}
		object, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		next, exists := object[part]
		if !exists {
			return nil
		}
		current = next
	}
	return current
}

func compareEq(actual interface{}, expected interface{}) bool {
	// Strict type equality: types must match exactly
	// For numbers, compare numerically but only if both are numeric types
	// Reject string<->number, bool<->string coercion

	// JSON numbers compare by numeric value rather than textual representation.
	if a, aok := actual.(json.Number); aok {
		if b, bok := expected.(json.Number); bok {
			return compareNumericEqual(a, b)
		}
		// json.Number vs other numeric type
		if isNumeric(expected) {
			return compareNumericEqual(actual, expected)
		}
		return false
	}

	// int64 vs int64
	if a, aok := actual.(int64); aok {
		if b, bok := expected.(int64); bok {
			return a == b
		}
		// int64 vs other numeric type
		if isNumeric(expected) {
			return compareNumericEqual(actual, expected)
		}
		return false
	}

	// float64 vs float64 (should not happen with UseNumber, but handle)
	if a, aok := actual.(float64); aok {
		if b, bok := expected.(float64); bok {
			return a == b
		}
		// float64 vs other numeric type
		if isNumeric(expected) {
			return compareNumericEqual(actual, expected)
		}
		return false
	}

	// Cross-type numeric comparison: both numeric but different types
	if isNumeric(actual) && isNumeric(expected) {
		return compareNumericEqual(actual, expected)
	}

	// For all other types, use standard equality (no coercion)
	return actual == expected
}

func isNumeric(v interface{}) bool {
	switch v.(type) {
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64,
		json.Number:
		return true
	}
	return false
}

func compareNumericEqual(a, b interface{}) bool {
	ar, aOK := numericRat(a)
	br, bOK := numericRat(b)
	return aOK && bOK && ar.Cmp(br) == 0
}

// numericRat converts supported numeric values to exact rational values where
// possible. This avoids overflow when comparing uint64 values and avoids
// silently equating distinct integers after conversion to float64.
func numericRat(value interface{}) (*big.Rat, bool) {
	var raw string
	switch n := value.(type) {
	case int:
		raw = strconv.FormatInt(int64(n), 10)
	case int8:
		raw = strconv.FormatInt(int64(n), 10)
	case int16:
		raw = strconv.FormatInt(int64(n), 10)
	case int32:
		raw = strconv.FormatInt(int64(n), 10)
	case int64:
		raw = strconv.FormatInt(n, 10)
	case uint:
		raw = strconv.FormatUint(uint64(n), 10)
	case uint8:
		raw = strconv.FormatUint(uint64(n), 10)
	case uint16:
		raw = strconv.FormatUint(uint64(n), 10)
	case uint32:
		raw = strconv.FormatUint(uint64(n), 10)
	case uint64:
		raw = strconv.FormatUint(n, 10)
	case float32:
		raw = strconv.FormatFloat(float64(n), 'g', -1, 32)
	case float64:
		raw = strconv.FormatFloat(n, 'g', -1, 64)
	case json.Number:
		raw = n.String()
	case string:
		// Preserve existing support for numeric strings in ordered comparisons.
		raw = n
	default:
		return nil, false
	}
	rat, ok := new(big.Rat).SetString(raw)
	return rat, ok
}

func compareCmp(actual interface{}, expected interface{}) int {
	a, aOK := numericRat(actual)
	b, bOK := numericRat(expected)
	if !aOK || !bOK {
		// Indicates incompatible types for numeric comparison.
		return 2
	}
	return a.Cmp(b)
}
func compareIn(val interface{}, list interface{}) bool {
	arr, ok := list.([]interface{})
	if !ok {
		return false
	}
	for _, item := range arr {
		if compareEq(val, item) {
			return true
		}
	}
	return false
}

// interpolateTemplates replaces placeholders in emitted event data.
// Supported: {{new_id}}, {{now}}, {{$.path}} (event data, nested paths allowed).
// A string that is exactly one {{$.path}} keeps the JSON type of the resolved
// value; placeholders embedded in longer strings are rendered as text.
// Returns error if any template variable cannot be resolved.
func interpolateTemplates(data json.RawMessage, event *domain.EventEnvelope, childID string, clock ports.Clock) (json.RawMessage, error) {
	var tree interface{}
	if err := json.Unmarshal(data, &tree); err != nil {
		return nil, err
	}
	ctx := templateContext{event: event, childID: childID, now: clock.Now().UTC().Format(time.RFC3339)}
	result, err := ctx.walk(tree)
	if err != nil {
		return nil, err
	}
	return json.Marshal(result)
}

var templatePlaceholder = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)

type templateContext struct {
	event   *domain.EventEnvelope
	childID string
	now     string
}

func (c templateContext) walk(v interface{}) (interface{}, error) {
	switch val := v.(type) {
	case string:
		return c.expand(val)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, item := range val {
			w, err := c.walk(item)
			if err != nil {
				return nil, err
			}
			out[k] = w
		}
		return out, nil
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, item := range val {
			w, err := c.walk(item)
			if err != nil {
				return nil, err
			}
			out[i] = w
		}
		return out, nil
	default:
		return val, nil
	}
}

func (c templateContext) lookup(expr string) (interface{}, bool) {
	switch expr {
	case "new_id":
		return c.childID, true
	case "now":
		return c.now, true
	}
	path := expr
	if !strings.HasPrefix(path, "$.") {
		path = "$." + path
	}
	if c.event == nil {
		return nil, false
	}
	val, err := resolvePath(path, c.event.Data, nil)
	if err != nil || val == nil {
		return nil, false
	}
	return val, true
}

func (c templateContext) expand(s string) (interface{}, error) {
	if !strings.Contains(s, "{{") {
		return s, nil
	}
	if m := templatePlaceholder.FindStringSubmatch(s); m != nil && m[0] == s {
		// Exact single placeholder - preserve type
		val, ok := c.lookup(m[1])
		if !ok {
			return nil, fmt.Errorf("unresolved template variable: %s", m[1])
		}
		return val, nil
	}
	// Embedded placeholders - must all resolve to strings
	var unresolved bool
	result := templatePlaceholder.ReplaceAllStringFunc(s, func(match string) string {
		val, ok := c.lookup(templatePlaceholder.FindStringSubmatch(match)[1])
		if !ok {
			unresolved = true
			return ""
		}
		if str, isStr := val.(string); isStr {
			return str
		}
		encoded, err := json.Marshal(val)
		if err != nil {
			unresolved = true
			return ""
		}
		return string(encoded)
	})
	if unresolved {
		return nil, fmt.Errorf("unresolved template variable in: %s", s)
	}
	return result, nil
}

func (e *Evaluator) buildEffect(revision *domain.RuleRevision, event *domain.EventEnvelope, ruleID string, actionIndex int, action domain.Action) (domain.Effect, error) {
	effect := domain.Effect{
		ID:          domain.ComputeEffectID(event.TenantID, event.ID, revision.RuleID, revision.Revision, ruleID, actionIndex),
		ExecutionID: "",
	}
	switch {
	case action.Emit != nil:
		effect = e.buildEmitEffect(effect, action.Emit)
	case action.EmitEvent != nil:
		var err error
		effect, err = e.buildEmitEventEffect(effect, event, ruleID, actionIndex, action.EmitEvent)
		if err != nil {
			return domain.Effect{}, err
		}
	case action.ScheduleEvent != nil:
		var err error
		effect, err = e.buildScheduleEventEffect(effect, event, ruleID, actionIndex, action.ScheduleEvent)
		if err != nil {
			return domain.Effect{}, err
		}
	case action.Command != nil:
		effect = buildCommandEffect(effect, action.Command)
	}
	effect.CreatedAt = e.clock.Now()
	return effect, nil
}

func (e *Evaluator) buildEmitEffect(effect domain.Effect, action *domain.EmitAction) domain.Effect {
	effect.Destination = action.Topic
	effect.Name = action.Topic
	effect.Payload = action.Data
	effect.EffectType = domain.EffectTypeEmit
	return effect
}

func buildCommandEffect(effect domain.Effect, action *domain.CommandAction) domain.Effect {
	effect.EffectType = domain.EffectTypeCommand
	effect.Destination = action.Destination
	effect.Name = action.Name
	effect.Payload = action.Data
	return effect
}

func (e *Evaluator) buildEmitEventEffect(
	effect domain.Effect,
	event *domain.EventEnvelope,
	ruleID string,
	actionIndex int,
	action *domain.EmitEventAction,
) (domain.Effect, error) {
	childID := domain.ComputeChildEventID(event.ID, ruleID, actionIndex)
	partitionKey, err := resolveEmitEventPartitionKey(action, event)
	if err != nil {
		return domain.Effect{}, err
	}

	childData, err := interpolateTemplates(action.Data, event, childID, e.clock)
	if err != nil {
		return domain.Effect{}, fmt.Errorf("interpolate templates: %w", err)
	}
	child := buildChildEventEnvelope(event, action, childID, partitionKey, childData, e.clock.Now())
	encoded, err := json.Marshal(child)
	if err != nil {
		return domain.Effect{}, fmt.Errorf("marshal child event: %w", err)
	}

	effect.EffectType = domain.EffectTypeEmitEvent
	effect.Destination = "events." + child.Type
	effect.Name = child.Type
	effect.Payload = encoded
	return effect, nil
}

func (e *Evaluator) buildScheduleEventEffect(
	effect domain.Effect,
	event *domain.EventEnvelope,
	ruleID string,
	actionIndex int,
	action *domain.ScheduleEventAction,
) (domain.Effect, error) {
	if action.DelaySeconds < 1 || action.DelaySeconds > domain.MaxScheduledDelaySeconds {
		return domain.Effect{}, fmt.Errorf("scheduled event delay must be between 1 and %d seconds", domain.MaxScheduledDelaySeconds)
	}
	childID := domain.ComputeChildEventID(event.ID, ruleID, actionIndex)
	partitionKey, err := resolveEmitEventPartitionKey(&domain.EmitEventAction{
		PartitionKey: action.PartitionKey, PartitionKeyPolicy: action.PartitionKeyPolicy,
		PartitionKeyPath: action.PartitionKeyPath,
	}, event)
	if err != nil {
		return domain.Effect{}, err
	}
	childData, err := interpolateTemplates(action.Data, event, childID, e.clock)
	if err != nil {
		return domain.Effect{}, fmt.Errorf("interpolate scheduled event templates: %w", err)
	}
	now := e.clock.Now().UTC()
	scheduled := domain.ScheduledEvent{
		EventID: childID, TenantID: event.TenantID, EventType: action.Type,
		PartitionKey: partitionKey, WorkflowID: event.WorkflowID, Payload: childData,
		Headers: map[string]string{
			"flowrulz_parent_event_id": event.ID,
			"flowrulz_scheduled_event_id": childID,
		},
		ScheduledAt: now.Add(time.Duration(action.DelaySeconds) * time.Second),
		Status: domain.ScheduledEventStatusPending, CreatedAt: now,
	}
	payload, err := json.Marshal(scheduled)
	if err != nil {
		return domain.Effect{}, fmt.Errorf("marshal scheduled event intent: %w", err)
	}
	effect.EffectType = domain.EffectTypeScheduleEvent
	effect.Destination = "scheduled_events"
	effect.Name = action.Type
	effect.Payload = payload
	return effect, nil
}

func resolveEmitEventPartitionKey(action *domain.EmitEventAction, event *domain.EventEnvelope) (string, error) {
	partitionKey := action.PartitionKey
	policy := action.PartitionKeyPolicy
	if policy == "" {
		policy = domain.PartitionKeyPolicyExplicit
	}
	switch policy {
	case domain.PartitionKeyPolicyInherit:
		partitionKey = event.PartitionKey
	case domain.PartitionKeyPolicyFromData:
		if action.PartitionKeyPath != "" {
			value, err := resolvePath(action.PartitionKeyPath, event.Data, nil)
			if err != nil || value == nil {
				return "", fmt.Errorf("resolve partition key from path %s: %w", action.PartitionKeyPath, err)
			}
			pk, err := formatPartitionKeyValue(value)
			if err != nil {
				return "", err
			}
			partitionKey = pk
		}
	}
	if partitionKey == "" {
		return "", fmt.Errorf("partition key resolution resulted in empty value for policy %s", policy)
	}
	return partitionKey, nil
}

// formatPartitionKeyValue formats a value as a partition key string.
// Numbers are formatted as plain decimal (no scientific notation).
// Only string and integer numbers are accepted; floats, bools, objects are rejected.
func formatPartitionKeyValue(v interface{}) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case json.Number:
		// json.Number preserves the original decimal representation
		// Reject floats (numbers with decimal point or exponent)
		if strings.ContainsAny(t.String(), ".eE") {
			return "", fmt.Errorf("partition key must be an integer, got float %s", t.String())
		}
		return t.String(), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		// Should not happen with UseNumber, but handle defensively
		if t == float64(int64(t)) {
			return strconv.FormatInt(int64(t), 10), nil
		}
		return "", fmt.Errorf("partition key must be an integer, got float %v", t)
	default:
		return "", fmt.Errorf("partition key must be string or integer, got %T", v)
	}
}

func buildChildEventEnvelope(
	parent *domain.EventEnvelope,
	action *domain.EmitEventAction,
	childID string,
	partitionKey string,
	data json.RawMessage,
	occurredAt time.Time,
) domain.EventEnvelope {
	return domain.EventEnvelope{
		ID: childID, Type: action.Type, TenantID: parent.TenantID,
		PartitionKey: partitionKey, OccurredAt: occurredAt, Data: data,
		Headers: map[string]string{
			"flowrulz_parent_event_id": parent.ID,
			"flowrulz_child_event_id":  childID,
		},
	}
}

func describeAction(action domain.Action) string {
	if action.Emit != nil {
		return "emit:" + action.Emit.Topic
	}
	if action.EmitEvent != nil {
		return "emit_event:" + action.EmitEvent.Type
	}
	if action.ScheduleEvent != nil {
		return "schedule_event:" + action.ScheduleEvent.Type
	}
	if action.Command != nil {
		return "command:" + action.Command.Destination + "/" + action.Command.Name
	}
	return "unknown"
}
