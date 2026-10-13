package rules

import (
	"encoding/json"
	"fmt"
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
	if value == nil {
		return false, nil
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
	if err := json.Unmarshal(factData, &value); err != nil {
		return nil, fmt.Errorf("unmarshal fact %s: %w", factKey, err)
	}
	return walkObjectPath(value, parts[1:]), nil
}

func resolveEventDataPath(data json.RawMessage, parts []string) (interface{}, error) {
	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
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
	a := normalizeNumber(actual)
	b := normalizeNumber(expected)
	if a != nil && b != nil {
		return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
	}
	return fmt.Sprintf("%v", actual) == fmt.Sprintf("%v", expected)
}

func normalizeNumber(v interface{}) interface{} {
	switch n := v.(type) {
	case float64:
		if n == float64(int64(n)) {
			return int64(n)
		}
		return n
	case int64:
		return n
	case json.Number:
		if i, err := n.Int64(); err == nil {
			return i
		}
		if f, err := n.Float64(); err == nil {
			return f
		}
	}
	return v
}

func compareCmp(actual interface{}, expected interface{}) int {
	a, aOk := toFloat64(actual)
	b, bOk := toFloat64(expected)
	if aOk && bOk {
		if a > b {
			return 1
		}
		if a < b {
			return -1
		}
		return 0
	}
	// If either value cannot be converted to a number, return an error indicator
	// by returning a special value that will cause the comparison to fail
	// This prevents incorrect lexical comparison like "100" < "20"
	return 2 // Indicates incompatible types for numeric comparison
}

func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
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
func interpolateTemplates(data json.RawMessage, event *domain.EventEnvelope, childID string, clock ports.Clock) (json.RawMessage, error) {
	var tree interface{}
	if err := json.Unmarshal(data, &tree); err != nil {
		return nil, err
	}
	ctx := templateContext{event: event, childID: childID, now: clock.Now().UTC().Format(time.RFC3339)}
	return json.Marshal(ctx.walk(tree))
}

var templatePlaceholder = regexp.MustCompile(`\{\{\s*([^{}]+?)\s*\}\}`)

type templateContext struct {
	event   *domain.EventEnvelope
	childID string
	now     string
}

func (c templateContext) walk(v interface{}) interface{} {
	switch val := v.(type) {
	case string:
		return c.expand(val)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, item := range val {
			out[k] = c.walk(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, item := range val {
			out[i] = c.walk(item)
		}
		return out
	default:
		return val
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

func (c templateContext) expand(s string) interface{} {
	if !strings.Contains(s, "{{") {
		return s
	}
	if m := templatePlaceholder.FindStringSubmatch(s); m != nil && m[0] == s {
		if val, ok := c.lookup(m[1]); ok {
			return val
		}
		return ""
	}
	return templatePlaceholder.ReplaceAllStringFunc(s, func(match string) string {
		val, ok := c.lookup(templatePlaceholder.FindStringSubmatch(match)[1])
		if !ok {
			return ""
		}
		if str, isStr := val.(string); isStr {
			return str
		}
		encoded, err := json.Marshal(val)
		if err != nil {
			return ""
		}
		return string(encoded)
	})
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
			partitionKey = fmt.Sprintf("%v", value)
		}
	}
	if partitionKey == "" {
		return "", fmt.Errorf("partition key resolution resulted in empty value for policy %s", policy)
	}
	return partitionKey, nil
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
	if action.Command != nil {
		return "command:" + action.Command.Destination + "/" + action.Command.Name
	}
	return "unknown"
}
