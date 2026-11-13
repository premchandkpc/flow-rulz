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
	acc, err := e.evaluateRules(revision, event, facts)
	if err != nil {
		return nil, err
	}
	hash := domain.ComputeDecisionHash(revision.ContentHash, event.ID, "", acc.matched, acc.effects)
	return &domain.Decision{
		MatchedRules: acc.matched,
		Effects:      acc.effects,
		Hash:         hash,
		Explanations: acc.explanations,
	}, nil
}

// decisionAccumulator carries the growing decision state across rule evaluations.
type decisionAccumulator struct {
	matched      []string
	effects      []domain.Effect
	explanations []domain.RuleExplanation
}

// newDecisionAccumulator allocates the decision state for a rule set of the
// given size, preserving the original empty-slice (not nil) semantics.
func newDecisionAccumulator(ruleCount int) *decisionAccumulator {
	return &decisionAccumulator{
		matched:      make([]string, 0),
		effects:      make([]domain.Effect, 0),
		explanations: make([]domain.RuleExplanation, 0, ruleCount),
	}
}

// evaluateRules iterates through compiled rules, evaluates predicates, and builds effects/explanations.
func (e *Evaluator) evaluateRules(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage) (*decisionAccumulator, error) {
	acc := newDecisionAccumulator(len(revision.Compiled))
	for _, rule := range revision.Compiled {
		ok, err := e.evalRule(revision, event, facts, rule, acc)
		if err != nil {
			return nil, err
		}
		if revision.MatchMode == domain.MatchModeFirstMatch && ok {
			break
		}
	}
	return acc, nil
}

// evalRule evaluates one rule's predicate and records its result; predicate
// errors are wrapped with the rule ID and classified as evaluation errors.
func (e *Evaluator) evalRule(revision *domain.RuleRevision, event *domain.EventEnvelope, facts map[string]json.RawMessage, rule domain.CompiledRule, acc *decisionAccumulator) (bool, error) {
	ok, err := e.evalPredicate(rule.When, event.Data, facts)
	if err != nil {
		return false, fmt.Errorf("rule %s: %w: %w", rule.ID, domain.ErrEvaluationError, err)
	}
	if err := e.processRuleResult(rule, ok, acc, revision, event); err != nil {
		return false, err
	}
	return ok, nil
}

// processRuleResult handles the result of a rule evaluation (matched or not):
// it records the matched rule ID, builds the branch effects and appends the
// rule explanation.
func (e *Evaluator) processRuleResult(rule domain.CompiledRule, ok bool, acc *decisionAccumulator, revision *domain.RuleRevision, event *domain.EventEnvelope) error {
	branch, reason, label := rule.Then, "predicate matched", "action"
	if !ok {
		branch, reason, label = rule.Otherwise, "predicate did not match", "otherwise action"
	}
	if ok {
		acc.matched = append(acc.matched, rule.ID)
	}
	actions, err := e.buildActionEffects(rule.ID, branch, label, revision, event, acc)
	if err != nil {
		return err
	}
	acc.explanations = append(acc.explanations, newRuleExplanation(rule, ok, reason, actions))
	return nil
}

// buildActionEffects builds the effects for one action branch in order and
// returns the human-readable action descriptions.
func (e *Evaluator) buildActionEffects(ruleID string, branch []domain.Action, label string, revision *domain.RuleRevision, event *domain.EventEnvelope, acc *decisionAccumulator) ([]string, error) {
	actions := make([]string, 0, len(branch))
	for i, action := range branch {
		effect, err := e.buildEffect(revision, event, ruleID, i, action)
		if err != nil {
			return nil, fmt.Errorf("rule %s %s %d: %w: %w", ruleID, label, i, domain.ErrEvaluationError, err)
		}
		acc.effects = append(acc.effects, effect)
		actions = append(actions, describeAction(action))
	}
	return actions, nil
}

// newRuleExplanation builds the explanation entry for one evaluated rule.
func newRuleExplanation(rule domain.CompiledRule, matched bool, reason string, actions []string) domain.RuleExplanation {
	return domain.RuleExplanation{
		RuleID: rule.ID, RuleName: rule.Name, Matched: matched,
		Priority: rule.Priority, Reason: reason, Actions: actions,
	}
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
		return missingFieldResult(p.Op), nil
	}
	return evaluateLeafValue(p.Op, value, p.Value)
}

// missingFieldResult applies the missing-field semantics documented in evalLeaf:
// only neq and not_in are true when the resolved value is absent.
func missingFieldResult(op domain.Operator) bool {
	switch op {
	case domain.OpNeq, domain.OpNotIn:
		return true
	default:
		return false
	}
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
	return applyOrdering(op, comparison)
}

// applyOrdering maps a three-way comparison result onto the relational operator.
func applyOrdering(op domain.Operator, comparison int) (bool, error) {
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
		next, ok := stepObjectPath(current, part)
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

// stepObjectPath advances one path segment, reporting false when the segment
// cannot be resolved (empty segments keep the current value).
func stepObjectPath(current interface{}, part string) (interface{}, bool) {
	if part == "" {
		return current, true
	}
	object, ok := current.(map[string]interface{})
	if !ok {
		return nil, false
	}
	next, exists := object[part]
	if !exists {
		return nil, false
	}
	return next, true
}

// compareEq enforces strict type equality: types must match exactly, numbers
// compare numerically only when both sides are numeric, and there is no
// string<->number or bool<->string coercion.
func compareEq(actual interface{}, expected interface{}) bool {
	if same, ok := sameTypeNumericEqual(actual, expected); ok {
		return same
	}
	// Cross-type numeric comparison: both numeric but different types.
	if isNumeric(actual) && isNumeric(expected) {
		return compareNumericEqual(actual, expected)
	}
	// For all other types, use standard equality (no coercion).
	return actual == expected
}

// sameTypeNumericEqual applies the fast paths for json.Number, int64 and
// float64 values compared against the same concrete type. handled=false means
// the fast path does not apply and the caller must decide.
func sameTypeNumericEqual(actual interface{}, expected interface{}) (same bool, handled bool) {
	switch a := actual.(type) {
	case json.Number:
		if b, ok := expected.(json.Number); ok {
			// JSON numbers compare by numeric value rather than textual representation.
			return compareNumericEqual(a, b), true
		}
	case int64:
		if b, ok := expected.(int64); ok {
			return a == b, true
		}
	case float64:
		if b, ok := expected.(float64); ok {
			return a == b, true
		}
	}
	return false, false
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
	raw, ok := numericString(value)
	if !ok {
		return nil, false
	}
	return new(big.Rat).SetString(raw)
}

// numericString formats a supported numeric value as an exact decimal string;
// json.Number keeps its original representation and strings pass through
// unchanged to preserve numeric-string support in ordered comparisons.
func numericString(value interface{}) (string, bool) {
	switch n := value.(type) {
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", n), true
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", n), true
	case float32:
		return strconv.FormatFloat(float64(n), 'g', -1, 32), true
	case float64:
		return strconv.FormatFloat(n, 'g', -1, 64), true
	case json.Number:
		return n.String(), true
	case string:
		return n, true
	}
	return "", false
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
		return c.walkMap(val)
	case []interface{}:
		return c.walkSlice(val)
	default:
		return val, nil
	}
}

// walkMap interpolates every value of an object, preserving unknown keys.
func (c templateContext) walkMap(val map[string]interface{}) (interface{}, error) {
	out := make(map[string]interface{}, len(val))
	for k, item := range val {
		w, err := c.walk(item)
		if err != nil {
			return nil, err
		}
		out[k] = w
	}
	return out, nil
}

// walkSlice interpolates every element of an array, preserving its order.
func (c templateContext) walkSlice(val []interface{}) (interface{}, error) {
	out := make([]interface{}, len(val))
	for i, item := range val {
		w, err := c.walk(item)
		if err != nil {
			return nil, err
		}
		out[i] = w
	}
	return out, nil
}

func (c templateContext) lookup(expr string) (interface{}, bool) {
	if val, ok := c.lookupBuiltin(expr); ok {
		return val, true
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

// lookupBuiltin resolves the template variables that do not come from event data.
func (c templateContext) lookupBuiltin(expr string) (interface{}, bool) {
	switch expr {
	case "new_id":
		return c.childID, true
	case "now":
		return c.now, true
	}
	return nil, false
}

func (c templateContext) expand(s string) (interface{}, error) {
	if !strings.Contains(s, "{{") {
		return s, nil
	}
	if m := templatePlaceholder.FindStringSubmatch(s); m != nil && m[0] == s {
		return c.expandExact(m[1])
	}
	return c.expandEmbedded(s)
}

// expandExact resolves a placeholder that makes up the entire string,
// preserving the JSON type of the resolved value.
func (c templateContext) expandExact(expr string) (interface{}, error) {
	val, ok := c.lookup(expr)
	if !ok {
		return nil, fmt.Errorf("unresolved template variable: %s", expr)
	}
	return val, nil
}

// expandEmbedded renders placeholders embedded in a longer string: every
// placeholder must resolve and non-string values are JSON-encoded.
func (c templateContext) expandEmbedded(s string) (interface{}, error) {
	var unresolved bool
	result := templatePlaceholder.ReplaceAllStringFunc(s, func(match string) string {
		rendered, ok := c.renderPlaceholder(match)
		if !ok {
			unresolved = true
			return ""
		}
		return rendered
	})
	if unresolved {
		return nil, fmt.Errorf("unresolved template variable in: %s", s)
	}
	return result, nil
}

// renderPlaceholder resolves one embedded placeholder; ok=false reports an
// unresolved variable or a value that cannot be JSON-encoded.
func (c templateContext) renderPlaceholder(match string) (string, bool) {
	val, ok := c.lookup(templatePlaceholder.FindStringSubmatch(match)[1])
	if !ok {
		return "", false
	}
	if str, isStr := val.(string); isStr {
		return str, true
	}
	encoded, err := json.Marshal(val)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func (e *Evaluator) buildEffect(revision *domain.RuleRevision, event *domain.EventEnvelope, ruleID string, actionIndex int, action domain.Action) (domain.Effect, error) {
	effect, err := e.applyActionEffect(baseEffect(revision, event, ruleID, actionIndex), event, ruleID, actionIndex, action)
	if err != nil {
		return domain.Effect{}, err
	}
	effect.CreatedAt = e.clock.Now()
	return effect, nil
}

// baseEffect creates the effect shell with its deterministic effect ID.
func baseEffect(revision *domain.RuleRevision, event *domain.EventEnvelope, ruleID string, actionIndex int) domain.Effect {
	return domain.Effect{
		ID:          domain.ComputeEffectID(event.TenantID, event.ID, revision.RuleID, revision.Revision, ruleID, actionIndex),
		ExecutionID: "",
	}
}

// applyActionEffect dispatches on the action payload to build the effect body.
func (e *Evaluator) applyActionEffect(effect domain.Effect, event *domain.EventEnvelope, ruleID string, actionIndex int, action domain.Action) (domain.Effect, error) {
	switch {
	case action.Emit != nil:
		return e.buildEmitEffect(effect, action.Emit), nil
	case action.EmitEvent != nil:
		return e.buildEmitEventEffect(effect, event, ruleID, actionIndex, action.EmitEvent)
	case action.ScheduleEvent != nil:
		return e.buildScheduleEventEffect(effect, event, ruleID, actionIndex, action.ScheduleEvent)
	case action.Command != nil:
		return buildCommandEffect(effect, action.Command), nil
	}
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

// childEventInputs carries the resolved identity, routing and payload shared
// by emit_event and schedule_event child-event actions.
type childEventInputs struct {
	childID      string
	partitionKey string
	data         json.RawMessage
}

// resolveChildEventInputs computes the deterministic child event ID, resolves
// the partition key and interpolates the child payload templates. The
// interpolation error is wrapped with interpLabel to preserve each action's
// error wording.
func (e *Evaluator) resolveChildEventInputs(event *domain.EventEnvelope, ruleID string, actionIndex int, keyAction *domain.EmitEventAction, data json.RawMessage, interpLabel string) (childEventInputs, error) {
	childID := domain.ComputeChildEventID(event.TenantID, event.ID, ruleID, actionIndex)
	partitionKey, err := resolveEmitEventPartitionKey(keyAction, event)
	if err != nil {
		return childEventInputs{}, err
	}
	childData, err := interpolateTemplates(data, event, childID, e.clock)
	if err != nil {
		return childEventInputs{}, fmt.Errorf("%s: %w", interpLabel, err)
	}
	return childEventInputs{childID: childID, partitionKey: partitionKey, data: childData}, nil
}

func (e *Evaluator) buildEmitEventEffect(effect domain.Effect, event *domain.EventEnvelope, ruleID string, actionIndex int, action *domain.EmitEventAction) (domain.Effect, error) {
	inputs, err := e.resolveChildEventInputs(event, ruleID, actionIndex, action, action.Data, "interpolate templates")
	if err != nil {
		return domain.Effect{}, err
	}
	child := buildChildEventEnvelope(event, action, inputs.childID, inputs.partitionKey, inputs.data, e.clock.Now())
	encoded, err := json.Marshal(child)
	if err != nil {
		return domain.Effect{}, fmt.Errorf("marshal child event: %w", err)
	}
	return applyChildEventEffect(effect, child, encoded), nil
}

// applyChildEventEffect sets the emit_event effect fields from the encoded child envelope.
func applyChildEventEffect(effect domain.Effect, child domain.EventEnvelope, encoded json.RawMessage) domain.Effect {
	effect.EffectType = domain.EffectTypeEmitEvent
	effect.Destination = "events." + child.Type
	effect.Name = child.Type
	effect.Payload = encoded
	return effect
}

// buildScheduleEventEffect validates the delay, resolves the child inputs and
// encodes the durable scheduled-event intent.
func (e *Evaluator) buildScheduleEventEffect(effect domain.Effect, event *domain.EventEnvelope, ruleID string, actionIndex int, action *domain.ScheduleEventAction) (domain.Effect, error) {
	if err := validateScheduleDelay(action.DelaySeconds); err != nil {
		return domain.Effect{}, err
	}
	keyAction := schedulePartitionKeyAction(action)
	inputs, err := e.resolveChildEventInputs(event, ruleID, actionIndex, keyAction, action.Data, "interpolate scheduled event templates")
	if err != nil {
		return domain.Effect{}, err
	}
	payload, err := json.Marshal(buildScheduledEvent(event, action, inputs, e.clock.Now().UTC()))
	if err != nil {
		return domain.Effect{}, fmt.Errorf("marshal scheduled event intent: %w", err)
	}
	return applyScheduledEventEffect(effect, action.Type, payload), nil
}

// validateScheduleDelay rejects delays outside the supported window.
func validateScheduleDelay(delaySeconds int64) error {
	if delaySeconds < 1 || delaySeconds > domain.MaxScheduledDelaySeconds {
		return fmt.Errorf("scheduled event delay must be between 1 and %d seconds", domain.MaxScheduledDelaySeconds)
	}
	return nil
}

// schedulePartitionKeyAction adapts a schedule_event action's routing fields to
// the shared partition-key policy validation.
func schedulePartitionKeyAction(action *domain.ScheduleEventAction) *domain.EmitEventAction {
	return &domain.EmitEventAction{
		PartitionKey: action.PartitionKey, PartitionKeyPolicy: action.PartitionKeyPolicy, PartitionKeyPath: action.PartitionKeyPath,
	}
}

// buildScheduledEvent assembles the pending scheduled-event intent with its
// deterministic child identity and parent/scheduling headers.
func buildScheduledEvent(event *domain.EventEnvelope, action *domain.ScheduleEventAction, inputs childEventInputs, now time.Time) domain.ScheduledEvent {
	return domain.ScheduledEvent{
		EventID: inputs.childID, TenantID: event.TenantID, EventType: action.Type,
		PartitionKey: inputs.partitionKey, WorkflowID: event.WorkflowID, Payload: inputs.data,
		Headers: map[string]string{
			"flowrulz_parent_event_id":    event.ID,
			"flowrulz_scheduled_event_id": inputs.childID,
			"flowrulz_correlation_id":     event.CorrelationID,
		},
		ScheduledAt: now.Add(time.Duration(action.DelaySeconds) * time.Second),
		Status:      domain.ScheduledEventStatusPending, CreatedAt: now,
	}
}

// applyScheduledEventEffect sets the schedule_event effect fields.
func applyScheduledEventEffect(effect domain.Effect, actionType string, payload json.RawMessage) domain.Effect {
	effect.EffectType = domain.EffectTypeScheduleEvent
	effect.Destination = "scheduled_events"
	effect.Name = actionType
	effect.Payload = payload
	return effect
}

func resolveEmitEventPartitionKey(action *domain.EmitEventAction, event *domain.EventEnvelope) (string, error) {
	partitionKey, policy, err := resolvePartitionKeyRouting(action, event)
	if err != nil {
		return "", err
	}
	if partitionKey == "" {
		return "", fmt.Errorf("partition key resolution resulted in empty value for policy %s", policy)
	}
	return partitionKey, nil
}

// resolvePartitionKeyRouting applies the partition-key policy (defaulting an
// empty policy to explicit) and returns the effective key with its policy.
func resolvePartitionKeyRouting(action *domain.EmitEventAction, event *domain.EventEnvelope) (string, domain.PartitionKeyPolicy, error) {
	policy := effectivePartitionKeyPolicy(action.PartitionKeyPolicy)
	key := action.PartitionKey
	switch policy {
	case domain.PartitionKeyPolicyInherit:
		key = event.PartitionKey
	case domain.PartitionKeyPolicyFromData:
		if action.PartitionKeyPath != "" {
			fromData, err := partitionKeyFromDataPath(action.PartitionKeyPath, event)
			if err != nil {
				return "", policy, err
			}
			key = fromData
		}
	}
	return key, policy, nil
}

// partitionKeyFromDataPath resolves a partition key from event data and
// formats it as a plain decimal/string key.
func partitionKeyFromDataPath(path string, event *domain.EventEnvelope) (string, error) {
	value, err := resolvePath(path, event.Data, nil)
	if err != nil {
		return "", fmt.Errorf("resolve partition key from path %s: %w", path, err)
	}
	if value == nil {
		return "", fmt.Errorf("resolve partition key from path %s: value is missing", path)
	}
	return formatPartitionKeyValue(value)
}

// formatPartitionKeyValue formats a value as a partition key string.
// Numbers are formatted as plain decimal (no scientific notation).
// Only string and integer numbers are accepted; floats, bools, objects are rejected.
func formatPartitionKeyValue(v interface{}) (string, error) {
	switch t := v.(type) {
	case string:
		return t, nil
	case json.Number:
		return formatJSONNumberPartitionKey(t)
	case int64:
		return strconv.FormatInt(t, 10), nil
	case float64:
		return formatFloatPartitionKey(t)
	default:
		return "", fmt.Errorf("partition key must be string or integer, got %T", v)
	}
}

// formatJSONNumberPartitionKey keeps json.Number's original decimal
// representation and rejects floats (numbers with a decimal point or exponent).
func formatJSONNumberPartitionKey(t json.Number) (string, error) {
	if strings.ContainsAny(t.String(), ".eE") {
		return "", fmt.Errorf("partition key must be an integer, got float %s", t.String())
	}
	return t.String(), nil
}

// formatFloatPartitionKey accepts only float64 values that are exact integers;
// it exists defensively because event data normally decodes to json.Number.
func formatFloatPartitionKey(t float64) (string, error) {
	if t == float64(int64(t)) {
		return strconv.FormatInt(int64(t), 10), nil
	}
	return "", fmt.Errorf("partition key must be an integer, got float %v", t)
}

func buildChildEventEnvelope(parent *domain.EventEnvelope, action *domain.EmitEventAction, childID string, partitionKey string, data json.RawMessage, occurredAt time.Time) domain.EventEnvelope {
	return domain.EventEnvelope{
		ID: childID, Type: action.Type, TenantID: parent.TenantID,
		PartitionKey: partitionKey, WorkflowID: parent.WorkflowID,
		CorrelationID: parent.CorrelationID, CausationID: parent.ID,
		OccurredAt: occurredAt, Data: data,
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
