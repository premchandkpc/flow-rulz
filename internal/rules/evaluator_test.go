package rules

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

const typeMismatchRuleSource = `{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [{
			"id": "amount-check",
			"priority": 1,
			"when": {"path": "$.amount", "op": "gt", "value": "not-a-number"},
			"then": [{"emit": {"topic": "orders.review", "data": {}}}]
		}]
	}`

func TestEvaluateTypeMismatchIsPermanent(t *testing.T) {
	revision := mustCompile(t, typeMismatchRuleSource)

	_, err := NewEvaluator(domain.SystemClock{}).Evaluate(revision, &domain.EventEnvelope{
		ID: "evt-type-mismatch", Type: "order.created", TenantID: "tenant-a",
		PartitionKey: "order-1", Data: json.RawMessage(`{"amount": 100}`),
	}, nil)
	if err == nil {
		t.Fatal("Evaluate() error = nil, want deterministic type mismatch")
	}
	if !errors.Is(err, domain.ErrEvaluationError) {
		t.Fatalf("Evaluate() error = %v, want ErrEvaluationError", err)
	}
	if !domain.IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = false, want true", err)
	}
}

const highValueRuleSource = `{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [{
			"id": "high-value",
			"priority": 100,
			"when": {
				"path": "$.total",
				"op": "gte",
				"value": 1000
			},
			"then": [{
				"emit": {
					"topic": "orders.review",
					"data": {"order_id": "$.id"}
				}
			}]
		}]
	}`

func TestEvaluateSimpleEq(t *testing.T) {
	revision := mustCompile(t, highValueRuleSource)

	event := testEvent("evt-1", "acme", "order-1", json.RawMessage(`{"total": 1500, "id": "order-1"}`))

	decision := mustEvaluate(t, revision, event)

	assertMatchedRules(t, decision, "high-value")
	assertEffectsCount(t, decision, 1)
	if decision.Hash == "" {
		t.Error("expected non-empty hash")
	}
}

const paymentRequestRuleSource = `{
		"rule_set":"order.created", "revision":1, "mode":"first_match",
		"rules":[{"id":"request-payment", "priority":1,
			"when":{"path":"$.total", "op":"gte", "value":1},
			"then":[{"emit_event":{"type":"payment.requested", "partition_key":"order-42", "data":{"order_id":"order-42"}}}]
		}]
	}`

func TestEvaluateEmitEventCreatesChildEnvelope(t *testing.T) {
	revision := mustCompile(t, paymentRequestRuleSource)
	decision := mustEvaluate(t, revision, testEvent("evt-parent", "acme", "order-42", json.RawMessage(`{"total":42}`)))
	assertEffectsCount(t, decision, 1)
	effect := decision.Effects[0]
	if effect.EffectType != domain.EffectTypeEmitEvent || effect.Destination != "events.payment.requested" {
		t.Fatalf("unexpected effect: %#v", effect)
	}
	assertPaymentChild(t, decodeChildEffect(t, effect))
}

func TestEvaluateEmitEventDeterministicChildID(t *testing.T) {
	revision := mustCompile(t, paymentRequestRuleSource)
	env := testEvent("evt-parent", "acme", "order-42", json.RawMessage(`{"total":42}`))
	env.WorkflowID, env.CorrelationID = "wf-42", "corr-42"
	child1 := evaluateChild(t, revision, env)
	child2 := evaluateChild(t, revision, env)
	if child1.ID != child2.ID {
		t.Errorf("child event ID not deterministic: %s != %s", child1.ID, child2.ID)
	}
	if child1.ID != domain.ComputeChildEventID("acme", "evt-parent", "request-payment", 0) {
		t.Errorf("child event ID doesn't match ComputeChildEventID: %s", child1.ID)
	}
	assertChildCorrelation(t, child1, env)
}

func TestEvaluateNoMatch(t *testing.T) {
	revision := mustCompile(t, highValueRuleSource)

	event := testEvent("evt-2", "acme", "order-2", json.RawMessage(`{"total": 500, "id": "order-2"}`))

	decision := mustEvaluate(t, revision, event)

	assertMatchedRules(t, decision)
	assertEffectsCount(t, decision, 0)
}

const allMatchesRuleSource = `{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "all_matches",
		"rules": [
			{
				"id": "high-value",
				"priority": 100,
				"when": {
					"path": "$.total",
					"op": "gte",
					"value": 1000
				},
				"then": [{
					"emit": {
						"topic": "orders.review",
						"data": {"reason": "high_value"}
					}
				}]
			},
			{
				"id": "us-order",
				"priority": 50,
				"when": {
					"path": "$.country",
					"op": "eq",
					"value": "US"
				},
				"then": [{
					"emit": {
						"topic": "orders.us",
						"data": {"reason": "us_order"}
					}
				}]
			}
		]
	}`

func TestEvaluateAllMatches(t *testing.T) {
	revision := mustCompile(t, allMatchesRuleSource)

	event := testEvent("evt-3", "acme", "order-3", json.RawMessage(`{"total": 1500, "country": "US"}`))

	decision := mustEvaluate(t, revision, event)

	assertMatchedRules(t, decision, "high-value", "us-order")
	assertEffectsCount(t, decision, 2)
}

func TestEvaluateDedeterministicHash(t *testing.T) {
	revision := mustCompile(t, highValueRuleSource)

	evaluator := NewEvaluator(domain.SystemClock{})
	event := testEvent("evt-4", "acme", "order-4", json.RawMessage(`{"total": 1500, "id": "order-4"}`))

	decision1, _ := evaluator.Evaluate(revision, event, nil)
	decision2, _ := evaluator.Evaluate(revision, event, nil)

	if decision1.Hash != decision2.Hash {
		t.Errorf("expected deterministic hash: %s != %s", decision1.Hash, decision2.Hash)
	}
}

const invalidMatchModeSource = `{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "invalid",
		"rules": [{
			"id": "r1",
			"priority": 1,
			"when": {"path": "$.x", "op": "eq", "value": 1},
			"then": [{"emit": {"topic": "t", "data": {}}}]
		}]
	}`

func TestCompilerInvalidMatchMode(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(invalidMatchModeSource)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for invalid match mode")
	}
}

const duplicateRuleIDsSource = `{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [
			{
				"id": "same-id",
				"priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 1},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			},
			{
				"id": "same-id",
				"priority": 2,
				"when": {"path": "$.x", "op": "eq", "value": 2},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			}
		]
	}`

func TestCompilerDuplicateRuleIDs(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(duplicateRuleIDsSource)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for duplicate rule IDs")
	}
}

const ambiguousPrioritySource = `{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [
			{
				"id": "r1",
				"priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 1},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			},
			{
				"id": "r2",
				"priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 2},
				"then": [{"emit": {"topic": "t", "data": {}}}]
			}
		]
	}`

func TestCompilerAmbiguousPriority(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(ambiguousPrioritySource)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for ambiguous priority")
	}
}

const earlyOtherwiseSource = `{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [
			{
				"id": "high-priority", "priority": 100,
				"when": {"path": "$.x", "op": "eq", "value": 1},
				"then": [{"emit": {"topic": "matched", "data": {}}}],
				"otherwise": [{"emit": {"topic": "unexpected", "data": {}}}]
			},
			{
				"id": "fallback", "priority": 1,
				"when": {"path": "$.x", "op": "eq", "value": 2},
				"then": [{"emit": {"topic": "fallback-match", "data": {}}}],
				"otherwise": [{"emit": {"topic": "fallback", "data": {}}}]
			}
		]
	}`

func TestCompilerRejectsEarlyOtherwiseInFirstMatch(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(earlyOtherwiseSource)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected first_match rule set with an early otherwise to be rejected")
	}
}

func TestResolvePathUsesNumber(t *testing.T) {
	data := json.RawMessage(`{"account_id":123456789,"large_id":9007199254740993}`)
	assertResolvedNumber(t, data, "$.account_id", "123456789")
	assertResolvedNumber(t, data, "$.large_id", "9007199254740993")
}

func TestResolveEmitEventPartitionKeyFormatsNumbersCorrectly(t *testing.T) {
	for _, number := range []string{"123456789", "9007199254740993"} {
		t.Run(number, func(t *testing.T) {
			action := &domain.EmitEventAction{Type: "child.event", PartitionKeyPolicy: domain.PartitionKeyPolicyFromData, PartitionKeyPath: "$.account_id", Data: json.RawMessage(`{}`)}
			event := &domain.EventEnvelope{ID: "parent-1", Type: "parent.event", TenantID: "tenant-a", PartitionKey: "parent-key", Data: json.RawMessage(`{"account_id":` + number + `}`)}
			key, err := resolveEmitEventPartitionKey(action, event)
			if err != nil || key != number {
				t.Fatalf("resolveEmitEventPartitionKey() = %q, %v, want %q", key, err, number)
			}
		})
	}
}

type strictEqualityCase struct {
	name             string
	actual, expected any
	want             bool
}

func strictEqualityCases() []strictEqualityCase {
	return []strictEqualityCase{
		{"string vs number: '1' vs 1", "1", 1, false},
		{"string vs number: '1' vs json.Number", "1", json.Number("1"), false},
		{"number vs number: 1 vs 1", 1, 1, true},
		{"json.Number vs json.Number: '1' vs '1'", json.Number("1"), json.Number("1"), true},
		{"bool vs string: true vs 'true'", true, "true", false},
		{"float64 vs int: 1.0 vs 1", 1.0, 1, true}, // numeric equality
		{"large int vs same", json.Number("9007199254740993"), json.Number("9007199254740993"), true},
		{"large int vs different", json.Number("9007199254740993"), json.Number("9007199254740992"), false},
	}
}

func TestCompareEqStrictTypes(t *testing.T) {
	for _, test := range strictEqualityCases() {
		t.Run(test.name, func(t *testing.T) {
			if got := compareEq(test.actual, test.expected); got != test.want {
				t.Errorf("compareEq(%v, %v) = %v, want %v", test.actual, test.expected, got, test.want)
			}
		})
	}
}

type missingFieldCase struct {
	name     string
	op       domain.Operator
	expected bool
}

func missingFieldCases() []missingFieldCase {
	return []missingFieldCase{
		{"eq missing", domain.OpEq, false},
		{"neq missing", domain.OpNeq, true}, // not equal to anything (including missing)
		{"in missing", domain.OpIn, false},
		{"not_in missing", domain.OpNotIn, true}, // not in list
		{"gt missing", domain.OpGt, false},
		{"gte missing", domain.OpGte, false},
		{"lt missing", domain.OpLt, false},
		{"lte missing", domain.OpLte, false},
		{"exists missing", domain.OpExists, false},
		{"not_exists missing", domain.OpNotExists, true},
	}
}

func TestEvalLeafMissingFieldSemantics(t *testing.T) {
	for _, test := range missingFieldCases() {
		t.Run(test.name, func(t *testing.T) {
			predicate := domain.Predicate{Path: "$.missing_field", Op: test.op, Value: "anything"}
			got, err := evalLeafForTest(predicate, json.RawMessage(`{"existing":"value"}`), nil)
			if err != nil {
				t.Fatalf("evalLeaf error: %v", err)
			}
			if got != test.expected {
				t.Errorf("operator %s on missing field = %v, want %v", test.op, got, test.expected)
			}
		})
	}
}

func TestEvalLeafWrongTypeIsError(t *testing.T) {
	// Type mismatch (e.g., "abc" > 5) should return an error, not false
	data := json.RawMessage(`{"field": "abc"}`)
	p := domain.Predicate{
		Path:  "$.field",
		Op:    domain.OpGt,
		Value: 5,
	}

	_, err := evalLeafForTest(p, data, nil)
	if err == nil {
		t.Fatal("expected error for type mismatch, got nil")
	}
	if !strings.Contains(err.Error(), "cannot compare") {
		t.Errorf("expected 'cannot compare' error, got: %v", err)
	}
}

const unresolvedTemplateSource = `{
		"rule_set": "order.created", "revision": 1, "mode": "first_match",
		"rules": [{
			"id": "r1", "priority": 1,
			"when": {"path": "$.total", "op": "gte", "value": 1},
			"then": [{
				"emit_event": {
					"type": "child.event",
					"partition_key": "key",
					"data": {"ref": "{{$.nonexistent_field}}"}
				}
			}]
		}]
	}`

func TestTemplateUnresolvedPathIsError(t *testing.T) {
	revision := mustCompile(t, unresolvedTemplateSource)

	evaluator := NewEvaluator(domain.SystemClock{})
	event := testEvent("evt-1", "tenant-a", "key", json.RawMessage(`{"total": 100}`))

	_, err := evaluator.Evaluate(revision, event, nil)
	if err == nil {
		t.Fatal("expected error for unresolved template path, got nil")
	}
	if !strings.Contains(err.Error(), "unresolved template") {
		t.Errorf("expected 'unresolved template' error, got: %v", err)
	}
}

// Helper to call unexported evalLeaf for testing
func evalLeafForTest(p domain.Predicate, data json.RawMessage, facts map[string]json.RawMessage) (bool, error) {
	e := &Evaluator{clock: domain.SystemClock{}}
	return e.evalLeaf(p, data, facts)
}

func TestCompareNumericEqualHandlesUint64WithoutOverflow(t *testing.T) {
	const maxUint64 = ^uint64(0)
	if !compareNumericEqual(maxUint64, maxUint64) {
		t.Fatal("equal maximum uint64 values should compare equal")
	}
	if compareNumericEqual(maxUint64, maxUint64-1) {
		t.Fatal("distinct maximum-range uint64 values must not compare equal")
	}
}

func TestCompareNumericEqualPreservesLargeJSONIntegerPrecision(t *testing.T) {
	if compareNumericEqual(json.Number("9007199254740992"), json.Number("9007199254740993")) {
		t.Fatal("distinct integers above float64's exact range must not compare equal")
	}
	if !compareNumericEqual(json.Number("1.0"), json.Number("1")) {
		t.Fatal("equivalent JSON numeric values should compare equal")
	}
}

const scheduleReminderSource = `{
		"rule_set":"order.created",
		"revision":1,
		"mode":"first_match",
		"rules":[{
			"id":"schedule-reminder",
			"priority":1,
			"when":{"path":"$.order_id","op":"exists"},
			"then":[{
				"schedule_event":{
					"type":"order.reminder",
					"delay_seconds":60,
					"partition_key_policy":"inherit",
					"data":{"order_id":"{{$.order_id}}"}
				}
			}]
		}]
	}`

func TestEvaluateScheduleEventCreatesDurableIntent(t *testing.T) {
	scheduled, now := evaluateScheduledReminder(t)
	if scheduled.EventID == "" || scheduled.TenantID != "tenant-a" || scheduled.EventType != "order.reminder" {
		t.Fatalf("scheduled identity = %#v", scheduled)
	}
	if scheduled.PartitionKey != "order-7" || scheduled.WorkflowID != "wf-7" || scheduled.Headers["flowrulz_correlation_id"] != "corr-7" {
		t.Fatalf("scheduled routing = %#v", scheduled)
	}
	if !scheduled.ScheduledAt.After(now) || scheduled.Status != domain.ScheduledEventStatusPending {
		t.Fatalf("scheduled state = %#v, want future pending event", scheduled)
	}
	payload := decodePayloadMap(t, scheduled.Payload)
	if payload["order_id"] != "order-7" {
		t.Fatalf("scheduled payload = %#v, want order_id order-7", payload)
	}
}

func TestResolveEmitEventPartitionKeyMissingPathReturnsUsefulError(t *testing.T) {
	_, err := resolveEmitEventPartitionKey(&domain.EmitEventAction{
		PartitionKeyPolicy: domain.PartitionKeyPolicyFromData,
		PartitionKeyPath:   "$.missing",
	}, &domain.EventEnvelope{Data: json.RawMessage(`{}`)})
	if err == nil {
		t.Fatal("expected missing partition key path to fail")
	}
	if strings.Contains(err.Error(), "%!w(<nil>)") {
		t.Fatalf("error contains malformed nil wrapping: %v", err)
	}
}
