package rules

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestEvaluateTypeMismatchIsPermanent(t *testing.T) {
	revision, err := NewCompiler(DefaultLimits()).Compile(json.RawMessage(`{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "first_match",
		"rules": [{
			"id": "amount-check",
			"priority": 1,
			"when": {"path": "$.amount", "op": "gt", "value": "not-a-number"},
			"then": [{"emit": {"topic": "orders.review", "data": {}}}]
		}]
	}`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	_, err = NewEvaluator(domain.SystemClock{}).Evaluate(revision, &domain.EventEnvelope{
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

func TestEvaluateSimpleEq(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
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
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator(domain.SystemClock{})
	event := &domain.EventEnvelope{
		ID:           "evt-1",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-1",
		Data:         json.RawMessage(`{"total": 1500, "id": "order-1"}`),
	}

	decision, err := evaluator.Evaluate(revision, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(decision.MatchedRules) != 1 {
		t.Errorf("expected 1 matched rule, got %d", len(decision.MatchedRules))
	}
	if len(decision.Effects) != 1 {
		t.Errorf("expected 1 effect, got %d", len(decision.Effects))
	}
	if decision.Hash == "" {
		t.Error("expected non-empty hash")
	}
}

func TestEvaluateEmitEventCreatesChildEnvelope(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	revision, err := compiler.Compile(json.RawMessage(`{
		"rule_set":"order.created", "revision":1, "mode":"first_match",
		"rules":[{"id":"request-payment", "priority":1,
			"when":{"path":"$.total", "op":"gte", "value":1},
			"then":[{"emit_event":{"type":"payment.requested", "partition_key":"order-42", "data":{"order_id":"order-42"}}}]
		}]
	}`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	decision, err := NewEvaluator(domain.SystemClock{}).Evaluate(revision, &domain.EventEnvelope{
		ID: "evt-parent", Type: "order.created", TenantID: "acme", PartitionKey: "order-42",
		Data: json.RawMessage(`{"total": 42}`),
	}, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if len(decision.Effects) != 1 {
		t.Fatalf("effects = %d, want 1", len(decision.Effects))
	}
	effect := decision.Effects[0]
	if effect.EffectType != domain.EffectTypeEmitEvent || effect.Destination != "events.payment.requested" {
		t.Fatalf("unexpected effect: %#v", effect)
	}
	var child domain.EventEnvelope
	if err := json.Unmarshal(effect.Payload, &child); err != nil {
		t.Fatalf("decode child: %v", err)
	}
	if child.ID == "" || child.ID == "evt-parent" || child.Type != "payment.requested" || child.TenantID != "acme" || child.PartitionKey != "order-42" {
		t.Fatalf("unexpected child: %#v", child)
	}
	if child.Headers["flowrulz_parent_event_id"] != "evt-parent" {
		t.Fatalf("parent header = %q", child.Headers["flowrulz_parent_event_id"])
	}
	if child.Headers["flowrulz_child_event_id"] != child.ID {
		t.Fatalf("child_event_id header missing or mismatched: %q", child.Headers["flowrulz_child_event_id"])
	}
}

func TestEvaluateEmitEventDeterministicChildID(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	revision, err := compiler.Compile(json.RawMessage(`{
		"rule_set":"order.created", "revision":1, "mode":"first_match",
		"rules":[{"id":"request-payment", "priority":1,
			"when":{"path":"$.total", "op":"gte", "value":1},
			"then":[{"emit_event":{"type":"payment.requested", "partition_key":"order-42", "data":{"order_id":"order-42"}}}]
		}]
	}`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	env := &domain.EventEnvelope{
		ID:           "evt-parent",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-42",
		Data:         json.RawMessage(`{"total": 42}`),
	}

	decision1, _ := NewEvaluator(domain.SystemClock{}).Evaluate(revision, env, nil)
	var child1 domain.EventEnvelope
	if err := json.Unmarshal(decision1.Effects[0].Payload, &child1); err != nil {
		t.Fatalf("unmarshal child1: %v", err)
	}

	decision2, _ := NewEvaluator(domain.SystemClock{}).Evaluate(revision, env, nil)
	var child2 domain.EventEnvelope
	if err := json.Unmarshal(decision2.Effects[0].Payload, &child2); err != nil {
		t.Fatalf("unmarshal child2: %v", err)
	}

	if child1.ID != child2.ID {
		t.Errorf("child event ID not deterministic: %s != %s", child1.ID, child2.ID)
	}
	if child1.ID != domain.ComputeChildEventID("evt-parent", "request-payment", 0) {
		t.Errorf("child event ID doesn't match ComputeChildEventID: %s", child1.ID)
	}
}

func TestEvaluateNoMatch(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
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
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator(domain.SystemClock{})
	event := &domain.EventEnvelope{
		ID:           "evt-2",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-2",
		Data:         json.RawMessage(`{"total": 500, "id": "order-2"}`),
	}

	decision, err := evaluator.Evaluate(revision, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(decision.MatchedRules) != 0 {
		t.Errorf("expected 0 matched rules, got %d", len(decision.MatchedRules))
	}
	if len(decision.Effects) != 0 {
		t.Errorf("expected 0 effects, got %d", len(decision.Effects))
	}
}

func TestEvaluateAllMatches(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
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
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator(domain.SystemClock{})
	event := &domain.EventEnvelope{
		ID:           "evt-3",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-3",
		Data:         json.RawMessage(`{"total": 1500, "country": "US"}`),
	}

	decision, err := evaluator.Evaluate(revision, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}

	if len(decision.MatchedRules) != 2 {
		t.Errorf("expected 2 matched rules, got %d", len(decision.MatchedRules))
	}
	if len(decision.Effects) != 2 {
		t.Errorf("expected 2 effects, got %d", len(decision.Effects))
	}
}

func TestEvaluateDedeterministicHash(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
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
	}`)

	revision, err := compiler.Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator(domain.SystemClock{})
	event := &domain.EventEnvelope{
		ID:           "evt-4",
		Type:         "order.created",
		TenantID:     "acme",
		PartitionKey: "order-4",
		Data:         json.RawMessage(`{"total": 1500, "id": "order-4"}`),
	}

	decision1, _ := evaluator.Evaluate(revision, event, nil)
	decision2, _ := evaluator.Evaluate(revision, event, nil)

	if decision1.Hash != decision2.Hash {
		t.Errorf("expected deterministic hash: %s != %s", decision1.Hash, decision2.Hash)
	}
}

func TestCompilerInvalidMatchMode(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
		"rule_set": "order-policy",
		"revision": 1,
		"mode": "invalid",
		"rules": [{
			"id": "r1",
			"priority": 1,
			"when": {"path": "$.x", "op": "eq", "value": 1},
			"then": [{"emit": {"topic": "t", "data": {}}}]
		}]
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for invalid match mode")
	}
}

func TestCompilerDuplicateRuleIDs(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
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
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for duplicate rule IDs")
	}
}

func TestCompilerAmbiguousPriority(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
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
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected error for ambiguous priority")
	}
}

func TestCompilerRejectsEarlyOtherwiseInFirstMatch(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	source := json.RawMessage(`{
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
	}`)

	_, err := compiler.Compile(source)
	if err == nil {
		t.Fatal("expected first_match rule set with an early otherwise to be rejected")
	}
}

func TestResolvePathUsesNumber(t *testing.T) {
	// Test that large integers preserve precision when resolved from event data
	data := json.RawMessage(`{"account_id": 123456789, "large_id": 9007199254740993}`)

	val, err := resolvePath("$.account_id", data, nil)
	if err != nil {
		t.Fatalf("resolvePath error: %v", err)
	}

	// Should be json.Number, not float64
	num, ok := val.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number, got %T: %v", val, val)
	}

	if num.String() != "123456789" {
		t.Errorf("expected '123456789', got %q", num.String())
	}

	val2, err := resolvePath("$.large_id", data, nil)
	if err != nil {
		t.Fatalf("resolvePath error: %v", err)
	}

	num2, ok := val2.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number for large_id, got %T: %v", val2, val2)
	}

	if num2.String() != "9007199254740993" {
		t.Errorf("expected '9007199254740993', got %q", num2.String())
	}
}

func TestResolveEmitEventPartitionKeyFormatsNumbersCorrectly(t *testing.T) {
	// Test that partition keys from numeric data are formatted as plain decimal
	action := &domain.EmitEventAction{
		Type:               "child.event",
		PartitionKeyPolicy: domain.PartitionKeyPolicyFromData,
		PartitionKeyPath:   "$.account_id",
		Data:               json.RawMessage(`{}`),
	}

	event := &domain.EventEnvelope{
		ID:           "parent-1",
		Type:         "parent.event",
		TenantID:     "tenant-a",
		PartitionKey: "parent-key",
		Data:         json.RawMessage(`{"account_id": 123456789}`),
	}

	partitionKey, err := resolveEmitEventPartitionKey(action, event)
	if err != nil {
		t.Fatalf("resolveEmitEventPartitionKey error: %v", err)
	}

	// Should be plain decimal, not scientific notation
	if partitionKey != "123456789" {
		t.Errorf("expected '123456789', got %q", partitionKey)
	}

	// Test with large integer (above 2^53)
	event2 := &domain.EventEnvelope{
		ID:           "parent-2",
		Type:         "parent.event",
		TenantID:     "tenant-a",
		PartitionKey: "parent-key",
		Data:         json.RawMessage(`{"account_id": 9007199254740993}`),
	}

	partitionKey2, err := resolveEmitEventPartitionKey(action, event2)
	if err != nil {
		t.Fatalf("resolveEmitEventPartitionKey error: %v", err)
	}

	if partitionKey2 != "9007199254740993" {
		t.Errorf("expected '9007199254740993', got %q", partitionKey2)
	}
}

func TestCompareEqStrictTypes(t *testing.T) {
	// Test strict equality: same JSON type required
	tests := []struct {
		name     string
		actual   interface{}
		expected interface{}
		want     bool
	}{
		{"string vs number: '1' vs 1", "1", 1, false},
		{"string vs number: '1' vs json.Number", "1", json.Number("1"), false},
		{"number vs number: 1 vs 1", 1, 1, true},
		{"json.Number vs json.Number: '1' vs '1'", json.Number("1"), json.Number("1"), true},
		{"bool vs string: true vs 'true'", true, "true", false},
		{"float64 vs int: 1.0 vs 1", 1.0, 1, true}, // numeric equality
		{"large int vs same", json.Number("9007199254740993"), json.Number("9007199254740993"), true},
		{"large int vs different", json.Number("9007199254740993"), json.Number("9007199254740992"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareEq(tt.actual, tt.expected)
			if got != tt.want {
				t.Errorf("compareEq(%v, %v) = %v, want %v", tt.actual, tt.expected, got, tt.want)
			}
		})
	}
}

func TestEvalLeafMissingFieldSemantics(t *testing.T) {
	// Test missing field semantics for different operators
	// data has no "missing_field"
	data := json.RawMessage(`{"existing": "value"}`)

	tests := []struct {
		name     string
		op       domain.Operator
		expected bool // what the operator should return for missing field
	}{
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := domain.Predicate{
				Path:  "$.missing_field",
				Op:    tt.op,
				Value: "anything",
			}

			got, err := evalLeafForTest(p, data, nil)
			if err != nil {
				t.Fatalf("evalLeaf error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("operator %s on missing field = %v, want %v", tt.op, got, tt.expected)
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

func TestTemplateUnresolvedPathIsError(t *testing.T) {
	compiler := NewCompiler(DefaultLimits())
	revision, err := compiler.Compile(json.RawMessage(`{
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
	}`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	evaluator := NewEvaluator(domain.SystemClock{})
	event := &domain.EventEnvelope{
		ID:           "evt-1",
		Type:         "order.created",
		TenantID:     "tenant-a",
		PartitionKey: "key",
		Data:         json.RawMessage(`{"total": 100}`),
	}

	_, err = evaluator.Evaluate(revision, event, nil)
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

func TestEvaluateScheduleEventCreatesDurableIntent(t *testing.T) {
	source := json.RawMessage(`{
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
	}`)
	revision, err := NewCompiler(DefaultLimits()).Compile(source)
	if err != nil {
		t.Fatalf("compile schedule action: %v", err)
	}
	now := time.Now().UTC()
	decision, err := NewEvaluator(domain.SystemClock{}).Evaluate(revision, &domain.EventEnvelope{
		ID:"evt-schedule", Type:"order.created", TenantID:"tenant-a", PartitionKey:"order-7",
		WorkflowID:"wf-7", OccurredAt:now, Data:json.RawMessage(`{"order_id":"order-7"}`),
	}, nil)
	if err != nil {
		t.Fatalf("evaluate schedule action: %v", err)
	}
	if len(decision.Effects) != 1 || decision.Effects[0].EffectType != domain.EffectTypeScheduleEvent {
		t.Fatalf("effects = %#v, want one schedule_event effect", decision.Effects)
	}
	var scheduled domain.ScheduledEvent
	if err := json.Unmarshal(decision.Effects[0].Payload, &scheduled); err != nil {
		t.Fatalf("decode scheduled intent: %v", err)
	}
	if scheduled.EventID == "" || scheduled.TenantID != "tenant-a" || scheduled.EventType != "order.reminder" {
		t.Fatalf("scheduled identity = %#v", scheduled)
	}
	if scheduled.PartitionKey != "order-7" || scheduled.WorkflowID != "wf-7" {
		t.Fatalf("scheduled routing = %#v", scheduled)
	}
	if !scheduled.ScheduledAt.After(now) || scheduled.Status != domain.ScheduledEventStatusPending {
		t.Fatalf("scheduled state = %#v, want future pending event", scheduled)
	}
	var payload map[string]any
	if err := json.Unmarshal(scheduled.Payload, &payload); err != nil {
		t.Fatalf("decode scheduled payload: %v", err)
	}
	if payload["order_id"] != "order-7" {
		t.Fatalf("scheduled payload = %#v, want order_id order-7", payload)
	}
}
