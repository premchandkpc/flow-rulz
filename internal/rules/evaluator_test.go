package rules

import (
	"encoding/json"
	"errors"
	"testing"

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
