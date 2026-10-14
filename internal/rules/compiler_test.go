package rules

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestCompileAcceptsEmitEventAction(t *testing.T) {
	source := json.RawMessage(`{"rule_set":"order.created","revision":1,"mode":"first_match","rules":[{"id":"create-payment","priority":10,"when":{"path":"$.total","op":"gte","value":1},"then":[{"emit_event":{"type":"payment.requested","partition_key_policy":"inherit","data":{"amount":100}}}]}]}`)
	compiler := NewCompiler(DefaultLimits())

	if _, err := compiler.Compile(source); err != nil {
		t.Fatalf("Compile returned error for emit_event action: %v", err)
	}
}

func TestCompileRejectsMultipleCompositePredicateOperators(t *testing.T) {
	source := json.RawMessage(`{"rule_set":"invalid.rules","revision":1,"mode":"first_match","rules":[{"id":"ambiguous","priority":10,"when":{"all":[{"path":"$.x","op":"eq","value":1}],"any":[{"path":"$.y","op":"eq","value":2}]},"then":[{"emit":{"topic":"test.event","data":{}}}]}]}`)
	compiler := NewCompiler(DefaultLimits())

	if _, err := compiler.Compile(source); !errors.Is(err, domain.ErrInvalidPredicate) {
		t.Fatalf("Compile error = %v, want ErrInvalidPredicate", err)
	}
}

func TestCompileRejectsEmptyCompositePredicate(t *testing.T) {
	source := json.RawMessage(`{"rule_set":"invalid.rules","revision":1,"mode":"first_match","rules":[{"id":"empty","priority":10,"when":{"all":[]},"then":[{"emit":{"topic":"test.event","data":{}}}]}]}`)
	compiler := NewCompiler(DefaultLimits())

	if _, err := compiler.Compile(source); !errors.Is(err, domain.ErrInvalidPredicate) {
		t.Fatalf("Compile error = %v, want ErrInvalidPredicate", err)
	}
}

func TestCompileEnforcesPredicateBudgetAcrossRuleSet(t *testing.T) {
	source := json.RawMessage(`{"rule_set":"limited.rules","revision":1,"mode":"all_matches","rules":[{"id":"first","priority":20,"when":{"path":"$.first","op":"exists"},"then":[{"emit":{"topic":"first.event","data":{}}}]},{"id":"second","priority":10,"when":{"path":"$.second","op":"exists"},"then":[{"emit":{"topic":"second.event","data":{}}}]}]}`)
	limits := DefaultLimits()
	limits.MaxPredicates = 1

	if _, err := NewCompiler(limits).Compile(source); !errors.Is(err, domain.ErrRuleExceedsLimits) {
		t.Fatalf("Compile error = %v, want ErrRuleExceedsLimits for aggregate predicate budget", err)
	}
}

func TestCompileEnforcesActionBudgetAcrossRuleSet(t *testing.T) {
	source := json.RawMessage(`{"rule_set":"limited.rules","revision":1,"mode":"all_matches","rules":[{"id":"first","priority":20,"when":{"path":"$.first","op":"exists"},"then":[{"emit":{"topic":"first.event","data":{}}}]},{"id":"second","priority":10,"when":{"path":"$.second","op":"exists"},"then":[{"emit":{"topic":"second.event","data":{}}}]}]}`)
	limits := DefaultLimits()
	limits.MaxActionsPerSet = 1

	if _, err := NewCompiler(limits).Compile(source); !errors.Is(err, domain.ErrRuleExceedsLimits) {
		t.Fatalf("Compile error = %v, want ErrRuleExceedsLimits for aggregate action budget", err)
	}
}

func TestCompileRejectsFactsPaths(t *testing.T) {
	source := json.RawMessage(`{"rule_set":"test","revision":1,"mode":"first_match","rules":[{"id":"r1","priority":1,"when":{"path":"$.facts.customer_tier","op":"eq","value":"gold"},"then":[{"emit":{"topic":"test","data":{}}}]},{"id":"r2","priority":2,"when":{"path":"$.facts.account.balance","op":"gt","value":1000},"then":[{"emit":{"topic":"test","data":{}}}]},{"id":"r3","priority":3,"when":{"all":[{"path":"$.facts.profile.status","op":"eq","value":"active"}]},"then":[{"emit":{"topic":"test","data":{}}}]}]}`)
	
	_, err := NewCompiler(DefaultLimits()).Compile(source)
	if err == nil {
		t.Fatal("expected error for $.facts.* path, got nil")
	}
	if !strings.Contains(err.Error(), "$.facts") {
		t.Errorf("expected error mentioning $.facts, got: %v", err)
	}
}

func TestCompileRejectsFactsPathInComposite(t *testing.T) {
	// Test $.facts.* inside all/any/not
	source := json.RawMessage(`{"rule_set":"test","revision":1,"mode":"first_match","rules":[{"id":"r1","priority":1,"when":{"all":[{"path":"$.total","op":"gte","value":100},{"path":"$.facts.tier","op":"eq","value":"premium"}]},"then":[{"emit":{"topic":"test","data":{}}}]}]}`)
	
	_, err := NewCompiler(DefaultLimits()).Compile(source)
	if err == nil {
		t.Fatal("expected error for $.facts.* in composite predicate, got nil")
	}
	if !strings.Contains(err.Error(), "$.facts") {
		t.Errorf("expected error mentioning $.facts, got: %v", err)
	}
}

func TestCompileStableSortEqualPriority(t *testing.T) {
	// Rules with different priorities should be ordered by priority (highest first)
	// Rules with same priority should be ordered by rule ID for deterministic evaluation
	source := json.RawMessage(`{"rule_set":"test","revision":1,"mode":"first_match","rules":[
		{"id":"z-rule","priority":12,"when":{"path":"$.x","op":"eq","value":1},"then":[{"emit":{"topic":"z","data":{}}}]},
		{"id":"a-rule","priority":10,"when":{"path":"$.x","op":"eq","value":2},"then":[{"emit":{"topic":"a","data":{}}}]},
		{"id":"m-rule","priority":11,"when":{"path":"$.x","op":"eq","value":3},"then":[{"emit":{"topic":"m","data":{}}}]}
	]}`)
	
	rev, err := NewCompiler(DefaultLimits()).Compile(source)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	
	// Should be sorted by priority: z-rule (12), m-rule (11), a-rule (10)
	if len(rev.Compiled) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rev.Compiled))
	}
	if rev.Compiled[0].ID != "z-rule" || rev.Compiled[1].ID != "m-rule" || rev.Compiled[2].ID != "a-rule" {
		t.Errorf("expected order [z-rule, m-rule, a-rule], got [%s, %s, %s]",
			rev.Compiled[0].ID, rev.Compiled[1].ID, rev.Compiled[2].ID)
	}
	
	// Evaluate with x=2 should match a-rule (lowest priority, last in sorted order)
	eval := NewEvaluator(domain.SystemClock{})
	event := &domain.EventEnvelope{
		ID: "evt-1", Type: "test", TenantID: "t1", PartitionKey: "k1",
		Data: json.RawMessage(`{"x": 2}`),
	}
	decision, err := eval.Evaluate(rev, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	if len(decision.MatchedRules) != 1 || decision.MatchedRules[0] != "a-rule" {
		t.Errorf("expected a-rule to match, got %v", decision.MatchedRules)
	}
}

func TestCompileStableSortSamePriorityByID(t *testing.T) {
	// Test that same priority rules are sorted deterministically by ID
	source := json.RawMessage(`{"rule_set":"test","revision":1,"mode":"all_matches","rules":[
		{"id":"z-rule","priority":10,"when":{"path":"$.x","op":"eq","value":1},"then":[{"emit":{"topic":"z","data":{}}}]},
		{"id":"a-rule","priority":10,"when":{"path":"$.x","op":"eq","value":1},"then":[{"emit":{"topic":"a","data":{}}}]},
		{"id":"m-rule","priority":10,"when":{"path":"$.x","op":"eq","value":1},"then":[{"emit":{"topic":"m","data":{}}}]}
	]}`)
	
	// This should fail because of ambiguous priority
	_, err := NewCompiler(DefaultLimits()).Compile(source)
	if err == nil {
		t.Fatal("expected ambiguous priority error")
	}
	if !strings.Contains(err.Error(), "ambiguous priority") {
		t.Errorf("expected ambiguous priority error, got: %v", err)
	}
}
