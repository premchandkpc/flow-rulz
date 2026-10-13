package rules

import (
	"encoding/json"
	"errors"
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
