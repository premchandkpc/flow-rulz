package rules

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	rulesengine "github.com/flowrulz/flow-rulz/internal/rules"
)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

const checkSource = `{"rule_set":"order.created","revision":3,"mode":"all_matches","rules":[
 {"id":"big","priority":10,"when":{"path":"$.total","op":"gte","value":100},
  "then":[{"emit_event":{"type":"payment.requested","partition_key_policy":"inherit","data":{"amount":"{{$.total}}"}}}]}]}`

func TestCheckRule(t *testing.T) {
	svc := &Service{compiler: rulesengine.NewCompiler(rulesengine.DefaultLimits()), clock: fixedClock{}}

	t.Run("invalid source reports the compile error", func(t *testing.T) {
		got := svc.CheckRule(context.Background(), "t", "", []byte(`{"rule_set":"x","revision":0,"rules":[]}`), nil)
		if got.Valid || got.Error == "" {
			t.Fatalf("expected invalid result with error, got %+v", got)
		}
	})
	t.Run("valid source without sample is not evaluated", func(t *testing.T) {
		got := svc.CheckRule(context.Background(), "t", "", []byte(checkSource), nil)
		if !got.Valid || got.RuleCount != 1 || got.Revision != 3 || got.Decision != nil {
			t.Fatalf("unexpected result %+v", got)
		}
	})
	t.Run("sample data is evaluated", func(t *testing.T) {
		got := svc.CheckRule(context.Background(), "t", "", []byte(checkSource), json.RawMessage(`{"total":250}`))
		if got.Decision == nil || len(got.Decision.MatchedRules) != 1 || len(got.Decision.Effects) != 1 {
			t.Fatalf("expected one matched rule and effect, got %+v", got)
		}
		miss := svc.CheckRule(context.Background(), "t", "", []byte(checkSource), json.RawMessage(`{"total":5}`))
		if miss.Decision == nil || len(miss.Decision.MatchedRules) != 0 {
			t.Fatalf("expected no match, got %+v", miss)
		}
	})
}
