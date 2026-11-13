package rules

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

// mustCompile compiles a rule set source and fails the test on error.
func mustCompile(t *testing.T, source string) *domain.RuleRevision {
	t.Helper()
	revision, err := NewCompiler(DefaultLimits()).Compile(json.RawMessage(source))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return revision
}

// compileError compiles a rule set source and returns the resulting error,
// failing the test if compilation unexpectedly succeeds.
func compileError(t *testing.T, source string) error {
	t.Helper()
	_, err := NewCompiler(DefaultLimits()).Compile(json.RawMessage(source))
	if err == nil {
		t.Fatal("expected compile error, got nil")
	}
	return err
}

// mustEvaluate runs rule evaluation with a fixed system clock and fails the
// test on error.
func mustEvaluate(t *testing.T, revision *domain.RuleRevision, event *domain.EventEnvelope) *domain.Decision {
	t.Helper()
	decision, err := NewEvaluator(domain.SystemClock{}).Evaluate(revision, event, nil)
	if err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	return decision
}

// testEvent builds an order.created event with the given identity and payload.
func testEvent(id, tenant, partitionKey string, data json.RawMessage) *domain.EventEnvelope {
	return &domain.EventEnvelope{
		ID: id, Type: "order.created", TenantID: tenant, PartitionKey: partitionKey, Data: data,
	}
}

// assertCompiledOrder asserts the compiled rule count and ID order.
func assertCompiledOrder(t *testing.T, revision *domain.RuleRevision, want ...string) {
	t.Helper()
	got := make([]string, len(revision.Compiled))
	for i, rule := range revision.Compiled {
		got[i] = rule.ID
	}
	if !slices.Equal(got, want) {
		t.Fatalf("compiled rules = %v, want %v", got, want)
	}
}

// assertMatchedRules asserts the exact matched rule list.
func assertMatchedRules(t *testing.T, decision *domain.Decision, want ...string) {
	t.Helper()
	if !slices.Equal(decision.MatchedRules, want) {
		t.Errorf("matched rules = %v, want %v", decision.MatchedRules, want)
	}
}

// assertEffectsCount asserts the number of decision effects.
func assertEffectsCount(t *testing.T, decision *domain.Decision, want int) {
	t.Helper()
	if len(decision.Effects) != want {
		t.Fatalf("effects = %d, want %d", len(decision.Effects), want)
	}
}

// decodeChildEffect decodes an emit_event effect payload as a child envelope.
func decodeChildEffect(t *testing.T, effect domain.Effect) domain.EventEnvelope {
	t.Helper()
	var child domain.EventEnvelope
	if err := json.Unmarshal(effect.Payload, &child); err != nil {
		t.Fatalf("decode child: %v", err)
	}
	return child
}

// decodeScheduledEffect decodes a schedule_event effect payload as a durable
// scheduled-event intent.
func decodeScheduledEffect(t *testing.T, effect domain.Effect) domain.ScheduledEvent {
	t.Helper()
	var scheduled domain.ScheduledEvent
	if err := json.Unmarshal(effect.Payload, &scheduled); err != nil {
		t.Fatalf("decode scheduled intent: %v", err)
	}
	return scheduled
}

// decodePayloadMap decodes arbitrary JSON into a generic map for field assertions.
func decodePayloadMap(t *testing.T, data []byte) map[string]interface{} {
	t.Helper()
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	return decoded
}

func assertPaymentChild(t *testing.T, child domain.EventEnvelope) {
	t.Helper()
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

func evaluateChild(t *testing.T, revision *domain.RuleRevision, event *domain.EventEnvelope) domain.EventEnvelope {
	t.Helper()
	decision := mustEvaluate(t, revision, event)
	assertEffectsCount(t, decision, 1)
	return decodeChildEffect(t, decision.Effects[0])
}

func assertChildCorrelation(t *testing.T, child domain.EventEnvelope, parent *domain.EventEnvelope) {
	t.Helper()
	if child.WorkflowID != parent.WorkflowID || child.CorrelationID != parent.CorrelationID || child.CausationID != parent.ID {
		t.Fatalf("child correlation metadata = workflow %q, correlation %q, causation %q", child.WorkflowID, child.CorrelationID, child.CausationID)
	}
}

func assertResolvedNumber(t *testing.T, data json.RawMessage, path, want string) {
	t.Helper()
	value, err := resolvePath(path, data, nil)
	if err != nil {
		t.Fatalf("resolvePath error: %v", err)
	}
	number, ok := value.(json.Number)
	if !ok {
		t.Fatalf("expected json.Number, got %T: %v", value, value)
	}
	if number.String() != want {
		t.Errorf("expected %q, got %q", want, number.String())
	}
}

func evaluateScheduledReminder(t *testing.T) (domain.ScheduledEvent, time.Time) {
	t.Helper()
	revision := mustCompile(t, scheduleReminderSource)
	now := time.Now().UTC()
	event := testEvent("evt-schedule", "tenant-a", "order-7", json.RawMessage(`{"order_id":"order-7"}`))
	event.WorkflowID, event.CorrelationID, event.OccurredAt = "wf-7", "corr-7", now
	decision := mustEvaluate(t, revision, event)
	assertEffectsCount(t, decision, 1)
	if decision.Effects[0].EffectType != domain.EffectTypeScheduleEvent {
		t.Fatalf("effects = %#v, want one schedule_event effect", decision.Effects)
	}
	return decodeScheduledEffect(t, decision.Effects[0]), now
}
