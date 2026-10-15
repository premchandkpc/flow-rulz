package logfirst

import (
	"context"
	"errors"
	"testing"
	"time"
)

type evaluatorResolver struct {
	refs []RulesetRef
	resolveErr error
	compiled CompiledRuleset
	contentHash string
	revisionErr error
}

func (r evaluatorResolver) Resolve(context.Context, string, int64, EventEnvelope) ([]RulesetRef, error) {
	return r.refs, r.resolveErr
}
func (r evaluatorResolver) GetRevision(context.Context, RulesetRef) (CompiledRuleset, string, error) {
	return r.compiled, r.contentHash, r.revisionErr
}

type evaluatorRuleset struct {
	effects []Effect
	err error
}
func (r evaluatorRuleset) Evaluate(context.Context, EventEnvelope) ([]Effect, error) {
	return r.effects, r.err
}

type evaluatorRouter struct {
	calls int
	effects []Effect
	err error
}
func (r *evaluatorRouter) Route(context.Context, EventEnvelope, string) ([]Effect, error) {
	r.calls++
	return r.effects, r.err
}

func evaluatorEvent() EventEnvelope {
	return EventEnvelope{
		SchemaVersion: 1, ID: "event-1", TenantID: "tenant-a",
		EventType: "orders.created", PartitionKey: "order-1",
		OccurredAt: time.Unix(100, 0).UTC(),
		Metadata: EventMetadata{TargetServiceID: "domain-a/orders", DeploymentRevision: 2},
	}
}

func TestRuleEvaluatorEvaluatesPinnedRulesAndRoutesOnce(t *testing.T) {
	now := time.Unix(200, 0)
	router := &evaluatorRouter{effects: []Effect{{
		IdempotencyKey: "delivery-1", Destination: "flow.deliveries", PartitionKey: "tenant-a:event-1",
	}}}
	evaluator := &RuleEvaluator{
		Resolver: evaluatorResolver{
			refs: []RulesetRef{
				{TenantID: "tenant-a", RulesetID: "orders-created", Revision: 3},
				{TenantID: "tenant-a", RulesetID: "orders-notify", Revision: 5},
			},
			compiled: evaluatorRuleset{effects: []Effect{{
				IdempotencyKey: "effect-1", Destination: "flow.commands", PartitionKey: "order-1",
			}}},
			contentHash: "sha256:rules",
		},
		Router: router, Now: func() time.Time { return now },
	}
	effects, audits, err := evaluator.Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
	if err != nil { t.Fatalf("Evaluate() error = %v", err) }
	if len(effects) != 3 { t.Fatalf("effects = %d, want 3", len(effects)) }
	if len(audits) != 2 { t.Fatalf("audits = %d, want 2", len(audits)) }
	if router.calls != 1 { t.Fatalf("router calls = %d, want 1", router.calls) }
	if effects[0].ID != "effect-1" || effects[2].ID != "delivery-1" { t.Fatalf("effect IDs not made deterministic: %#v", effects) }
	if effects[0].TenantID != "tenant-a" || effects[0].CausationID != "event-1" || effects[0].Hop != 1 { t.Fatalf("rule effect metadata = %#v", effects[0]) }
	for _, audit := range audits {
		if audit.TenantID != "tenant-a" || audit.EventID != "event-1" || audit.ContentHash != "sha256:rules" || !audit.OccurredAt.Equal(now.UTC()) {
			t.Fatalf("audit = %#v", audit)
		}
	}
}

func TestRuleEvaluatorRejectsInvalidEnvelopeAndDeployment(t *testing.T) {
	tests := []struct {
		name string
		edit func(*EventEnvelope)
		want error
	}{
		{"unsupported schema", func(e *EventEnvelope) { e.SchemaVersion = 2 }, ErrUnsupportedSchema},
		{"missing id", func(e *EventEnvelope) { e.ID = "" }, nil},
		{"missing tenant", func(e *EventEnvelope) { e.TenantID = "" }, nil},
		{"missing event type", func(e *EventEnvelope) { e.EventType = "" }, nil},
		{"missing partition key", func(e *EventEnvelope) { e.PartitionKey = "" }, nil},
		{"wrong service", func(e *EventEnvelope) { e.Metadata.TargetServiceID = "domain-a/billing" }, nil},
		{"wrong revision", func(e *EventEnvelope) { e.Metadata.DeploymentRevision = 1 }, nil},
		{"hop limit", func(e *EventEnvelope) { e.Metadata.Hop = 4 }, ErrHopLimit},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := evaluatorEvent(); tt.edit(&event)
			_, _, err := (&RuleEvaluator{Resolver: evaluatorResolver{}, MaxHop: 4}).Evaluate(context.Background(), "domain-a/orders", 2, event)
			if err == nil { t.Fatal("expected validation error") }
			if tt.want != nil && !errors.Is(err, tt.want) { t.Fatalf("error = %v, want %v", err, tt.want) }
		})
	}
}

func TestRuleEvaluatorRejectsCrossTenantOrInvalidResolverReferences(t *testing.T) {
	tests := []RulesetRef{
		{TenantID: "tenant-b", RulesetID: "orders-created", Revision: 1},
		{TenantID: "tenant-a", RulesetID: "", Revision: 1},
		{TenantID: "tenant-a", RulesetID: "orders-created", Revision: 0},
	}
	for i, ref := range tests {
		t.Run(string(rune('a'+i)), func(t *testing.T) {
			_, _, err := (&RuleEvaluator{Resolver: evaluatorResolver{refs: []RulesetRef{ref}}}).Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
			if err == nil { t.Fatal("expected invalid resolver reference error") }
		})
	}
}

func TestRuleEvaluatorPropagatesResolutionFailures(t *testing.T) {
	resolveErr := errors.New("projection unavailable")
	_, _, err := (&RuleEvaluator{Resolver: evaluatorResolver{resolveErr: resolveErr}}).Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
	if !errors.Is(err, resolveErr) { t.Fatalf("resolve error = %v", err) }

	revisionErr := errors.New("immutable revision missing")
	_, _, err = (&RuleEvaluator{Resolver: evaluatorResolver{
		refs: []RulesetRef{{TenantID: "tenant-a", RulesetID: "orders-created", Revision: 1}},
		revisionErr: revisionErr,
	}}).Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
	if !errors.Is(err, ErrMissingRuleset) || !errors.Is(err, revisionErr) { t.Fatalf("revision error = %v", err) }

	evaluationErr := errors.New("rule engine failed")
	_, _, err = (&RuleEvaluator{Resolver: evaluatorResolver{
		refs: []RulesetRef{{TenantID: "tenant-a", RulesetID: "orders-created", Revision: 1}},
		compiled: evaluatorRuleset{err: evaluationErr},
	}}).Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
	if !errors.Is(err, evaluationErr) { t.Fatalf("evaluation error = %v", err) }
}

func TestRuleEvaluatorRejectsNilCompiledRuleset(t *testing.T) {
	_, _, err := (&RuleEvaluator{Resolver: evaluatorResolver{
		refs: []RulesetRef{{TenantID: "tenant-a", RulesetID: "orders-created", Revision: 1}},
	}}).Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
	if !errors.Is(err, ErrMissingRuleset) { t.Fatalf("error = %v, want ErrMissingRuleset", err) }
}

func TestRuleEvaluatorRejectsMalformedEffects(t *testing.T) {
	tests := []struct {
		name string
		ruleEffects []Effect
		subEffects []Effect
		useRouter bool
	}{
		{"rule effect without idempotency", []Effect{{Destination:"commands", PartitionKey:"key"}}, nil, false},
		{"rule effect without destination", []Effect{{IdempotencyKey:"id", PartitionKey:"key"}}, nil, false},
		{"rule effect without partition key", []Effect{{IdempotencyKey:"id", Destination:"commands"}}, nil, false},
		{"subscriber effect without idempotency", nil, []Effect{{Destination:"deliveries", PartitionKey:"key"}}, true},
		{"subscriber effect without destination", nil, []Effect{{IdempotencyKey:"id", PartitionKey:"key"}}, true},
		{"subscriber effect without partition key", nil, []Effect{{IdempotencyKey:"id", Destination:"deliveries"}}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolver := evaluatorResolver{}
			router := &evaluatorRouter{effects: tt.subEffects}
			if !tt.useRouter {
				resolver = evaluatorResolver{
					refs: []RulesetRef{{TenantID:"tenant-a", RulesetID:"orders-created", Revision:1}},
					compiled: evaluatorRuleset{effects:tt.ruleEffects},
				}
				router = nil
			}
			_, _, err := (&RuleEvaluator{Resolver:resolver, Router:router}).Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
			if err == nil { t.Fatal("expected malformed effect error") }
		})
	}
}

func TestRuleEvaluatorPropagatesSubscriberRouterFailure(t *testing.T) {
	routerErr := errors.New("subscriber projection unavailable")
	_, _, err := (&RuleEvaluator{Resolver:evaluatorResolver{}, Router:&evaluatorRouter{err:routerErr}}).Evaluate(context.Background(), "domain-a/orders", 2, evaluatorEvent())
	if !errors.Is(err, routerErr) { t.Fatalf("router error = %v", err) }
}
