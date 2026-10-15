package logfirst

import (
	"context"
	"errors"
	"testing"
	"time"
)

type resolverStub struct {
	refs []RulesetRef
	err  error
}

func (r resolverStub) Resolve(context.Context, string, int64, EventEnvelope) ([]RulesetRef, error) {
	return r.refs, r.err
}

func (resolverStub) GetRevision(context.Context, RulesetRef) (CompiledRuleset, string, error) {
	return nil, "", errors.New("unexpected revision lookup")
}

type subscriberRouterStub struct {
	calls   int
	effects []Effect
	err     error
}

func (r *subscriberRouterStub) Route(context.Context, EventEnvelope, string) ([]Effect, error) {
	r.calls++
	return r.effects, r.err
}

func TestSubscriberRoutingRunsWhenDeploymentHasNoRulesets(t *testing.T) {
	router := &subscriberRouterStub{effects: []Effect{{
		ID: "delivery-1", IdempotencyKey: "delivery-1",
		Destination: "flow.subscriber.service-a", PartitionKey: "tenant-1:event-1",
	}}}
	evaluator := &RuleEvaluator{Resolver: resolverStub{}, Router: router}
	event := EventEnvelope{
		SchemaVersion: 1, ID: "event-1", TenantID: "tenant-1",
		EventType: "orders.created", PartitionKey: "order-1",
		OccurredAt: time.Unix(1, 0).UTC(),
		Metadata:   EventMetadata{TargetServiceID: "domain-a/service-a", DeploymentRevision: 2},
	}

	effects, audits, err := evaluator.Evaluate(context.Background(), "domain-a/service-a", 2, event)
	if err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	if router.calls != 1 {
		t.Fatalf("subscriber router calls = %d, want 1", router.calls)
	}
	if len(effects) != 1 || effects[0].ID != "delivery-1" {
		t.Fatalf("effects = %#v, want the subscriber delivery effect", effects)
	}
	if len(audits) != 0 {
		t.Fatalf("audits = %#v, want none without assigned rulesets", audits)
	}
}

func TestRuleEvaluatorRejectsEventForDifferentServiceDeployment(t *testing.T) {
	router := &subscriberRouterStub{}
	evaluator := &RuleEvaluator{Resolver: resolverStub{}, Router: router}
	event := EventEnvelope{
		SchemaVersion: 1, ID: "event-1", TenantID: "tenant-1",
		EventType: "orders.created", PartitionKey: "order-1",
		Metadata: EventMetadata{TargetServiceID: "domain-a/service-b", DeploymentRevision: 2},
	}

	if _, _, err := evaluator.Evaluate(context.Background(), "domain-a/service-a", 2, event); err == nil {
		t.Fatal("expected target deployment mismatch error")
	}
	if router.calls != 0 {
		t.Fatalf("subscriber router called %d times for a misrouted event", router.calls)
	}
}
