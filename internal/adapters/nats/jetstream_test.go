package nats

import (
	"context"
	"strings"
	"testing"
)

func TestRulesetRoutingUsesDisjointSubjectToken(t *testing.T) {
	payload := []byte(`{"id":"evt-1","type":"orders.created","tenant_id":"tenant-a","partition_key":"order-1"}`)
	got := subjectForMessage("events", 7, payload, []string{"orders.created"})
	want := ruleSetSubject("events", "orders.created", 7)
	if got != want {
		t.Fatalf("subjectForMessage() = %q, want %q", got, want)
	}
	if strings.Contains(got, "orders.created") {
		t.Fatalf("raw ruleset leaked into subject: %q", got)
	}
}

func TestRulesetRoutingFallsBackToLegacyForUnconfiguredTypes(t *testing.T) {
	payload := []byte(`{"id":"evt-1","type":"payments.created","tenant_id":"tenant-a","partition_key":"payment-1"}`)
	if got, want := subjectForMessage("events", 3, payload, []string{"orders.created"}), "events.shard.3"; got != want {
		t.Fatalf("subjectForMessage() = %q, want %q", got, want)
	}
}

func TestNormalizeConfigRejectsDuplicateRulesets(t *testing.T) {
	_, err := normalizeConfig(Config{RuleSets: []string{"orders.created", "orders.created"}})
	if err == nil || !strings.Contains(err.Error(), "duplicate NATS ruleset") {
		t.Fatalf("normalizeConfig() error = %v, want duplicate ruleset error", err)
	}
}

func TestNewConsumerRejectsFiniteMaxDeliverWithoutRecovery(t *testing.T) {
	_, err := NewConsumer(context.Background(), Config{MaxDeliver: 10})
	if err == nil {
		t.Fatal("NewConsumer() error = nil, want finite MaxDeliver to be rejected")
	}
	if !strings.Contains(err.Error(), "MaxDeliver must be -1 (unlimited)") {
		t.Fatalf("NewConsumer() error = %v, want explicit unlimited-delivery requirement", err)
	}
}
