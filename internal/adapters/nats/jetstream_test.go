package nats

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestRulesetRoutingUsesDisjointSubjectToken(t *testing.T) {
	payload := []byte(`{"id":"evt-1","type":"orders.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":"tenant-a","partition_key":"order-1"}}`)
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
	payload := []byte(`{"id":"evt-1","type":"payments.created","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{"tenant_id":"tenant-a","partition_key":"payment-1"}}`)
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

func TestStreamSubjectsIncludeApplicationCommandAndServiceRoutes(t *testing.T) {
	got := streamSubjects(Config{Subjects: []string{"events"}})
	want := map[string]bool{
		"events.shard.*":           true,
		"events.ruleset.*.shard.*": true,
		"commands.>":               true,
		"effects.>":                true,
		"services.>":               true,
	}
	for _, subject := range got {
		delete(want, subject)
	}
	if len(want) != 0 {
		t.Fatalf("stream subjects are missing required routes: %v", want)
	}
}

func TestStreamRetentionIsLogBased(t *testing.T) {
	cfg := streamConfig(Config{Stream: "flowrulz-test", Subjects: []string{"events"}, MaxAge: 24 * time.Hour, MaxBytes: 1024, MaxMsgs: 50})
	if cfg.Retention != jetstream.LimitsPolicy {
		t.Fatalf("stream retention policy = %v, want %v", cfg.Retention, jetstream.LimitsPolicy)
	}
	if cfg.Storage != jetstream.FileStorage {
		t.Fatalf("stream storage = %v, want file storage", cfg.Storage)
	}
	if cfg.MaxAge != 24*time.Hour || cfg.MaxBytes != 1024 || cfg.MaxMsgs != 50 {
		t.Fatalf("stream limits = age:%s bytes:%d msgs:%d, want 24h/1024/50", cfg.MaxAge, cfg.MaxBytes, cfg.MaxMsgs)
	}
}
