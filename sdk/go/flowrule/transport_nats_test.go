package flowrulz

import (
	"strings"
	"testing"
)

func TestNATSSubjectRoutingUsesConfiguredRuleSet(t *testing.T) {
	got := natsSubjectForEnvelope("events", 4, "orders.created", []string{"orders.created"})
	if !strings.HasPrefix(got, "events.ruleset.") || !strings.HasSuffix(got, ".shard.4") {
		t.Fatalf("natsSubjectForEnvelope() = %q, want ruleset route", got)
	}
	if strings.Contains(got, "orders.created") {
		t.Fatalf("ruleset route contains raw event type: %q", got)
	}
}

func TestNATSSubjectRoutingKeepsLegacyForUnconfiguredRuleSet(t *testing.T) {
	want := "events.shard.2"
	got := natsSubjectForEnvelope("events", 2, "payments.created", []string{"orders.created"})
	if got != want {
		t.Fatalf("natsSubjectForEnvelope() = %q, want %q", got, want)
	}
}

func TestNATSSubscriptionFiltersOnlyIncludeRequestedRuleSet(t *testing.T) {
	filters := natsSubscriptionFilters("events", "orders.created", []string{"orders.created", "payments.created"})
	if len(filters) != 2 {
		t.Fatalf("got %d filters, want legacy and requested ruleset filters: %v", len(filters), filters)
	}
	if filters[0] != "events.shard.*" {
		t.Fatalf("legacy filter = %q", filters[0])
	}
	if !strings.HasPrefix(filters[1], "events.ruleset.") || !strings.HasSuffix(filters[1], ".shard.*") {
		t.Fatalf("ruleset filter = %q", filters[1])
	}
	if strings.Contains(filters[1], "payments.created") {
		t.Fatalf("filter includes an unrelated ruleset: %q", filters[1])
	}
}

func TestNATSSubscriptionFiltersUseLegacyRouteForUnconfiguredRuleSet(t *testing.T) {
	filters := natsSubscriptionFilters("events", "payments.created", []string{"orders.created"})
	if len(filters) != 1 || filters[0] != "events.shard.*" {
		t.Fatalf("filters = %v, want only the legacy route", filters)
	}
}
