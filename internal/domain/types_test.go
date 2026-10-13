package domain

import "testing"

func TestShardHashUsesStableFNV1aValues(t *testing.T) {
	tests := []struct {
		input string
		want  uint32
	}{
		{input: "", want: 2166136261},
		{input: "a", want: 3826002220},
		{input: "worker-test", want: 930350870},
	}

	for _, tt := range tests {
		if got := ShardHash(tt.input); got != tt.want {
			t.Errorf("ShardHash(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestComputeExecutionIDIsDeterministic(t *testing.T) {
	first := ComputeExecutionID("tenant-a", "event-123", "orders", 7)
	second := ComputeExecutionID("tenant-a", "event-123", "orders", 7)
	if first == "" {
		t.Fatal("execution ID must not be empty")
	}
	if first != second {
		t.Fatalf("same execution identity inputs produced different IDs: %q != %q", first, second)
	}
}

func TestComputeExecutionIDChangesWhenIdentityChanges(t *testing.T) {
	base := ComputeExecutionID("tenant-a", "event-123", "orders", 7)
	tests := []struct {
		name     string
		tenantID string
		eventID  string
		ruleSet  string
		revision int64
	}{
		{name: "tenant", tenantID: "tenant-b", eventID: "event-123", ruleSet: "orders", revision: 7},
		{name: "event", tenantID: "tenant-a", eventID: "event-456", ruleSet: "orders", revision: 7},
		{name: "rule set", tenantID: "tenant-a", eventID: "event-123", ruleSet: "payments", revision: 7},
		{name: "revision", tenantID: "tenant-a", eventID: "event-123", ruleSet: "orders", revision: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeExecutionID(tt.tenantID, tt.eventID, tt.ruleSet, tt.revision)
			if got == base {
				t.Fatalf("identity changed (%s) but execution ID stayed the same: %q", tt.name, got)
			}
		})
	}
}
