package mongo

import "testing"

func TestNormalizeWorkflowListLimit(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  int
	}{
		{name: "negative defaults", input: -1, want: 100},
		{name: "zero defaults", input: 0, want: 100},
		{name: "positive preserved", input: 25, want: 25},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizeWorkflowListLimit(tt.input); got != tt.want {
				t.Fatalf("normalizeWorkflowListLimit(%d) = %d, want %d", tt.input, got, tt.want)
			}
		})
	}
}
