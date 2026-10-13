package mongo

import "testing"

func TestNormalizeQuarantineSourceType(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{input: "", want: ""},
		{input: "event", want: "event"},
		{input: "effect", want: "effect"},
		{input: "$ne", wantErr: true},
		{input: `{"$where":"return true"}`, wantErr: true},
		{input: "workflow", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := normalizeQuarantineSourceType(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("normalizeQuarantineSourceType(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("normalizeQuarantineSourceType(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestNormalizeQuarantineErrorClass(t *testing.T) {
	valid := []string{
		"validation", "rule", "evaluation", "execution", "effect",
		"broker", "database", "authorization", "quarantine", "internal",
	}
	for _, value := range valid {
		t.Run(value, func(t *testing.T) {
			got, err := normalizeQuarantineErrorClass(value)
			if err != nil {
				t.Fatalf("normalizeQuarantineErrorClass(%q): %v", value, err)
			}
			if got != value {
				t.Fatalf("normalizeQuarantineErrorClass(%q) = %q", value, got)
			}
		})
	}

	for _, value := range []string{
		"$ne",
		`{"$where":"return true"}`,
		"unknown",
		"Validation",
	} {
		t.Run("reject_"+value, func(t *testing.T) {
			if got, err := normalizeQuarantineErrorClass(value); err == nil {
				t.Fatalf("normalizeQuarantineErrorClass(%q) = %q, want error", value, got)
			}
		})
	}
}
