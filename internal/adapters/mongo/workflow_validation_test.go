package mongo

import (
	"strings"
	"testing"
)

func TestValidateWorkflowTypeName(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "simple", value: "orders", wantErr: false},
		{name: "versioned", value: "orders.v1", wantErr: false},
		{name: "underscore", value: "order_events", wantErr: false},
		{name: "empty", value: "", wantErr: true},
		{name: "query operator", value: "orders.$where", wantErr: true},
		{name: "leading punctuation", value: ".orders", wantErr: true},
		{name: "too long", value: strings.Repeat("a", 129), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateWorkflowTypeName(tt.value)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateWorkflowTypeName(%q) error = %v, wantErr %v", tt.value, err, tt.wantErr)
			}
		})
	}
}
