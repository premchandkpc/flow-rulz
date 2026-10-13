package mongo

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// This regression test documents the security boundary for workflow_type filters:
// workflow types must pass validation and are encoded as BSON strings, never as
// caller-controlled BSON documents or MongoDB operators.
func TestWorkflowTypeQueryValueIsValidatedBSONScalar(t *testing.T) {
	const validType = "orders.v1"
	if err := validateWorkflowTypeName(validType); err != nil {
		t.Fatalf("valid workflow type rejected: %v", err)
	}

	for _, invalid := range []string{
		"",
		`{"$ne":null}`,
		`$where`,
		"orders type",
		"orders/type",
	} {
		if err := validateWorkflowTypeName(invalid); err == nil {
			t.Errorf("expected workflow type %q to be rejected", invalid)
		}
	}

	filter := bson.D{
		{Key: "workflow_type", Value: validType},
		{Key: "version", Value: int64(1)},
	}
	encoded, err := bson.Marshal(filter)
	if err != nil {
		t.Fatalf("marshal filter: %v", err)
	}

	var decoded bson.M
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal filter: %v", err)
	}
	value, ok := decoded["workflow_type"].(string)
	if !ok {
		t.Fatalf("workflow_type encoded as %T, want BSON string", decoded["workflow_type"])
	}
	if value != validType {
		t.Fatalf("workflow_type = %q, want %q", value, validType)
	}
	if len(decoded) != 2 {
		t.Fatalf("filter contains %d keys, want exactly the two fixed keys", len(decoded))
	}
}
