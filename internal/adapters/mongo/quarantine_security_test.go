package mongo

import (
	"testing"

	"go.mongodb.org/mongo-driver/bson"
)

// TestMongoEqualityKeepsCallerInputAsScalar verifies that MongoDB operator-like
// strings remain equality operands rather than becoming query operators.
func TestMongoEqualityKeepsCallerInputAsScalar(t *testing.T) {
	for _, value := range []string{
		"$ne",
		`{"$where":"return true"}`,
		`{"$gt":""}`,
		"event",
	} {
		t.Run(value, func(t *testing.T) {
			encoded, err := bson.Marshal(bson.D{
				{Key: "source_type", Value: mongoEquality(value)},
			})
			if err != nil {
				t.Fatalf("marshal equality filter: %v", err)
			}

			var decoded bson.M
			if err := bson.Unmarshal(encoded, &decoded); err != nil {
				t.Fatalf("unmarshal equality filter: %v", err)
			}

			equality, ok := decoded["source_type"].(bson.M)
			if !ok {
				t.Fatalf("source_type has type %T, want bson.M", decoded["source_type"])
			}
			got, ok := equality["$eq"].(string)
			if !ok {
				t.Fatalf("source_type.$eq has type %T, want string", equality["$eq"])
			}
			if got != value {
				t.Fatalf("source_type.$eq = %q, want exact input %q", got, value)
			}
			if len(equality) != 1 {
				t.Fatalf("source_type equality expression has %d keys, want exactly $eq", len(equality))
			}
		})
	}
}
