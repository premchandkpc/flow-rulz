package rules

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

func TestInterpolateTemplates(t *testing.T) {
	event := &domain.EventEnvelope{Data: json.RawMessage(`{"amount":1500000,"vip":true,"tags":["a","b"],"customer":{"id":"c-1"},"id":"t-9"}`)}
	in := json.RawMessage(`{"amount":"{{$.amount}}","vip":"{{$.vip}}","tags":"{{$.tags}}","cid":"{{$.customer.id}}","label":"txn {{$.id}} of {{$.amount}}","new":"{{new_id}}","at":"{{now}}","missing":"{{$.nope}}","lit":5}`)
	out, err := interpolateTemplates(in, event, "child-1", fixedClock{})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]interface{}{
		"amount": float64(1500000), "vip": true, "tags": []interface{}{"a", "b"}, "cid": "c-1",
		"label": "txn t-9 of 1500000", "new": "child-1", "at": "2026-01-02T03:04:05Z", "missing": "", "lit": float64(5),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}
