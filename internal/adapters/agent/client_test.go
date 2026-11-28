package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDecide(t *testing.T) {
	for _, tc := range []struct {
		name    string
		actions []string
		want    string
	}{
		{"read then decide", []string{`{"read":"amount"}`, `{"decision":"clear"}`}, "clear"},
		{"bounded reads", []string{`{"read":"amount"}`, `{"read":"amount"}`, `{"read":"amount"}`}, "review"},
		{"reject unknown field", []string{`{"read":"secret"}`}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls >= len(tc.actions) {
					http.Error(w, "extra turn", 400)
					return
				}
				var body struct{ Messages []message }
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				if calls == 1 && body.Messages[len(body.Messages)-1].Content != "42" {
					t.Error("missing selected value")
				}
				fmt.Fprintf(w, `{"choices":[{"message":{"content":%q}}]}`, tc.actions[calls])
				calls++
			}))
			defer server.Close()
			c, err := New(server.URL, "test")
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.Decide(context.Background(), map[string]json.RawMessage{"amount": json.RawMessage("42")})
			if got != tc.want || (err != nil) != (tc.want == "") || calls != len(tc.actions) {
				t.Fatalf("decision=%q calls=%d error=%v", got, calls, err)
			}
		})
	}
}

func TestDecideCanceledContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("canceled context must not reach model")
	}))
	defer server.Close()
	c, err := New(server.URL, "test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if decision, err := c.Decide(ctx, nil); err == nil || decision != "" {
		t.Fatalf("decision=%q error=%v", decision, err)
	}
}
