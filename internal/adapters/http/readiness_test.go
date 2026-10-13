package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestReadinessHandler(t *testing.T) {
	ok := func(context.Context) error { return nil }
	down := func(context.Context) error { return errors.New("boom: secret-dsn") }
	tests := []struct {
		name   string
		checks map[string]func(context.Context) error
		code   int
		body   string
	}{
		{"all up", map[string]func(context.Context) error{"database": ok, "nats": ok}, http.StatusOK, `"status":"ready"`},
		{"one down", map[string]func(context.Context) error{"database": ok, "nats": down}, http.StatusServiceUnavailable, `"nats":{"status":"down"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			readinessHandler(tt.checks)(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))
			if rec.Code != tt.code || !strings.Contains(rec.Body.String(), tt.body) {
				t.Fatalf("got %d %s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "secret-dsn") {
				t.Fatal("dependency error detail leaked")
			}
		})
	}
}
