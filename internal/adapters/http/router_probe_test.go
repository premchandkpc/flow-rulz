package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/auth"
)

func TestProbesWorkWithoutCredentialsWhenAuthIsEnabled(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	cfg := RouterConfig{RequireAuth: true, AuthValidator: auth.NewValidator([]byte("test-secret"), "", "")}
	cfg.ReadinessChecks = map[string]func(context.Context) error{"db": func(context.Context) error { return nil }}
	router := NewRouterWithConfig(nil, nil, nil, nil, nil, nil, cfg)
	assertProbeStatus(t, router, http.MethodGet, "/health", http.StatusOK)
	assertProbeStatus(t, router, http.MethodGet, "/ready", http.StatusOK)
	assertProbeStatus(t, router, http.MethodGet, "/v1/rules/active", http.StatusUnauthorized)
	assertProbeStatus(t, router, http.MethodPost, "/health", http.StatusUnauthorized)
}

func TestReadinessFailsWhenAuthOrDatabaseIsUnavailable(t *testing.T) {
	cfg := RouterConfig{RequireAuth: true}
	router := NewRouterWithConfig(nil, nil, nil, nil, nil, nil, cfg)
	assertProbeStatus(t, router, http.MethodGet, "/health", http.StatusOK)
	assertProbeStatus(t, router, http.MethodGet, "/ready", http.StatusServiceUnavailable)
	cfg.AuthValidator = auth.NewValidator([]byte("test-secret"), "", "")
	cfg.ReadinessChecks = map[string]func(context.Context) error{"db": func(context.Context) error { return errors.New("down") }}
	router = NewRouterWithConfig(nil, nil, nil, nil, nil, nil, cfg)
	assertProbeStatus(t, router, http.MethodGet, "/ready", http.StatusServiceUnavailable)
}

func assertProbeStatus(t *testing.T, router http.Handler, method, path string, expected int) {
	t.Helper()
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(method, path, nil))
	if recorder.Code != expected {
		t.Fatalf("%s %s: status=%d, want=%d", method, path, recorder.Code, expected)
	}
}
