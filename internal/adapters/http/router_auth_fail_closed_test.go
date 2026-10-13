package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouterFailsClosedWhenRequiredAuthValidatorIsMissing(t *testing.T) {
	router := NewRouterWithPublisher(
		nil, nil, nil, nil, nil, nil, nil, nil, 1,
		RouterConfig{RequireAuth: true},
	)

	protected := httptest.NewRecorder()
	router.ServeHTTP(protected, httptest.NewRequest(http.MethodGet, "/v1/rules/active", nil))
	if protected.Code != http.StatusServiceUnavailable {
		t.Fatalf("protected route status = %d, want %d; body=%s", protected.Code, http.StatusServiceUnavailable, protected.Body.String())
	}

	health := httptest.NewRecorder()
	router.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d; body=%s", health.Code, http.StatusOK, health.Body.String())
	}
}
