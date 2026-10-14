package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRouterFailsClosedWhenRequiredAuthValidatorIsMissing(t *testing.T) {
	router := NewRouterWithConfig(
		nil, nil, nil, nil, nil, nil,
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

func TestManagementRouterDoesNotExposeEventIngestion(t *testing.T) {
	router := NewRouterWithConfig(nil, nil, nil, nil, nil, nil, RouterConfig{})

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/events/order.created", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("event ingestion status = %d, want %d; body=%s", recorder.Code, http.StatusNotFound, recorder.Body.String())
	}
}
