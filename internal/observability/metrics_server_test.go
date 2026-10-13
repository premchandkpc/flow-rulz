package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMetricsServerCORSAllowsOnlyConfiguredOrigin(t *testing.T) {
	server := NewMetricsServer("http://127.0.0.1:5173")
	handler := server.Handler

	allowed := httptest.NewRequest(http.MethodOptions, "/metrics", nil)
	allowed.Header.Set("Origin", "http://127.0.0.1:5173")
	allowed.Header.Set("Access-Control-Request-Method", "GET")
	allowedResponse := httptest.NewRecorder()
	handler.ServeHTTP(allowedResponse, allowed)
	if allowedResponse.Code != http.StatusNoContent {
		t.Fatalf("allowed origin status = %d, want %d", allowedResponse.Code, http.StatusNoContent)
	}
	if got := allowedResponse.Header().Get("Access-Control-Allow-Origin"); got != "http://127.0.0.1:5173" {
		t.Fatalf("allow-origin = %q", got)
	}

	blocked := httptest.NewRequest(http.MethodOptions, "/metrics", nil)
	blocked.Header.Set("Origin", "https://untrusted.example")
	blockedResponse := httptest.NewRecorder()
	handler.ServeHTTP(blockedResponse, blocked)
	if blockedResponse.Code != http.StatusForbidden {
		t.Fatalf("blocked origin status = %d, want %d", blockedResponse.Code, http.StatusForbidden)
	}
}

func TestMetricsServerDoesNotEnableCORSByDefault(t *testing.T) {
	server := NewMetricsServer("")
	request := httptest.NewRequest(http.MethodOptions, "/metrics", nil)
	request.Header.Set("Origin", "https://console.example")
	response := httptest.NewRecorder()
	server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
	if got := response.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("unexpected allow-origin %q", got)
	}
}
