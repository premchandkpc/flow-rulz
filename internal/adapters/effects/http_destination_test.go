package effects

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestHTTPDestinationAcceptsSuccessfulStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/orders/created" {
			t.Errorf("request path = %q, want %q", r.URL.Path, "/orders/created")
		}
		if got := r.Header.Get("Idempotency-Key"); got != "effect-123" {
			t.Errorf("idempotency key = %q, want %q", got, "effect-123")
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	destination := NewHTTPDestination(server.URL)
	err := destination.Send(context.Background(), &domain.Effect{
		ID: "123", ExecutionID: "exec-1", Destination: "orders", Name: "created",
		Payload: []byte(`{"order_id":"order-1"}`),
	}, "effect-123")
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
}

func TestHTTPDestinationRejectsNon2xxStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusFound)
	}))
	defer server.Close()

	destination := NewHTTPDestination(server.URL)
	err := destination.Send(context.Background(), &domain.Effect{
		ID: "123", Destination: "orders", Name: "created", Payload: []byte(`{}`),
	}, "effect-123")
	if err == nil {
		t.Fatal("Send() error = nil, want non-2xx status error")
	}
	if !strings.Contains(err.Error(), "destination returned non-success status 302") {
		t.Fatalf("Send() error = %q, want status 302", err)
	}
}

func TestHTTPDestinationBoundsErrorResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(strings.Repeat("x", 1<<20)))
	}))
	defer server.Close()

	destination := NewHTTPDestination(server.URL)
	err := destination.Send(context.Background(), &domain.Effect{
		ID: "123", Destination: "orders", Name: "created", Payload: []byte(`{}`),
	}, "effect-123")
	if err == nil {
		t.Fatal("Send() error = nil, want response error")
	}
	if got := len(err.Error()); got > (5 << 10) {
		t.Fatalf("error length = %d, expected bounded error response", got)
	}
	if !strings.Contains(err.Error(), "destination returned non-success status 502") {
		t.Fatalf("error = %q, want status-only error", err.Error())
	}
	if strings.Contains(err.Error(), strings.Repeat("x", 100)) {
		t.Fatal("destination response body leaked into error")
	}
}
