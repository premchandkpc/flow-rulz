package flowrulz

import (
	"strings"
	"testing"
)

func TestHTTPTransportRejectedForControlPlaneAPI(t *testing.T) {
	_, err := NewTransport(&Config{Transport: TransportHTTP})
	if err == nil {
		t.Fatal("NewTransport(HTTP) error = nil, want control-plane boundary error")
	}
	if got := err.Error(); !strings.Contains(got, "HTTP event ingestion is not supported by the control-plane API; use the NATS transport") {
		t.Fatalf("error = %q, want actionable NATS transport guidance", got)
	}
}
