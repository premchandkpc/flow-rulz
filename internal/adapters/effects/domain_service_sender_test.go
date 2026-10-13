package effects

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type domainServiceRepositoryStub struct {
	ports.DomainServiceRepository
	service *domain.DomainService
}

func (s domainServiceRepositoryStub) GetService(_ context.Context, tenantID, serviceID string) (*domain.DomainService, error) {
	if s.service == nil || s.service.TenantID != tenantID || s.service.ID != serviceID {
		return nil, nil
	}
	return s.service, nil
}

func serviceEventPayload(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(domain.EventEnvelope{
		ID: "source-event-1", Type: "orders.created", TenantID: "tenant-a",
		PartitionKey: "order-1", OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"order_id":"order-1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDomainServiceSenderUsesStableEffectAndSourceEventIdentity(t *testing.T) {
	var received int
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		received++
		if got := r.Header.Get("X-FlowRulz-Effect-ID"); got != "outbox-effect-9" {
			t.Errorf("effect ID header = %q", got)
		}
		if got := r.Header.Get("X-FlowRulz-Event-ID"); got != "source-event-1" {
			t.Errorf("event ID header = %q", got)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "effect-outbox-effect-9" {
			t.Errorf("idempotency key = %q", got)
		}
		if got := r.Header.Get("X-FlowRulz-Tenant-ID"); got != "tenant-a" {
			t.Errorf("tenant ID header = %q", got)
		}
	}))
	defer server.Close()

	sender := NewDomainServiceSender(domainServiceRepositoryStub{service: &domain.DomainService{
		ID: "service-a", TenantID: "tenant-a", Active: true, EndpointURL: server.URL,
	}}, true)
	err := sender.Send(context.Background(), &domain.Effect{
		ID: "outbox-effect-9", TenantID: "tenant-a", Destination: "service-a",
		Name: "orders.created", EffectType: domain.EffectTypeServiceEvent, Payload: serviceEventPayload(t),
	}, "effect-outbox-effect-9")
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if received != 1 {
		t.Fatalf("callback count = %d, want 1", received)
	}
}

func TestDomainServiceSenderDoesNotFollowRedirects(t *testing.T) {
	var redirected int
	destination := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected++
	}))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()

	sender := NewDomainServiceSender(domainServiceRepositoryStub{service: &domain.DomainService{
		ID: "service-a", TenantID: "tenant-a", Active: true, EndpointURL: redirect.URL,
	}}, true)
	err := sender.Send(context.Background(), &domain.Effect{
		ID: "effect-1", TenantID: "tenant-a", Destination: "service-a",
		Name: "orders.created", EffectType: domain.EffectTypeServiceEvent, Payload: serviceEventPayload(t),
	}, "effect-effect-1")
	if err == nil || !strings.Contains(err.Error(), "status 307") {
		t.Fatalf("Send() error = %v, want redirect status error", err)
	}
	if redirected != 0 {
		t.Fatalf("redirect destination received %d requests, want 0", redirected)
	}
}

func TestDomainServiceSenderHonorsCallerDeadline(t *testing.T) {
	handlerStarted := make(chan struct{})
	handlerExited := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(handlerStarted)
		defer close(handlerExited)

		// The handler must never hold httptest.Server.Close hostage. Client
		// cancellation should cancel the request context, but a bounded
		// fallback also keeps the test deterministic if transport behavior
		// changes.
		select {
		case <-r.Context().Done():
		case <-time.After(time.Second):
		}
	}))
	defer server.Close()

	sender := NewDomainServiceSender(domainServiceRepositoryStub{service: &domain.DomainService{
		ID: "service-a", TenantID: "tenant-a", Active: true, EndpointURL: server.URL,
	}}, true)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- sender.Send(ctx, &domain.Effect{
			ID: "effect-1", TenantID: "tenant-a", Destination: "service-a",
			Name: "orders.created", EffectType: domain.EffectTypeServiceEvent, Payload: serviceEventPayload(t),
		}, "effect-effect-1")
	}()

	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTP handler did not start before timeout")
	}

	select {
	case err := <-sendDone:
		if err == nil {
			t.Fatal("Send() error = nil, want caller deadline error")
		}
	case <-time.After(time.Second):
		t.Fatal("Send() did not return promptly after caller deadline")
	}

	select {
	case <-handlerExited:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP handler did not exit after cancellation")
	}
}

func TestIsPublicIPRejectsSpecialUseRanges(t *testing.T) {
	tests := []struct {
		name string
		ip   string
		want bool
	}{
		{name: "public ipv4", ip: "1.1.1.1", want: true},
		{name: "private ipv4", ip: "10.0.0.1"},
		{name: "loopback ipv4", ip: "127.0.0.1"},
		{name: "link local ipv4", ip: "169.254.169.254"},
		{name: "carrier grade nat", ip: "100.64.0.1"},
		{name: "documentation ipv4", ip: "198.51.100.7"},
		{name: "reserved ipv4", ip: "240.0.0.1"},
		{name: "public ipv6", ip: "2606:4700:4700::1111", want: true},
		{name: "unique local ipv6", ip: "fd00::1"},
		{name: "documentation ipv6", ip: "2001:db8::1"},
		{name: "nat64 translation", ip: "64:ff9b::a00:1"},
		{name: "ipv4 mapped loopback", ip: "::ffff:127.0.0.1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ip := net.ParseIP(test.ip)
			if got := isPublicIP(ip); got != test.want {
				t.Fatalf("isPublicIP(%s) = %t, want %t", test.ip, got, test.want)
			}
		})
	}
}

func TestDialDomainServiceRejectsNonPublicDNSAnswers(t *testing.T) {
	_, err := dialDomainService(context.Background(), "tcp", "localhost:80", false)
	if err == nil || !strings.Contains(err.Error(), "non-public address") {
		t.Fatalf("dialDomainService() error = %v, want non-public address rejection", err)
	}
}
