package effects

import (
	"context"
	"encoding/json"
	"errors"
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

type natsPublisherStub struct {
	subject string
	payload []byte
	err     error
}

func (p *natsPublisherStub) Publish(_ context.Context, subject string, payload []byte) error {
	p.subject = subject
	p.payload = append([]byte(nil), payload...)
	return p.err
}

func serviceEventPayload(t *testing.T) []byte {
	t.Helper()
	data, err := json.Marshal(domain.EventEnvelope{
		ID: "source-event-1", Type: "orders.created", TenantID: "tenant-a",
		PartitionKey: "order-1", OccurredAt: time.Unix(10, 0).UTC(), Data: json.RawMessage(`{"order_id":"order-1"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func TestDomainServiceSenderPublishesDurableNATSDeliveryWithStableIdentity(t *testing.T) {
	publisher := &natsPublisherStub{}
	sender := NewDomainServiceSender(domainServiceRepositoryStub{service: &domain.DomainService{
		ID: "service-a", TenantID: "tenant-a", Active: true,
	}}, publisher)
	err := sender.Send(context.Background(), &domain.Effect{
		ID: "outbox-effect-9", ExecutionID: "execution-1", TenantID: "tenant-a",
		Destination: "service-a", Name: "orders.created",
		EffectType: domain.EffectTypeServiceEvent, Payload: serviceEventPayload(t),
	}, "effect-outbox-effect-9")
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if want := domain.ServiceSubject("tenant-a", "service-a"); publisher.subject != want {
		t.Fatalf("subject = %q, want %q", publisher.subject, want)
	}
	var delivered domain.Effect
	if err := json.Unmarshal(publisher.payload, &delivered); err != nil {
		t.Fatalf("decode delivery: %v", err)
	}
	if delivered.ID != "outbox-effect-9" || delivered.IdempotencyKey != "effect-outbox-effect-9" {
		t.Fatalf("delivery identity = (%q, %q), want stable effect identity", delivered.ID, delivered.IdempotencyKey)
	}
}

func TestDomainServiceSenderRejectsInactiveOrMissingService(t *testing.T) {
	for _, service := range []*domain.DomainService{nil, {ID: "service-a", TenantID: "tenant-a", Active: false}} {
		sender := NewDomainServiceSender(domainServiceRepositoryStub{service: service}, &natsPublisherStub{})
		err := sender.Send(context.Background(), &domain.Effect{
			ID: "effect-1", TenantID: "tenant-a", Destination: "service-a", Name: "orders.created",
			EffectType: domain.EffectTypeServiceEvent, Payload: serviceEventPayload(t),
		}, "effect-effect-1")
		if err == nil {
			t.Fatal("Send() error = nil, want missing/inactive service error")
		}
	}
}

func TestDomainServiceSenderReturnsPublishErrorForOutboxRetry(t *testing.T) {
	publisher := &natsPublisherStub{err: errors.New("broker unavailable")}
	sender := NewDomainServiceSender(domainServiceRepositoryStub{service: &domain.DomainService{
		ID: "service-a", TenantID: "tenant-a", Active: true,
	}}, publisher)
	err := sender.Send(context.Background(), &domain.Effect{
		ID: "effect-1", TenantID: "tenant-a", Destination: "service-a", Name: "orders.created",
		EffectType: domain.EffectTypeServiceEvent, Payload: serviceEventPayload(t),
	}, "effect-effect-1")
	if err == nil {
		t.Fatal("Send() error = nil, want broker error")
	}
}

func TestDomainServiceSenderResolvesGlobalSubscriptionForEventTenant(t *testing.T) {
	publisher := &natsPublisherStub{}
	sender := NewDomainServiceSender(domainServiceRepositoryStub{service: &domain.DomainService{
		ID: "audit-consumer", TenantID: domain.DomainConfigurationScope, Active: true,
	}}, publisher)
	err := sender.Send(context.Background(), &domain.Effect{
		ID: "effect-global-1", ExecutionID: "execution-1", TenantID: "tenant-a",
		Destination: "audit-consumer", Name: "orders.created",
		EffectType: domain.EffectTypeServiceEvent, Payload: serviceEventPayload(t),
	}, "effect-global-1")
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if want := domain.ServiceSubject(domain.DomainConfigurationScope, "audit-consumer"); publisher.subject != want {
		t.Fatalf("subject = %q, want %q", publisher.subject, want)
	}
	var delivered domain.Effect
	if err := json.Unmarshal(publisher.payload, &delivered); err != nil {
		t.Fatalf("decode delivery: %v", err)
	}
	if delivered.TenantID != "tenant-a" {
		t.Fatalf("delivery tenant identity = %q, want event tenant tenant-a", delivered.TenantID)
	}
}
