package events

import (
	"context"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type globalSubscriberRepositoryStub struct {
	ports.SubscriberRepository
	items []*domain.DomainService
	err error
}

func (s globalSubscriberRepositoryStub) ListSubscribersForEvent(context.Context, string, string) ([]*domain.DomainService, error) {
	return s.items, s.err
}

func TestSubscriberRouterIncludesGlobalAndMatchingLegacySubscriptions(t *testing.T) {
	router := SubscriberRouter{}
	repo := globalSubscriberRepositoryStub{items: []*domain.DomainService{
		{ID: "global-audit", TenantID: domain.DomainConfigurationScope, Active: true},
		{ID: "tenant-a-consumer", TenantID: "tenant-a", Active: true},
		{ID: "tenant-b-consumer", TenantID: "tenant-b", Active: true},
		{ID: "disabled", TenantID: domain.DomainConfigurationScope, Active: false},
		nil,
	}}
	event := &domain.EventEnvelope{
		ID: "event-1", Type: "orders.created", TenantID: "tenant-a", PartitionKey: "order-1",
		OccurredAt: time.Unix(10, 0).UTC(), Data: []byte(`{"order_id":"order-1"}`),
	}
	effects, err := router.Route(context.Background(), repo, event, "execution-1", time.Unix(11, 0).UTC())
	if err != nil {
		t.Fatalf("Route() error = %v", err)
	}
	if len(effects) != 2 {
		t.Fatalf("effects count = %d, want global and same-tenant subscriptions", len(effects))
	}
	if effects[0].Destination != "global-audit" || effects[1].Destination != "tenant-a-consumer" {
		t.Fatalf("subscription destinations = %#v", []string{effects[0].Destination, effects[1].Destination})
	}
	for _, effect := range effects {
		if effect.TenantID != event.TenantID || effect.Name != event.Type || effect.ExecutionID != "execution-1" {
			t.Fatalf("delivery effect lost event identity: %#v", effect)
		}
		if effect.ID == "" || effect.Status != domain.OutboxStatusPending {
			t.Fatalf("delivery effect identity/status invalid: %#v", effect)
		}
	}
}

func TestSubscriberRouterPropagatesRepositoryErrors(t *testing.T) {
	repoErr := context.DeadlineExceeded
	event := &domain.EventEnvelope{ID: "event-1", Type: "orders.created", TenantID: "tenant-a", PartitionKey: "order-1"}
	_, err := (SubscriberRouter{}).Route(context.Background(), globalSubscriberRepositoryStub{err: repoErr}, event, "execution-1", time.Now())
	if err == nil {
		t.Fatal("Route() error = nil, want repository error")
	}
}
