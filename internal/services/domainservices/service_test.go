package domainservices

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type repositoryStub struct {
	ports.DomainServiceRepository
	domains  map[string]*domain.TenantDomain
	services map[string]*domain.DomainService
}

func newRepositoryStub() *repositoryStub {
	return &repositoryStub{
		domains:  make(map[string]*domain.TenantDomain),
		services: make(map[string]*domain.DomainService),
	}
}

func cloneTenantDomain(item *domain.TenantDomain) *domain.TenantDomain {
	if item == nil {
		return nil
	}
	cloned := *item
	cloned.EventTypes = append([]string(nil), item.EventTypes...)
	cloned.Deployments = append([]domain.ServiceDeployment(nil), item.Deployments...)
	for i := range cloned.Deployments {
		cloned.Deployments[i].Rulesets = append([]domain.RulesetAssignment(nil), item.Deployments[i].Rulesets...)
	}
	return &cloned
}

func (r *repositoryStub) SaveDomain(_ context.Context, item *domain.TenantDomain) error {
	r.domains[item.TenantID+"/"+item.ID] = cloneTenantDomain(item)
	return nil
}

func (r *repositoryStub) GetDomain(_ context.Context, tenantID, domainID string) (*domain.TenantDomain, error) {
	item := r.domains[tenantID+"/"+domainID]
	return cloneTenantDomain(item), nil
}

func (r *repositoryStub) ListDomains(_ context.Context, tenantID string) ([]*domain.TenantDomain, error) {
	items := make([]*domain.TenantDomain, 0)
	for _, item := range r.domains {
		if item.TenantID == tenantID {
			items = append(items, cloneTenantDomain(item))
		}
	}
	return items, nil
}

func (r *repositoryStub) SaveService(_ context.Context, item *domain.DomainService) error {
	cloned := *item
	r.services[item.TenantID+"/"+item.ID] = &cloned
	return nil
}

func (r *repositoryStub) GetService(_ context.Context, tenantID, serviceID string) (*domain.DomainService, error) {
	item := r.services[tenantID+"/"+serviceID]
	if item == nil {
		return nil, nil
	}
	cloned := *item
	return &cloned, nil
}

func (r *repositoryStub) ListServices(_ context.Context, tenantID string) ([]*domain.DomainService, error) {
	items := make([]*domain.DomainService, 0)
	for _, item := range r.services {
		if item.TenantID == tenantID {
			cloned := *item
			items = append(items, &cloned)
		}
	}
	return items, nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }

func TestSaveDomainValidatesInputsAndTenantScopedNames(t *testing.T) {
	repo := newRepositoryStub()
	service := NewService(repo, fixedClock{now: time.Unix(10, 0).UTC()})
	item, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{" orders.created ", "orders.created"},
	})
	if err != nil {
		t.Fatalf("SaveDomain() error = %v", err)
	}
	if item.TenantID != "tenant-a" || len(item.EventTypes) != 1 || item.EventTypes[0] != "orders.created" {
		t.Fatalf("normalized domain = %#v", item)
	}
	if _, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{"orders.updated"},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate SaveDomain() error = %v, want ErrConflict", err)
	}
	if _, err := service.SaveDomain(context.Background(), "tenant-b", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{"orders.created"},
	}); err != nil {
		t.Fatalf("same domain name in separate tenant: %v", err)
	}
	if _, err := service.SaveDomain(context.Background(), "bad\ntenant", &domain.TenantDomain{
		Name: "bad-domain", EventTypes: []string{"orders.created"},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid tenant ID error = %v, want ErrInvalidInput", err)
	}
	if _, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "invalid", EventTypes: []string{"Invalid.Event"},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid event type error = %v, want ErrInvalidInput", err)
	}
}

func TestSaveServiceRequiresTenantOwnedDomainsAndRejectsHTTPCallbacks(t *testing.T) {
	repo := newRepositoryStub()
	service := NewService(repo, fixedClock{now: time.Unix(20, 0).UTC()})
	domainA, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{"orders.created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SaveService(context.Background(), "tenant-a", &domain.DomainService{
		Name: "receiver", EndpointURL: "https://callback.example/hook", DomainIDs: []string{domainA.ID},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("HTTP callback error = %v, want ErrInvalidInput", err)
	}
	if _, err := service.SaveService(context.Background(), "tenant-b", &domain.DomainService{
		Name: "receiver", DomainIDs: []string{domainA.ID},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-tenant domain subscription error = %v, want ErrInvalidInput", err)
	}
}

func TestSaveServiceAssignsStableNATSSubjectAndRejectsDuplicateName(t *testing.T) {
	repo := newRepositoryStub()
	service := NewService(repo, fixedClock{now: time.Unix(30, 0).UTC()})
	item, err := service.SaveService(context.Background(), "tenant-a", &domain.DomainService{
		Name: "receiver", EventTypes: []string{"orders.created"},
	})
	if err != nil {
		t.Fatalf("SaveService() error = %v", err)
	}
	if !item.Active || item.ID == "" {
		t.Fatalf("new service must be active and identified: %#v", item)
	}
	if want := domain.ServiceSubject("tenant-a", item.ID); item.NATSSubject != want {
		t.Fatalf("NATS subject = %q, want %q", item.NATSSubject, want)
	}
	if _, err := service.SaveService(context.Background(), "tenant-a", &domain.DomainService{
		Name: "receiver", EventTypes: []string{"orders.created"},
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate service name error = %v, want ErrConflict", err)
	}
}

func TestSaveSubscriptionUsesEventTypeFiltersWithoutTenantConfig(t *testing.T) {
	repo := newRepositoryStub()
	service := NewService(repo, fixedClock{now: time.Unix(80, 0).UTC()})
	item, err := service.SaveService(context.Background(), domain.DomainConfigurationScope, &domain.DomainService{
		Name: "audit-consumer",
		EventTypes: []string{" orders.created ", "orders.created", "payments.completed"},
	})
	if err != nil {
		t.Fatalf("SaveService() error = %v", err)
	}
	if item.TenantID != domain.DomainConfigurationScope {
		t.Fatalf("internal scope = %q, want shared config scope", item.TenantID)
	}
	if len(item.EventTypes) != 2 || item.EventTypes[0] != "orders.created" || item.EventTypes[1] != "payments.completed" {
		t.Fatalf("normalized event filters = %#v", item.EventTypes)
	}
	if item.NATSSubject == "" {
		t.Fatal("subscription must receive a stable NATS subject")
	}
}

func TestSaveSubscriptionRejectsInvalidEventType(t *testing.T) {
	service := NewService(newRepositoryStub(), fixedClock{now: time.Unix(81, 0).UTC()})
	_, err := service.SaveService(context.Background(), domain.DomainConfigurationScope, &domain.DomainService{
		Name: "audit-consumer",
		EventTypes: []string{"Invalid Event Type"},
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("SaveService() error = %v, want ErrInvalidInput", err)
	}
}
