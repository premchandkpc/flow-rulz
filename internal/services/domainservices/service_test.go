package domainservices

import (
	"context"
	"errors"
	"strings"
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

func (r *repositoryStub) SaveDomain(_ context.Context, item *domain.TenantDomain) error {
	cloned := *item
	r.domains[item.TenantID+"/"+item.ID] = &cloned
	return nil
}

func (r *repositoryStub) GetDomain(_ context.Context, tenantID, domainID string) (*domain.TenantDomain, error) {
	item := r.domains[tenantID+"/"+domainID]
	if item == nil {
		return nil, nil
	}
	cloned := *item
	return &cloned, nil
}

func (r *repositoryStub) ListDomains(_ context.Context, tenantID string) ([]*domain.TenantDomain, error) {
	items := make([]*domain.TenantDomain, 0)
	for _, item := range r.domains {
		if item.TenantID == tenantID {
			cloned := *item
			items = append(items, &cloned)
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
	service := NewService(repo, fixedClock{now: time.Unix(10, 0).UTC()}, true)
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

func TestSaveServiceRequiresTenantOwnedDomainsAndSafeEndpointScheme(t *testing.T) {
	repo := newRepositoryStub()
	service := NewService(repo, fixedClock{now: time.Unix(20, 0).UTC()}, false)
	domainA, err := service.SaveDomain(context.Background(), "tenant-a", &domain.TenantDomain{
		Name: "orders", EventTypes: []string{"orders.created"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.SaveService(context.Background(), "tenant-a", &domain.DomainService{
		Name: "receiver", EndpointURL: "http://callback.example/hook", DomainIDs: []string{domainA.ID},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("production HTTP endpoint error = %v, want ErrInvalidInput", err)
	}
	if _, err := service.SaveService(context.Background(), "tenant-b", &domain.DomainService{
		Name: "receiver", EndpointURL: "https://callback.example/hook", DomainIDs: []string{domainA.ID},
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("cross-tenant domain subscription error = %v, want ErrInvalidInput", err)
	}
	if _, err := service.SaveService(context.Background(), "tenant-a", &domain.DomainService{
		Name: "receiver", EndpointURL: "https://user:pass@callback.example/hook",
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("endpoint credentials error = %v, want ErrInvalidInput", err)
	}
}

func TestSaveServiceAllowsHTTPOnlyWhenConfiguredAndRejectsDuplicateName(t *testing.T) {
	repo := newRepositoryStub()
	service := NewService(repo, fixedClock{now: time.Unix(30, 0).UTC()}, true)
	item, err := service.SaveService(context.Background(), "tenant-a", &domain.DomainService{
		Name: "receiver", EndpointURL: "http://127.0.0.1:8080/callback",
	})
	if err != nil {
		t.Fatalf("development SaveService() error = %v", err)
	}
	if !item.Active || item.ID == "" {
		t.Fatalf("new service must be active and identified: %#v", item)
	}
	if _, err := service.SaveService(context.Background(), "tenant-a", &domain.DomainService{
		Name: "receiver", EndpointURL: "https://callback.example/hook",
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate service name error = %v, want ErrConflict", err)
	}
}

func TestValidateEndpointRejectsCredentialsFragmentsAndOversizedURLs(t *testing.T) {
	oversized := "https://" + strings.Repeat("a", 2050) + ".example"
	for _, raw := range []string{
		"https://user:pass@example.com/hook",
		"https://example.com/hook#secret",
		oversized,
	} {
		if err := validateEndpoint(raw, false); err == nil {
			t.Errorf("validateEndpoint(%q) error = nil, want invalid endpoint", raw)
		}
	}
}
