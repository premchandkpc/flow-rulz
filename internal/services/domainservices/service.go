// Package domainservices manages tenant domains and services.
// It provides CRUD operations for domains and services, and validates
// event types and tenant-scoped NATS delivery identities.
package domainservices

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// ErrInvalidInput indicates invalid input for domain or service operations.
var ErrInvalidInput = errors.New("invalid domain service input")

// ErrNotFound indicates a domain or service was not found.
var ErrNotFound = errors.New("domain or service not found")

// ErrConflict indicates a domain or service name already exists.
var ErrConflict = errors.New("domain or service name already exists")

var (
	namePattern      = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	eventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
)

const (
	maxEventTypesPerDomain = 100
	maxDomainsPerService   = 25
)

// Service manages tenant domains and services.
// It handles CRUD operations, validation, and event type subscription queries.
type Service struct {
	repository ports.DomainServiceRepository
	clock      ports.Clock
}

// NewService creates a new domain Service with the given repository and clock.
func NewService(repository ports.DomainServiceRepository, clock ports.Clock) *Service {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	return &Service{repository: repository, clock: clock}
}

// SaveDomain creates or updates a tenant domain.
func (s *Service) SaveDomain(ctx context.Context, tenantID string, item *domain.TenantDomain) (*domain.TenantDomain, error) {
	if err := normalizeAndValidateDomain(item, tenantID); err != nil {
		return nil, err
	}
	if err := s.ensureDomainNameAvailable(ctx, item); err != nil {
		return nil, err
	}
	if err := s.initializeDomainIdentity(ctx, tenantID, item); err != nil {
		return nil, err
	}
	if err := s.persistDomain(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func normalizeAndValidateDomain(item *domain.TenantDomain, tenantID string) error {
	if item == nil {
		return fmt.Errorf("%w: domain is required", ErrInvalidInput)
	}
	item.TenantID = tenantID
	item.Name = strings.TrimSpace(item.Name)
	item.EventTypes = uniqueTrimmed(item.EventTypes)
	if !validTenantID(tenantID) || !namePattern.MatchString(item.Name) ||
		len(item.EventTypes) == 0 || len(item.EventTypes) > maxEventTypesPerDomain {
		return fmt.Errorf("%w: domain name and 1-%d event_types are required", ErrInvalidInput, maxEventTypesPerDomain)
	}
	for _, eventType := range item.EventTypes {
		if !eventTypePattern.MatchString(eventType) {
			return fmt.Errorf("%w: invalid event type %q", ErrInvalidInput, eventType)
		}
	}
	return nil
}

func (s *Service) ensureDomainNameAvailable(ctx context.Context, item *domain.TenantDomain) error {
	existingDomains, err := s.repository.ListDomains(ctx, item.TenantID)
	if err != nil {
		return fmt.Errorf("check tenant domain names: %w", err)
	}
	for _, existing := range existingDomains {
		if existing.ID != item.ID && existing.Name == item.Name {
			return fmt.Errorf("%w: domain name %q already exists", ErrConflict, item.Name)
		}
	}
	return nil
}

func (s *Service) initializeDomainIdentity(ctx context.Context, tenantID string, item *domain.TenantDomain) error {
	if item.ID == "" {
		item.ID = domain.NewID()
		return nil
	}
	existing, err := s.repository.GetDomain(ctx, tenantID, item.ID)
	if err != nil {
		return fmt.Errorf("get tenant domain before update: %w", err)
	}
	if existing == nil {
		return ErrNotFound
	}
	item.CreatedAt = existing.CreatedAt
	return nil
}

func (s *Service) persistDomain(ctx context.Context, item *domain.TenantDomain) error {
	now := s.clock.Now()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if err := s.repository.SaveDomain(ctx, item); err != nil {
		if errors.Is(err, domain.ErrDomainServiceConflict) {
			return fmt.Errorf("%w: domain name %q already exists", ErrConflict, item.Name)
		}
		return fmt.Errorf("save tenant domain: %w", err)
	}
	return nil
}

// GetDomain retrieves a tenant domain by ID.
func (s *Service) GetDomain(ctx context.Context, tenantID, domainID string) (*domain.TenantDomain, error) {
	item, err := s.repository.GetDomain(ctx, tenantID, domainID)
	if err != nil {
		return nil, fmt.Errorf("get tenant domain: %w", err)
	}
	if item == nil {
		return nil, ErrNotFound
	}
	return item, nil
}

// ListDomains returns all domains for a tenant.
func (s *Service) ListDomains(ctx context.Context, tenantID string) ([]*domain.TenantDomain, error) {
	items, err := s.repository.ListDomains(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tenant domains: %w", err)
	}
	return items, nil
}

// DeleteDomain removes a tenant domain.
func (s *Service) DeleteDomain(ctx context.Context, tenantID, domainID string) error {
	if _, err := s.GetDomain(ctx, tenantID, domainID); err != nil {
		return err
	}
	if err := s.repository.DeleteDomain(ctx, tenantID, domainID); err != nil {
		return fmt.Errorf("delete tenant domain: %w", err)
	}
	return nil
}

// SaveService creates or updates a domain service.
func (s *Service) SaveService(ctx context.Context, tenantID string, item *domain.DomainService) (*domain.DomainService, error) {
	if err := normalizeAndValidateService(item, tenantID); err != nil {
		return nil, err
	}
	if err := s.validateServiceDomains(ctx, tenantID, item.DomainIDs); err != nil {
		return nil, err
	}
	if err := s.ensureServiceNameAvailable(ctx, item); err != nil {
		return nil, err
	}
	if err := s.initializeServiceIdentity(ctx, tenantID, item); err != nil {
		return nil, err
	}
	if err := s.persistService(ctx, item); err != nil {
		return nil, err
	}
	item.EndpointURL = ""
	item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	return item, nil
}

func normalizeAndValidateService(item *domain.DomainService, tenantID string) error {
	if item == nil {
		return fmt.Errorf("%w: service is required", ErrInvalidInput)
	}
	item.TenantID = tenantID
	item.Name = strings.TrimSpace(item.Name)
	item.EndpointURL = strings.TrimSpace(item.EndpointURL)
	item.DomainIDs = uniqueTrimmed(item.DomainIDs)
	if !validTenantID(tenantID) || !namePattern.MatchString(item.Name) || len(item.DomainIDs) > maxDomainsPerService {
		return fmt.Errorf("%w: service name and at most %d domain_ids are required", ErrInvalidInput, maxDomainsPerService)
	}
	if item.EndpointURL != "" {
		return fmt.Errorf("%w: endpoint_url HTTP callbacks are no longer supported; FlowRulz assigns a NATS subject", ErrInvalidInput)
	}
	return nil
}

func (s *Service) validateServiceDomains(ctx context.Context, tenantID string, domainIDs []string) error {
	for _, domainID := range domainIDs {
		if _, err := s.GetDomain(ctx, tenantID, domainID); err != nil {
			if errors.Is(err, ErrNotFound) {
				return fmt.Errorf("%w: domain %q does not belong to this tenant", ErrInvalidInput, domainID)
			}
			return err
		}
	}
	return nil
}

func (s *Service) ensureServiceNameAvailable(ctx context.Context, item *domain.DomainService) error {
	existingServices, err := s.repository.ListServices(ctx, item.TenantID)
	if err != nil {
		return fmt.Errorf("check tenant service names: %w", err)
	}
	for _, existing := range existingServices {
		if existing.ID != item.ID && existing.Name == item.Name {
			return fmt.Errorf("%w: service name %q already exists", ErrConflict, item.Name)
		}
	}
	return nil
}

func (s *Service) initializeServiceIdentity(ctx context.Context, tenantID string, item *domain.DomainService) error {
	if item.ID == "" {
		item.ID = domain.NewID()
		item.Active = true
		return nil
	}
	existing, err := s.repository.GetService(ctx, tenantID, item.ID)
	if err != nil {
		return fmt.Errorf("get tenant service before update: %w", err)
	}
	if existing == nil {
		return ErrNotFound
	}
	item.CreatedAt = existing.CreatedAt
	// Ordinary updates must not silently reactivate a disabled service.
	item.Active = existing.Active
	return nil
}

func (s *Service) persistService(ctx context.Context, item *domain.DomainService) error {
	now := s.clock.Now()
	if item.CreatedAt.IsZero() {
		item.CreatedAt = now
	}
	item.UpdatedAt = now
	if err := s.repository.SaveService(ctx, item); err != nil {
		if errors.Is(err, domain.ErrDomainServiceConflict) {
			return fmt.Errorf("%w: service name %q already exists", ErrConflict, item.Name)
		}
		return fmt.Errorf("save tenant service: %w", err)
	}
	return nil
}

// GetService retrieves a domain service by ID.
func (s *Service) GetService(ctx context.Context, tenantID, serviceID string) (*domain.DomainService, error) {
	item, err := s.repository.GetService(ctx, tenantID, serviceID)
	if err != nil {
		return nil, fmt.Errorf("get tenant service: %w", err)
	}
	if item == nil {
		return nil, ErrNotFound
	}
	item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	return item, nil
}

// ListServices returns all services for a tenant.
func (s *Service) ListServices(ctx context.Context, tenantID string) ([]*domain.DomainService, error) {
	items, err := s.repository.ListServices(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tenant services: %w", err)
	}
	for _, item := range items {
		item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	}
	return items, nil
}

// DeleteService removes a domain service.
func (s *Service) DeleteService(ctx context.Context, tenantID, serviceID string) error {
	if _, err := s.GetService(ctx, tenantID, serviceID); err != nil {
		return err
	}
	if err := s.repository.DeleteService(ctx, tenantID, serviceID); err != nil {
		return fmt.Errorf("deactivate tenant service: %w", err)
	}
	return nil
}

// SubscribersForEvent returns services subscribed to a specific event type.
func (s *Service) SubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error) {
	items, err := s.repository.ListSubscribersForEvent(ctx, tenantID, eventType)
	if err != nil {
		return nil, fmt.Errorf("list event subscribers: %w", err)
	}
	if len(items) > 100 {
		return nil, fmt.Errorf("event %q exceeds the 100-service fan-out limit", eventType)
	}
	for _, item := range items {
		item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	}
	return items, nil
}


func validTenantID(tenantID string) bool {
	if tenantID == "" || len(tenantID) > 128 {
		return false
	}
	for _, r := range tenantID {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func uniqueTrimmed(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
