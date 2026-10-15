// Package domainservices manages domain configuration and service ruleset assignments.
package domainservices

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	maxDomainsPerService = 25
)

// Service manages domain configuration and service ruleset assignments.
type Service struct {
	repository  ports.DomainServiceRepository
	clock       ports.Clock
	ruleService ports.RuleService
}

// NewService creates a new domain Service with the given repository and clock.
func NewService(repository ports.DomainServiceRepository, clock ports.Clock) *Service {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	return &Service{repository: repository, clock: clock}
}

// WithRuleService enables validation that Service assignments reference immutable ruleset revisions.
func (s *Service) WithRuleService(ruleService ports.RuleService) *Service {
	s.ruleService = ruleService
	return s
}

// SaveDomain creates or updates a Domain configuration.
func (s *Service) SaveDomain(ctx context.Context, _ string, item *domain.TenantDomain) (*domain.TenantDomain, error) {
	scopeID := domain.DomainConfigurationScope
	if err := normalizeAndValidateDomain(item, scopeID); err != nil {
		return nil, err
	}
	if err := s.validateAssignedRulesets(ctx, item); err != nil {
		return nil, err
	}
	if err := s.ensureDomainNameAvailable(ctx, item); err != nil {
		return nil, err
	}
	if err := s.initializeDomainIdentity(ctx, scopeID, item); err != nil {
		return nil, err
	}
	if err := s.validateDeploymentRevisionChanges(ctx, scopeID, item); err != nil {
		return nil, err
	}
	if err := s.persistDomain(ctx, item); err != nil {
		return nil, err
	}
	return item, nil
}

func (s *Service) validateAssignedRulesets(ctx context.Context, item *domain.TenantDomain) error {
	if s.ruleService == nil {
		return nil
	}
	for _, service := range item.Deployments {
		for _, assignment := range service.Rulesets {
			revision, err := s.ruleService.GetRevision(ctx, domain.DomainConfigurationScope, assignment.RulesetID, assignment.Revision)
			if err != nil {
				return fmt.Errorf("%w: resolve ruleset %q revision %d: %w", ErrInvalidInput, assignment.RulesetID, assignment.Revision, err)
			}
			if revision == nil || revision.TenantScope != domain.DomainConfigurationScope ||
				revision.RuleID != assignment.RulesetID || revision.Revision != assignment.Revision {
				return fmt.Errorf("%w: ruleset %q revision %d is not available in global configuration", ErrInvalidInput, assignment.RulesetID, assignment.Revision)
			}
		}
	}
	return nil
}

func (s *Service) validateDeploymentRevisionChanges(ctx context.Context, scopeID string, item *domain.TenantDomain) error {
	existing, err := s.repository.GetDomain(ctx, scopeID, item.ID)
	if err != nil {
		return fmt.Errorf("load existing domain deployment revisions: %w", err)
	}
	if existing == nil {
		return nil
	}
	previous := make(map[string]domain.ServiceDeployment, len(existing.Deployments))
	for _, deployment := range existing.Deployments {
		previous[deployment.ServiceID] = deployment
	}
	currentIDs := make(map[string]struct{}, len(item.Deployments))
	for _, deployment := range item.Deployments {
		currentIDs[deployment.ServiceID] = struct{}{}
		old, found := previous[deployment.ServiceID]
		if !found {
			continue
		}
		if deployment.Revision < old.Revision {
			return fmt.Errorf("%w: deployment %q revision cannot move backwards from %d to %d", ErrInvalidInput, deployment.ServiceID, old.Revision, deployment.Revision)
		}
		old.Revision = 0
		current := deployment
		current.Revision = 0
		if !reflect.DeepEqual(old, current) && deployment.Revision <= previous[deployment.ServiceID].Revision {
			return fmt.Errorf("%w: changing deployment %q assignments or worker profile requires a higher revision", ErrInvalidInput, deployment.ServiceID)
		}
	}
	for serviceID, old := range previous {
		if _, present := currentIDs[serviceID]; !present && old.Active {
			return fmt.Errorf("%w: active deployment %q must be deactivated with a higher revision before removal", ErrInvalidInput, serviceID)
		}
	}
	return nil
}

func normalizeAndValidateDomain(item *domain.TenantDomain, scopeID string) error {
	if item == nil {
		return fmt.Errorf("%w: domain is required", ErrInvalidInput)
	}
	item.TenantID = scopeID
	item.Name = strings.TrimSpace(item.Name)
	// Event type filters belong to subscriber registrations, not Domain configuration.
	item.EventTypes = []string{}
	if !validIdentifier(scopeID) || !namePattern.MatchString(item.Name) {
		return fmt.Errorf("%w: a valid domain name is required", ErrInvalidInput)
	}
	// Event type, tenant identity, and partition key are supplied by each event.
	if err := validateDeployments(item.Deployments); err != nil {
		return err
	}
	return nil
}

func validateDeployments(deployments []domain.ServiceDeployment) error {
	serviceIDs := make(map[string]struct{}, len(deployments))
	serviceNames := make(map[string]struct{}, len(deployments))
	for _, deployment := range deployments {
		if !namePattern.MatchString(deployment.ServiceID) || !namePattern.MatchString(deployment.Name) || deployment.Revision <= 0 {
			return fmt.Errorf("%w: deployment service_id/name and positive revision are required", ErrInvalidInput)
		}
		if _, exists := serviceIDs[deployment.ServiceID]; exists {
			return fmt.Errorf("%w: duplicate deployment service_id %q", ErrInvalidInput, deployment.ServiceID)
		}
		if _, exists := serviceNames[deployment.Name]; exists {
			return fmt.Errorf("%w: duplicate deployment name %q", ErrInvalidInput, deployment.Name)
		}
		serviceIDs[deployment.ServiceID] = struct{}{}
		serviceNames[deployment.Name] = struct{}{}
		if len(deployment.Rulesets) == 0 || len(deployment.Rulesets) > 500 {
			return fmt.Errorf("%w: deployment %q must assign 1-500 ruleset revisions", ErrInvalidInput, deployment.ServiceID)
		}
		seenRulesets := make(map[string]struct{}, len(deployment.Rulesets))
		for _, assignment := range deployment.Rulesets {
			if strings.TrimSpace(assignment.RulesetID) == "" || len(assignment.RulesetID) > 128 || !validIdentifier(assignment.RulesetID) || assignment.Revision <= 0 {
				return fmt.Errorf("%w: deployment %q contains an invalid ruleset assignment", ErrInvalidInput, deployment.ServiceID)
			}
			if _, exists := seenRulesets[assignment.RulesetID]; exists {
				return fmt.Errorf("%w: deployment %q assigns ruleset %q more than once", ErrInvalidInput, deployment.ServiceID, assignment.RulesetID)
			}
			seenRulesets[assignment.RulesetID] = struct{}{}
		}
		profile := deployment.WorkerProfile
		if profile.MinReplicas < 0 || profile.MaxReplicas < 0 || profile.Concurrency < 0 ||
			(profile.MaxReplicas > 0 && profile.MinReplicas > profile.MaxReplicas) {
			return fmt.Errorf("%w: deployment %q has an invalid worker_profile", ErrInvalidInput, deployment.ServiceID)
		}
	}
	return nil
}

func (s *Service) ensureDomainNameAvailable(ctx context.Context, item *domain.TenantDomain) error {
	existingDomains, err := s.repository.ListDomains(ctx, item.TenantID)
	if err != nil {
		return fmt.Errorf("check domain names: %w", err)
	}
	for _, existing := range existingDomains {
		if existing.ID != item.ID && existing.Name == item.Name {
			return fmt.Errorf("%w: domain name %q already exists", ErrConflict, item.Name)
		}
	}
	return nil
}

func (s *Service) initializeDomainIdentity(ctx context.Context, scopeID string, item *domain.TenantDomain) error {
	if item.ID == "" {
		item.ID = domain.NewID()
		return nil
	}
	existing, err := s.repository.GetDomain(ctx, scopeID, item.ID)
	if err != nil {
		return fmt.Errorf("get domain before update: %w", err)
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
		return fmt.Errorf("save domain: %w", err)
	}
	return nil
}

// GetDomain retrieves a Domain by ID.
func (s *Service) GetDomain(ctx context.Context, _ string, domainID string) (*domain.TenantDomain, error) {
	item, err := s.repository.GetDomain(ctx, domain.DomainConfigurationScope, domainID)
	if err != nil {
		return nil, fmt.Errorf("get domain: %w", err)
	}
	if item == nil {
		return nil, ErrNotFound
	}
	return item, nil
}

// ListDomains returns all configured Domains.
func (s *Service) ListDomains(ctx context.Context, _ string) ([]*domain.TenantDomain, error) {
	items, err := s.repository.ListDomains(ctx, domain.DomainConfigurationScope)
	if err != nil {
		return nil, fmt.Errorf("list domains: %w", err)
	}
	return items, nil
}

// DeleteDomain removes a Domain configuration.
func (s *Service) DeleteDomain(ctx context.Context, _ string, domainID string) error {
	if _, err := s.GetDomain(ctx, domain.DomainConfigurationScope, domainID); err != nil {
		return err
	}
	if err := s.repository.DeleteDomain(ctx, domain.DomainConfigurationScope, domainID); err != nil {
		return fmt.Errorf("delete domain: %w", err)
	}
	return nil
}

// SaveService creates or updates a domain service.
func (s *Service) SaveService(ctx context.Context, _ string, item *domain.DomainService) (*domain.DomainService, error) {
	scopeID := domain.DomainConfigurationScope
	if err := normalizeAndValidateService(item, scopeID); err != nil {
		return nil, err
	}
	if len(item.DomainIDs) > 0 {
		if err := s.validateServiceDomains(ctx, scopeID, item.DomainIDs); err != nil {
			return nil, err
		}
	}
	if err := s.ensureServiceNameAvailable(ctx, item); err != nil {
		return nil, err
	}
	if err := s.initializeServiceIdentity(ctx, scopeID, item); err != nil {
		return nil, err
	}
	if err := s.persistService(ctx, item); err != nil {
		return nil, err
	}
	item.EndpointURL = ""
	item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	return item, nil
}

func normalizeAndValidateService(item *domain.DomainService, scopeID string) error {
	if item == nil {
		return fmt.Errorf("%w: service is required", ErrInvalidInput)
	}
	item.TenantID = scopeID
	item.Name = strings.TrimSpace(item.Name)
	item.EndpointURL = strings.TrimSpace(item.EndpointURL)
	item.DomainIDs = uniqueTrimmed(item.DomainIDs)
	item.EventTypes = uniqueTrimmed(item.EventTypes)
	if item.EventTypes == nil {
		item.EventTypes = []string{}
	}
	if !validIdentifier(scopeID) || !namePattern.MatchString(item.Name) || len(item.DomainIDs) > maxDomainsPerService {
		return fmt.Errorf("%w: a valid subscription name and at most %d legacy domain_ids are allowed", ErrInvalidInput, maxDomainsPerService)
	}
	if len(item.EventTypes) == 0 && len(item.DomainIDs) == 0 {
		return fmt.Errorf("%w: at least one event_type is required", ErrInvalidInput)
	}
	for _, eventType := range item.EventTypes {
		if !eventTypePattern.MatchString(eventType) {
			return fmt.Errorf("%w: invalid subscription event type %q", ErrInvalidInput, eventType)
		}
	}
	if item.EndpointURL != "" {
		return fmt.Errorf("%w: endpoint_url HTTP callbacks are no longer supported; FlowRulz assigns a NATS subject", ErrInvalidInput)
	}
	return nil
}

func (s *Service) validateServiceDomains(ctx context.Context, scopeID string, domainIDs []string) error {
	for _, domainID := range domainIDs {
		if _, err := s.GetDomain(ctx, scopeID, domainID); err != nil {
			if errors.Is(err, ErrNotFound) {
				return fmt.Errorf("%w: domain %q does not exist", ErrInvalidInput, domainID)
			}
			return err
		}
	}
	return nil
}

func (s *Service) ensureServiceNameAvailable(ctx context.Context, item *domain.DomainService) error {
	existingServices, err := s.repository.ListServices(ctx, item.TenantID)
	if err != nil {
		return fmt.Errorf("check subscription names: %w", err)
	}
	for _, existing := range existingServices {
		if existing.ID != item.ID && existing.Name == item.Name {
			return fmt.Errorf("%w: subscription name %q already exists", ErrConflict, item.Name)
		}
	}
	return nil
}

func (s *Service) initializeServiceIdentity(ctx context.Context, scopeID string, item *domain.DomainService) error {
	if item.ID == "" {
		item.ID = domain.NewID()
		item.Active = true
		return nil
	}
	existing, err := s.repository.GetService(ctx, scopeID, item.ID)
	if err != nil {
		return fmt.Errorf("get subscription before update: %w", err)
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
			return fmt.Errorf("%w: subscription name %q already exists", ErrConflict, item.Name)
		}
		return fmt.Errorf("save subscription: %w", err)
	}
	return nil
}

// GetService retrieves a domain service by ID.
func (s *Service) GetService(ctx context.Context, _ string, serviceID string) (*domain.DomainService, error) {
	item, err := s.repository.GetService(ctx, domain.DomainConfigurationScope, serviceID)
	if err != nil {
		return nil, fmt.Errorf("get subscription: %w", err)
	}
	if item == nil {
		return nil, ErrNotFound
	}
	item.EndpointURL = ""
	item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	return item, nil
}

// ListServices returns all services for a tenant.
func (s *Service) ListServices(ctx context.Context, _ string) ([]*domain.DomainService, error) {
	items, err := s.repository.ListServices(ctx, domain.DomainConfigurationScope)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	for _, item := range items {
		item.EndpointURL = ""
		item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	}
	return items, nil
}

// DeleteService removes a domain service.
func (s *Service) DeleteService(ctx context.Context, _ string, serviceID string) error {
	if _, err := s.GetService(ctx, domain.DomainConfigurationScope, serviceID); err != nil {
		return err
	}
	if err := s.repository.DeleteService(ctx, domain.DomainConfigurationScope, serviceID); err != nil {
		return fmt.Errorf("deactivate subscription: %w", err)
	}
	return nil
}

// SubscribersForEvent returns services subscribed to a specific event type.
func (s *Service) SubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error) {
	items, err := s.repository.ListSubscribersForEvent(ctx, domain.DomainConfigurationScope, eventType)
	if err != nil {
		return nil, fmt.Errorf("list event subscribers: %w", err)
	}
	if len(items) > 100 {
		return nil, fmt.Errorf("event %q exceeds the 100-service fan-out limit", eventType)
	}
	for _, item := range items {
		item.EndpointURL = ""
		item.NATSSubject = domain.ServiceSubject(item.TenantID, item.ID)
	}
	return items, nil
}

func validIdentifier(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
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
