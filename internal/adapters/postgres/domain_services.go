package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DomainServiceRepository persists tenant domains and services.
type DomainServiceRepository struct {
	querier Querier
}

// NewDomainServiceRepository creates a new DomainServiceRepository.
func NewDomainServiceRepository(querier Querier) *DomainServiceRepository {
	return &DomainServiceRepository{querier: querier}
}

// SaveDomain inserts or updates a tenant domain.
func (r *DomainServiceRepository) SaveDomain(ctx context.Context, item *domain.TenantDomain) error {
	deploymentItems := item.Deployments
	if deploymentItems == nil {
		deploymentItems = []domain.ServiceDeployment{}
	}
	deployments, err := json.Marshal(deploymentItems)
	if err != nil {
		return fmt.Errorf("encode domain service deployments: %w", err)
	}
	_, err = r.querier.Exec(ctx, `
		INSERT INTO tenant_domains (tenant_id, domain_id, name, event_types, deployments, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, domain_id) DO UPDATE SET
			name = EXCLUDED.name, event_types = EXCLUDED.event_types,
			deployments = EXCLUDED.deployments, updated_at = EXCLUDED.updated_at
	`, item.TenantID, item.ID, item.Name, item.EventTypes, deployments, item.CreatedAt, item.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: tenant domain name already exists", domain.ErrDomainServiceConflict)
		}
		return fmt.Errorf("save tenant domain: %w", err)
	}
	return nil
}

// GetDomain retrieves a tenant domain by ID.
func (r *DomainServiceRepository) GetDomain(ctx context.Context, tenantID, domainID string) (*domain.TenantDomain, error) {
	item := &domain.TenantDomain{}
	var deployments []byte
	err := r.querier.QueryRow(ctx, `
		SELECT domain_id, tenant_id, name, event_types, deployments, created_at, updated_at
		FROM tenant_domains WHERE tenant_id = $1 AND domain_id = $2
	`, tenantID, domainID).Scan(&item.ID, &item.TenantID, &item.Name, &item.EventTypes, &deployments, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get tenant domain: %w", err)
	}
	if len(deployments) > 0 {
		if err := json.Unmarshal(deployments, &item.Deployments); err != nil {
			return nil, fmt.Errorf("decode domain service deployments: %w", err)
		}
	}
	return item, nil
}

// ListDomains returns all domains for a tenant.
func (r *DomainServiceRepository) ListDomains(ctx context.Context, tenantID string) ([]*domain.TenantDomain, error) {
	rows, err := r.querier.Query(ctx, `
		SELECT domain_id, tenant_id, name, event_types, deployments, created_at, updated_at
		FROM tenant_domains WHERE tenant_id = $1 ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tenant domains: %w", err)
	}
	defer rows.Close()

	items := make([]*domain.TenantDomain, 0)
	for rows.Next() {
		item := &domain.TenantDomain{}
		var deployments []byte
		if err := rows.Scan(&item.ID, &item.TenantID, &item.Name, &item.EventTypes, &deployments, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan tenant domain: %w", err)
		}
		if len(deployments) > 0 {
			if err := json.Unmarshal(deployments, &item.Deployments); err != nil {
				return nil, fmt.Errorf("decode domain service deployments: %w", err)
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant domains: %w", err)
	}
	return items, nil
}

// DeleteDomain removes a tenant domain.
func (r *DomainServiceRepository) DeleteDomain(ctx context.Context, tenantID, domainID string) error {
	_, err := r.querier.Exec(ctx, `DELETE FROM tenant_domains WHERE tenant_id = $1 AND domain_id = $2`, tenantID, domainID)
	if err != nil {
		return fmt.Errorf("delete tenant domain: %w", err)
	}
	return nil
}

// SaveService inserts or updates a domain service.
func (r *DomainServiceRepository) SaveService(ctx context.Context, item *domain.DomainService) error {
	if pool, ok := r.querier.(*pgxpool.Pool); ok {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin save tenant service: %w", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := (&DomainServiceRepository{querier: tx}).saveService(ctx, item); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit tenant service: %w", err)
		}
		return nil
	}

	return r.saveService(ctx, item)
}

func (r *DomainServiceRepository) saveService(ctx context.Context, item *domain.DomainService) error {
	_, err := r.querier.Exec(ctx, `
		INSERT INTO tenant_domain_services (tenant_id, service_id, name, endpoint_url, active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, service_id) DO UPDATE SET
			name = EXCLUDED.name, endpoint_url = EXCLUDED.endpoint_url, active = EXCLUDED.active, updated_at = EXCLUDED.updated_at
	`, item.TenantID, item.ID, item.Name, item.EndpointURL, item.Active, item.CreatedAt, item.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: tenant service name already exists", domain.ErrDomainServiceConflict)
		}
		return fmt.Errorf("save tenant service: %w", err)
	}
	if _, err := r.querier.Exec(ctx, `DELETE FROM tenant_domain_subscriptions WHERE tenant_id = $1 AND service_id = $2`, item.TenantID, item.ID); err != nil {
		return fmt.Errorf("replace tenant service subscriptions: %w", err)
	}
	for _, domainID := range item.DomainIDs {
		if _, err := r.querier.Exec(ctx, `
			INSERT INTO tenant_domain_subscriptions (tenant_id, service_id, domain_id)
			VALUES ($1, $2, $3)
		`, item.TenantID, item.ID, domainID); err != nil {
			return fmt.Errorf("save tenant service subscription %q: %w", domainID, err)
		}
	}
	return nil
}

// GetService retrieves a domain service by ID.
func (r *DomainServiceRepository) GetService(ctx context.Context, tenantID, serviceID string) (*domain.DomainService, error) {
	item := &domain.DomainService{}
	err := r.querier.QueryRow(ctx, `
		SELECT service_id, tenant_id, name, endpoint_url, active, created_at, updated_at
		FROM tenant_domain_services WHERE tenant_id = $1 AND service_id = $2
	`, tenantID, serviceID).Scan(&item.ID, &item.TenantID, &item.Name, &item.EndpointURL, &item.Active, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get tenant service: %w", err)
	}
	item.DomainIDs, err = r.listServiceDomains(ctx, tenantID, serviceID)
	if err != nil {
		return nil, err
	}
	return item, nil
}

// ListServices returns all services for a tenant.
func (r *DomainServiceRepository) ListServices(ctx context.Context, tenantID string) ([]*domain.DomainService, error) {
	rows, err := r.querier.Query(ctx, `
		SELECT service_id, tenant_id, name, endpoint_url, active, created_at, updated_at
		FROM tenant_domain_services WHERE tenant_id = $1 ORDER BY name
	`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list tenant services: %w", err)
	}
	defer rows.Close()

	items := make([]*domain.DomainService, 0)
	for rows.Next() {
		item := &domain.DomainService{}
		if err := rows.Scan(&item.ID, &item.TenantID, &item.Name, &item.EndpointURL, &item.Active, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan tenant service: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant services: %w", err)
	}
	for _, item := range items {
		item.DomainIDs, err = r.listServiceDomains(ctx, tenantID, item.ID)
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

// DeleteService removes a domain service.
func (r *DomainServiceRepository) DeleteService(ctx context.Context, tenantID, serviceID string) error {
	_, err := r.querier.Exec(ctx, `
		UPDATE tenant_domain_services SET active = FALSE, updated_at = $3
		WHERE tenant_id = $1 AND service_id = $2
	`, tenantID, serviceID, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("deactivate tenant service: %w", err)
	}
	return nil
}

// ListSubscribersForEvent returns services subscribed to an event type.
func (r *DomainServiceRepository) ListSubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error) {
	rows, err := r.querier.Query(ctx, `
		SELECT DISTINCT s.service_id, s.tenant_id, s.name, s.endpoint_url, s.active, s.created_at, s.updated_at
		FROM tenant_domain_services s
		JOIN tenant_domain_subscriptions sub ON sub.tenant_id = s.tenant_id AND sub.service_id = s.service_id
		JOIN tenant_domains d ON d.tenant_id = sub.tenant_id AND d.domain_id = sub.domain_id
		WHERE s.tenant_id = $1 AND s.active = TRUE AND d.event_types @> ARRAY[$2]::text[]
		ORDER BY s.service_id
		LIMIT 101
	`, tenantID, eventType)
	if err != nil {
		return nil, fmt.Errorf("list services subscribed to event: %w", err)
	}
	defer rows.Close()

	items := make([]*domain.DomainService, 0)
	for rows.Next() {
		item := &domain.DomainService{}
		if err := rows.Scan(&item.ID, &item.TenantID, &item.Name, &item.EndpointURL, &item.Active, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan event subscriber: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate event subscribers: %w", err)
	}
	if len(items) > 100 {
		return nil, fmt.Errorf("event %q exceeds maximum of 100 tenant subscribers", eventType)
	}
	return items, nil
}

func (r *DomainServiceRepository) listServiceDomains(ctx context.Context, tenantID, serviceID string) ([]string, error) {
	rows, err := r.querier.Query(ctx, `
		SELECT domain_id FROM tenant_domain_subscriptions
		WHERE tenant_id = $1 AND service_id = $2 ORDER BY domain_id
	`, tenantID, serviceID)
	if err != nil {
		return nil, fmt.Errorf("list service domain subscriptions: %w", err)
	}
	defer rows.Close()

	ids := make([]string, 0)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan service domain subscription: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate service domain subscriptions: %w", err)
	}
	return ids, nil
}

var _ ports.DomainServiceRepository = (*DomainServiceRepository)(nil)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
