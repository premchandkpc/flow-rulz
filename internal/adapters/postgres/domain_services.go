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
	deployments, err := encodeDeployments(item.Deployments)
	if err != nil {
		return err
	}
	_, err = r.querier.Exec(ctx, domainServiceRepositorySaveDomainSQL, item.TenantID, item.ID, item.Name, item.EventTypes, deployments, item.CreatedAt, item.UpdatedAt)
	return domainWriteError(err, "domain name already exists", "save domain")
}

// GetDomain retrieves a tenant domain by ID.
func (r *DomainServiceRepository) GetDomain(ctx context.Context, tenantID, domainID string) (*domain.TenantDomain, error) {
	row := r.querier.QueryRow(ctx, domainServiceRepositoryGetDomainSQL, tenantID, domainID)
	item, err := scanTenantDomain(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get domain: %w", err)
	}
	return item, nil
}

// ListDomains returns all domains for a tenant.
func (r *DomainServiceRepository) ListDomains(ctx context.Context, tenantID string) ([]*domain.TenantDomain, error) {
	rows, err := r.querier.Query(ctx, domainServiceRepositoryListDomainsSQL, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list domains: %w", err)
	}
	defer rows.Close()
	return scanTenantDomains(rows)
}

// DeleteDomain removes a tenant domain.
func (r *DomainServiceRepository) DeleteDomain(ctx context.Context, tenantID, domainID string) error {
	_, err := r.querier.Exec(ctx, `DELETE FROM tenant_domains WHERE tenant_id = $1 AND domain_id = $2`, tenantID, domainID)
	if err != nil {
		return fmt.Errorf("delete domain: %w", err)
	}
	return nil
}

// SaveService inserts or updates a domain service.
func (r *DomainServiceRepository) SaveService(ctx context.Context, item *domain.DomainService) error {
	if pool, ok := r.querier.(*pgxpool.Pool); ok {
		return saveSubscriptionTransaction(ctx, pool, item)
	}
	return r.saveService(ctx, item)
}

func (r *DomainServiceRepository) saveService(ctx context.Context, item *domain.DomainService) error {
	_, err := r.querier.Exec(ctx, domainServiceRepositorysaveServiceSQL, item.TenantID, item.ID, item.Name, item.EndpointURL, item.EventTypes, item.Active, item.CreatedAt, item.UpdatedAt)
	if err := domainWriteError(err, "subscription name already exists", "save subscription"); err != nil {
		return err
	}
	return r.replaceServiceDomains(ctx, item)
}

// GetService retrieves a domain service by ID.
func (r *DomainServiceRepository) GetService(ctx context.Context, tenantID, serviceID string) (*domain.DomainService, error) {
	item := &domain.DomainService{}
	err := r.querier.QueryRow(ctx, domainServiceRepositoryGetServiceSQL, tenantID, serviceID).Scan(&item.ID, &item.TenantID, &item.Name, &item.EndpointURL, &item.EventTypes, &item.Active, &item.CreatedAt, &item.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get subscription: %w", err)
	}
	item.DomainIDs, err = r.listServiceDomains(ctx, tenantID, serviceID)
	if err != nil {
		return nil, err
	}
	return item, nil
}

// ListServices returns all services for a tenant.
func (r *DomainServiceRepository) ListServices(ctx context.Context, tenantID string) ([]*domain.DomainService, error) {
	rows, err := r.querier.Query(ctx, domainServiceRepositoryListServicesSQL, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()
	items, err := scanDomainServices(rows, "subscription", "subscriptions")
	if err != nil {
		return nil, err
	}
	return r.loadServiceDomains(ctx, tenantID, items)
}

// DeleteService removes a domain service.
func (r *DomainServiceRepository) DeleteService(ctx context.Context, tenantID, serviceID string) error {
	_, err := r.querier.Exec(ctx, domainServiceRepositoryDeleteServiceSQL, tenantID, serviceID, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("deactivate subscription: %w", err)
	}
	return nil
}

// ListSubscribersForEvent returns services subscribed to an event type.
func (r *DomainServiceRepository) ListSubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error) {
	rows, err := r.querier.Query(ctx, domainServiceRepositoryListSubscribersForEventSQL, tenantID, eventType, domain.DomainConfigurationScope)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions for event: %w", err)
	}
	defer rows.Close()
	items, err := scanDomainServices(rows, "event subscription", "event subscriptions")
	if err != nil {
		return nil, err
	}
	if len(items) > 100 {
		return nil, fmt.Errorf("event %q exceeds maximum of 100 subscriptions", eventType)
	}
	return items, nil
}

func (r *DomainServiceRepository) listServiceDomains(ctx context.Context, tenantID, serviceID string) ([]string, error) {
	rows, err := r.querier.Query(ctx, domainServiceRepositorylistServiceDomainsSQL, tenantID, serviceID)
	if err != nil {
		return nil, fmt.Errorf("list service domain subscriptions: %w", err)
	}
	defer rows.Close()
	return scanServiceDomainIDs(rows)
}

var _ ports.DomainServiceRepository = (*DomainServiceRepository)(nil)

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

const domainServiceRepositorySaveDomainSQL = `
		INSERT INTO tenant_domains (tenant_id, domain_id, name, event_types, deployments, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (tenant_id, domain_id) DO UPDATE SET
			name = EXCLUDED.name, event_types = EXCLUDED.event_types,
			deployments = EXCLUDED.deployments, updated_at = EXCLUDED.updated_at
	`

const domainServiceRepositoryGetDomainSQL = `
		SELECT domain_id, tenant_id, name, event_types, deployments, created_at, updated_at
		FROM tenant_domains WHERE tenant_id = $1 AND domain_id = $2
	`

const domainServiceRepositoryListDomainsSQL = `
		SELECT domain_id, tenant_id, name, event_types, deployments, created_at, updated_at
		FROM tenant_domains WHERE tenant_id = $1 ORDER BY name
	`

const domainServiceRepositorysaveServiceSQL = `
		INSERT INTO tenant_domain_services (tenant_id, service_id, name, endpoint_url, event_types, active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (tenant_id, service_id) DO UPDATE SET
			name = EXCLUDED.name, endpoint_url = EXCLUDED.endpoint_url, event_types = EXCLUDED.event_types,
			active = EXCLUDED.active, updated_at = EXCLUDED.updated_at
	`

const domainServiceRepositorysaveServiceSQL2 = `
			INSERT INTO tenant_domain_subscriptions (tenant_id, service_id, domain_id)
			VALUES ($1, $2, $3)
		`

const domainServiceRepositoryGetServiceSQL = `
		SELECT service_id, tenant_id, name, endpoint_url, event_types, active, created_at, updated_at
		FROM tenant_domain_services WHERE tenant_id = $1 AND service_id = $2
	`

const domainServiceRepositoryListServicesSQL = `
		SELECT service_id, tenant_id, name, endpoint_url, event_types, active, created_at, updated_at
		FROM tenant_domain_services WHERE tenant_id = $1 ORDER BY name
	`

const domainServiceRepositoryDeleteServiceSQL = `
		UPDATE tenant_domain_services SET active = FALSE, updated_at = $3
		WHERE tenant_id = $1 AND service_id = $2
	`

const domainServiceRepositoryListSubscribersForEventSQL = `
		SELECT DISTINCT s.service_id, s.tenant_id, s.name, s.endpoint_url, s.event_types, s.active, s.created_at, s.updated_at
		FROM tenant_domain_services s
		WHERE s.tenant_id IN ($1, $3) AND s.active = TRUE
		  AND (
			s.event_types @> ARRAY[$2]::text[]
			OR EXISTS (
				SELECT 1
				FROM tenant_domain_subscriptions sub
				JOIN tenant_domains d ON d.tenant_id = sub.tenant_id AND d.domain_id = sub.domain_id
				WHERE sub.tenant_id = s.tenant_id AND sub.service_id = s.service_id
				  AND d.event_types @> ARRAY[$2]::text[]
			)
		  )
		ORDER BY s.service_id
		LIMIT 101
	`

const domainServiceRepositorylistServiceDomainsSQL = `
		SELECT domain_id FROM tenant_domain_subscriptions
		WHERE tenant_id = $1 AND service_id = $2 ORDER BY domain_id
	`

func encodeDeployments(items []domain.ServiceDeployment) ([]byte, error) {
	if items == nil {
		items = []domain.ServiceDeployment{}
	}
	data, err := json.Marshal(items)
	if err != nil {
		return nil, fmt.Errorf("encode domain service deployments: %w", err)
	}
	return data, nil
}

func domainWriteError(err error, conflict, operation string) error {
	if err == nil {
		return nil
	}
	if isUniqueViolation(err) {
		return fmt.Errorf("%w: %s", domain.ErrDomainServiceConflict, conflict)
	}
	return fmt.Errorf("%s: %w", operation, err)
}

func scanTenantDomain(row pgx.Row) (*domain.TenantDomain, error) {
	item := &domain.TenantDomain{}
	var deployments []byte
	if err := row.Scan(&item.ID, &item.TenantID, &item.Name, &item.EventTypes, &deployments, &item.CreatedAt, &item.UpdatedAt); err != nil {
		return nil, err
	}
	if len(deployments) > 0 {
		if err := json.Unmarshal(deployments, &item.Deployments); err != nil {
			return nil, fmt.Errorf("decode domain service deployments: %w", err)
		}
	}
	return item, nil
}

func scanTenantDomains(rows pgx.Rows) ([]*domain.TenantDomain, error) {
	items := make([]*domain.TenantDomain, 0)
	for rows.Next() {
		item, err := scanTenantDomain(rows)
		if err != nil {
			return nil, fmt.Errorf("scan domain: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate domains: %w", err)
	}
	return items, nil
}

func saveSubscriptionTransaction(ctx context.Context, pool *pgxpool.Pool, item *domain.DomainService) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin save subscription: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := (&DomainServiceRepository{querier: tx}).saveService(ctx, item); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit subscription: %w", err)
	}
	return nil
}

func (r *DomainServiceRepository) replaceServiceDomains(ctx context.Context, item *domain.DomainService) error {
	if _, err := r.querier.Exec(ctx, `DELETE FROM tenant_domain_subscriptions WHERE tenant_id = $1 AND service_id = $2`, item.TenantID, item.ID); err != nil {
		return fmt.Errorf("replace legacy subscription domain links: %w", err)
	}
	for _, domainID := range item.DomainIDs {
		if _, err := r.querier.Exec(ctx, domainServiceRepositorysaveServiceSQL2, item.TenantID, item.ID, domainID); err != nil {
			return fmt.Errorf("save legacy subscription domain link %q: %w", domainID, err)
		}
	}
	return nil
}

func scanDomainService(row pgx.Row) (*domain.DomainService, error) {
	item := &domain.DomainService{}
	err := row.Scan(&item.ID, &item.TenantID, &item.Name, &item.EndpointURL, &item.EventTypes, &item.Active, &item.CreatedAt, &item.UpdatedAt)
	return item, err
}

func scanDomainServices(rows pgx.Rows, scanLabel, iterationLabel string) ([]*domain.DomainService, error) {
	items := make([]*domain.DomainService, 0)
	for rows.Next() {
		item, err := scanDomainService(rows)
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", scanLabel, err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s: %w", iterationLabel, err)
	}
	return items, nil
}

func (r *DomainServiceRepository) loadServiceDomains(ctx context.Context, tenantID string, items []*domain.DomainService) ([]*domain.DomainService, error) {
	for _, item := range items {
		var err error
		item.DomainIDs, err = r.listServiceDomains(ctx, tenantID, item.ID)
		if err != nil {
			return nil, err
		}
	}
	return items, nil
}

func scanServiceDomainIDs(rows pgx.Rows) ([]string, error) {
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
