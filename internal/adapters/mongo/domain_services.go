package mongo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// DomainServiceRepository persists tenant domains and domain services.
type DomainServiceRepository struct {
	querier Querier
}

// NewDomainServiceRepository creates and returns the configured value.
func NewDomainServiceRepository(q Querier) *DomainServiceRepository {
	return &DomainServiceRepository{querier: q}
}

// SaveDomain provides the corresponding repository operation.
func (r *DomainServiceRepository) SaveDomain(ctx context.Context, item *domain.TenantDomain) error {
	doc := tenantDomainDoc{
		ID: item.ID, TenantID: item.TenantID, Name: item.Name, EventTypes: item.EventTypes, Deployments: item.Deployments,
		CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
	}
	_, err := r.querier.Collection(collectionTenantDomains).UpdateOne(ctx,
		bson.M{"tenant_id": item.TenantID, "domain_id": item.ID},
		bson.M{"$set": doc}, options.Update().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("%w: tenant domain name already exists", domain.ErrDomainServiceConflict)
		}
		return fmt.Errorf("save tenant domain: %w", err)
	}
	return nil
}

// GetDomain retrieves the requested value.
func (r *DomainServiceRepository) GetDomain(ctx context.Context, tenantID, domainID string) (*domain.TenantDomain, error) {
	var doc tenantDomainDoc
	err := r.querier.Collection(collectionTenantDomains).FindOne(ctx, bson.M{"tenant_id": tenantID, "domain_id": domainID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get tenant domain: %w", err)
	}
	return doc.toDomain(), nil
}

// ListDomains returns matching records.
func (r *DomainServiceRepository) ListDomains(ctx context.Context, tenantID string) ([]*domain.TenantDomain, error) {
	cursor, err := r.querier.Collection(collectionTenantDomains).Find(ctx,
		bson.M{"tenant_id": tenantID}, options.Find().SetSort(bson.D{{Key: "name", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list tenant domains: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	items := make([]*domain.TenantDomain, 0)
	for cursor.Next(ctx) {
		var doc tenantDomainDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode tenant domain: %w", err)
		}
		items = append(items, doc.toDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant domains: %w", err)
	}
	return items, nil
}

// DeleteDomain provides the corresponding repository operation.
func (r *DomainServiceRepository) DeleteDomain(ctx context.Context, tenantID, domainID string) error {
	_, err := r.querier.Collection(collectionTenantDomains).DeleteOne(ctx, bson.M{"tenant_id": tenantID, "domain_id": domainID})
	if err != nil {
		return fmt.Errorf("delete tenant domain: %w", err)
	}
	_, err = r.querier.Collection(collectionDomainServices).UpdateMany(ctx,
		bson.M{"tenant_id": tenantID, "domain_ids": domainID},
		bson.M{"$pull": bson.M{"domain_ids": domainID}})
	if err != nil {
		return fmt.Errorf("remove deleted domain from service subscriptions: %w", err)
	}
	return nil
}

// SaveService provides the corresponding repository operation.
func (r *DomainServiceRepository) SaveService(ctx context.Context, item *domain.DomainService) error {
	doc := domainServiceDoc{
		ID: item.ID, TenantID: item.TenantID, Name: item.Name, EndpointURL: item.EndpointURL,
		EventTypes: item.EventTypes, DomainIDs: item.DomainIDs, Active: item.Active, CreatedAt: item.CreatedAt, UpdatedAt: item.UpdatedAt,
	}
	_, err := r.querier.Collection(collectionDomainServices).UpdateOne(ctx,
		bson.M{"tenant_id": item.TenantID, "service_id": item.ID},
		bson.M{"$set": doc}, options.Update().SetUpsert(true))
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("%w: tenant service name already exists", domain.ErrDomainServiceConflict)
		}
		return fmt.Errorf("save tenant service: %w", err)
	}
	return nil
}

// GetService retrieves the requested value.
func (r *DomainServiceRepository) GetService(ctx context.Context, tenantID, serviceID string) (*domain.DomainService, error) {
	var doc domainServiceDoc
	err := r.querier.Collection(collectionDomainServices).FindOne(ctx,
		bson.M{"tenant_id": tenantID, "service_id": serviceID}).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, fmt.Errorf("get tenant service: %w", err)
	}
	return doc.toDomain(), nil
}

// ListServices returns matching records.
func (r *DomainServiceRepository) ListServices(ctx context.Context, tenantID string) ([]*domain.DomainService, error) {
	cursor, err := r.querier.Collection(collectionDomainServices).Find(ctx,
		bson.M{"tenant_id": tenantID}, options.Find().SetSort(bson.D{{Key: "name", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list tenant services: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	items := make([]*domain.DomainService, 0)
	for cursor.Next(ctx) {
		var doc domainServiceDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode tenant service: %w", err)
		}
		items = append(items, doc.toDomain())
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenant services: %w", err)
	}
	return items, nil
}

// DeleteService provides the corresponding repository operation.
func (r *DomainServiceRepository) DeleteService(ctx context.Context, tenantID, serviceID string) error {
	_, err := r.querier.Collection(collectionDomainServices).UpdateOne(ctx,
		bson.M{"tenant_id": tenantID, "service_id": serviceID},
		bson.M{"$set": bson.M{"active": false}, "$currentDate": bson.M{"updated_at": true}})
	if err != nil {
		return fmt.Errorf("deactivate tenant service: %w", err)
	}
	return nil
}

// ListSubscribersForEvent returns matching records.
func (r *DomainServiceRepository) ListSubscribersForEvent(ctx context.Context, tenantID, eventType string) ([]*domain.DomainService, error) {
	// New subscriptions are global configuration with explicit event-type filters.
	// Also read tenant-scoped legacy subscriptions during migration.
	cursor, err := r.querier.Collection(collectionDomainServices).Find(ctx,
		bson.M{
			"tenant_id": bson.M{"$in": []string{tenantID, domain.DomainConfigurationScope}},
			"active": true,
			"event_types": eventType,
		},
		options.Find().SetLimit(101).SetSort(bson.D{{Key: "service_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("list subscriptions for event: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()
	items := make([]*domain.DomainService, 0)
	seen := make(map[string]struct{})
	for cursor.Next(ctx) {
		var doc domainServiceDoc
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode event subscription: %w", err)
		}
		item := doc.toDomain()
		key := item.TenantID + "/" + item.ID
		seen[key] = struct{}{}
		items = append(items, item)
	}
	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate event subscriptions: %w", err)
	}

	// Legacy subscriptions used DomainIDs plus the event_types configured on each domain.
	domainCursor, err := r.querier.Collection(collectionTenantDomains).Find(ctx,
		bson.M{"tenant_id": tenantID, "event_types": eventType},
		options.Find().SetProjection(bson.M{"domain_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("find legacy domains for event: %w", err)
	}
	defer func() { _ = domainCursor.Close(ctx) }()
	domainIDs := make([]string, 0)
	for domainCursor.Next(ctx) {
		var doc struct { ID string `bson:"domain_id"` }
		if err := domainCursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode legacy domain: %w", err)
		}
		domainIDs = append(domainIDs, doc.ID)
	}
	if err := domainCursor.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy domains: %w", err)
	}
	if len(domainIDs) > 0 {
		legacyCursor, err := r.querier.Collection(collectionDomainServices).Find(ctx,
			bson.M{"tenant_id": tenantID, "active": true, "domain_ids": bson.M{"$in": domainIDs}},
			options.Find().SetLimit(101).SetSort(bson.D{{Key: "service_id", Value: 1}}))
		if err != nil {
			return nil, fmt.Errorf("list legacy event subscribers: %w", err)
		}
		defer func() { _ = legacyCursor.Close(ctx) }()
		for legacyCursor.Next(ctx) {
			var doc domainServiceDoc
			if err := legacyCursor.Decode(&doc); err != nil {
				return nil, fmt.Errorf("decode legacy subscriber: %w", err)
			}
			item := doc.toDomain()
			key := item.TenantID + "/" + item.ID
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				items = append(items, item)
			}
		}
		if err := legacyCursor.Err(); err != nil {
			return nil, fmt.Errorf("iterate legacy subscribers: %w", err)
		}
	}
	if len(items) > 100 {
		return nil, fmt.Errorf("event %q exceeds maximum of 100 subscriptions", eventType)
	}
	return items, nil
}

type tenantDomainDoc struct {
	ID          string                     `bson:"domain_id"`
	TenantID    string                     `bson:"tenant_id"`
	Name        string                     `bson:"name"`
	EventTypes  []string                   `bson:"event_types"`
	Deployments []domain.ServiceDeployment `bson:"deployments,omitempty"`
	CreatedAt   time.Time                  `bson:"created_at"`
	UpdatedAt   time.Time                  `bson:"updated_at"`
}

func (d tenantDomainDoc) toDomain() *domain.TenantDomain {
	return &domain.TenantDomain{ID: d.ID, TenantID: d.TenantID, Name: d.Name, EventTypes: d.EventTypes, Deployments: d.Deployments, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt}
}

type domainServiceDoc struct {
	ID          string    `bson:"service_id"`
	TenantID    string    `bson:"tenant_id"`
	Name        string    `bson:"name"`
	EndpointURL string    `bson:"endpoint_url"`
	EventTypes  []string  `bson:"event_types"`
	DomainIDs   []string  `bson:"domain_ids"`
	Active      bool      `bson:"active"`
	CreatedAt   time.Time `bson:"created_at"`
	UpdatedAt   time.Time `bson:"updated_at"`
}

func (d domainServiceDoc) toDomain() *domain.DomainService {
	return &domain.DomainService{
		ID: d.ID, TenantID: d.TenantID, Name: d.Name, EndpointURL: d.EndpointURL,
		EventTypes: d.EventTypes, DomainIDs: d.DomainIDs, Active: d.Active, CreatedAt: d.CreatedAt, UpdatedAt: d.UpdatedAt,
	}
}

var _ ports.DomainServiceRepository = (*DomainServiceRepository)(nil)
