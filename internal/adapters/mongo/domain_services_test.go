package mongo

import (
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func TestDomainServiceIndexesAreTenantScoped(t *testing.T) {
	indexes := indexModels()
	tests := []struct {
		collection string
		keys       []string
		unique     bool
	}{
		{collection: collectionTenantDomains, keys: []string{"tenant_id", "name"}, unique: true},
		{collection: collectionDomainServices, keys: []string{"tenant_id", "name"}, unique: true},
		{collection: collectionDomainServices, keys: []string{"tenant_id", "active", "domain_ids"}},
		{collection: collectionTenantDomains, keys: []string{"tenant_id", "event_types"}},
	}
	for _, test := range tests {
		t.Run(test.collection+"/"+test.keys[1], func(t *testing.T) {
			if !hasIndex(indexes[test.collection], test.keys, test.unique) {
				t.Fatalf("missing index on %s with keys %v (unique=%t)", test.collection, test.keys, test.unique)
			}
		})
	}
}

func hasIndex(models []mongo.IndexModel, wanted []string, unique bool) bool {
	for _, model := range models {
		document, ok := model.Keys.(bson.D)
		if !ok || len(document) != len(wanted) {
			continue
		}
		match := true
		for i, element := range document {
			if element.Key != wanted[i] || element.Value != 1 {
				match = false
				break
			}
		}
		gotUnique := model.Options != nil && model.Options.Unique != nil && *model.Options.Unique
		if match && gotUnique == unique {
			return true
		}
	}
	return false
}

func TestTenantDomainBSONRoundTripPreservesServiceDeployments(t *testing.T) {
	want := domain.TenantDomain{
		ID: "domain-orders", TenantID: "tenant-a", Name: "orders",
		EventTypes: []string{"orders.created"},
		Deployments: []domain.ServiceDeployment{{
			ServiceID: "order-worker", Name: "order-worker", Revision: 3, Active: true,
			Rulesets: []domain.RulesetAssignment{{RulesetID: "order-created", Revision: 7}},
			WorkerProfile: domain.WorkerProfile{MinReplicas: 1, MaxReplicas: 4, Concurrency: 8},
		}},
	}
	doc := tenantDomainDoc{
		ID: want.ID, TenantID: want.TenantID, Name: want.Name, EventTypes: want.EventTypes,
		Deployments: want.Deployments, CreatedAt: want.CreatedAt, UpdatedAt: want.UpdatedAt,
	}
	encoded, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal tenant domain: %v", err)
	}
	var decoded tenantDomainDoc
	if err := bson.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal tenant domain: %v", err)
	}
	got := decoded.toDomain()
	if len(got.Deployments) != 1 || got.Deployments[0].ServiceID != "order-worker" ||
		got.Deployments[0].Revision != 3 || len(got.Deployments[0].Rulesets) != 1 ||
		got.Deployments[0].Rulesets[0].Revision != 7 || got.Deployments[0].WorkerProfile.Concurrency != 8 {
		t.Fatalf("deployment fields did not survive BSON round trip: %#v", got.Deployments)
	}
}
