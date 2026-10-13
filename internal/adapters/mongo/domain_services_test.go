package mongo

import (
	"testing"

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
