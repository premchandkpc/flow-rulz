package events

import (
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestImmutableRevisionCacheIsTenantScoped(t *testing.T) {
	cache := newImmutableRevisionCache(4)
	a := &domain.RuleRevision{TenantScope: "tenant-a", RuleID: "orders", Revision: 2}
	b := &domain.RuleRevision{TenantScope: "tenant-b", RuleID: "orders", Revision: 2}
	cache.put(a)
	cache.put(b)

	if got, ok := cache.get(revisionCacheKey{tenantScope: "tenant-a", ruleSet: "orders", revision: 2}); !ok || got != a {
		t.Fatalf("tenant-a cache lookup = (%p, %v), want (%p, true)", got, ok, a)
	}
	if got, ok := cache.get(revisionCacheKey{tenantScope: "tenant-b", ruleSet: "orders", revision: 2}); !ok || got != b {
		t.Fatalf("tenant-b cache lookup = (%p, %v), want (%p, true)", got, ok, b)
	}
}

func TestImmutableRevisionCacheEvictsLeastRecentlyUsed(t *testing.T) {
	cache := newImmutableRevisionCache(2)
	first := &domain.RuleRevision{TenantScope: "tenant-a", RuleID: "orders", Revision: 1}
	second := &domain.RuleRevision{TenantScope: "tenant-a", RuleID: "orders", Revision: 2}
	third := &domain.RuleRevision{TenantScope: "tenant-a", RuleID: "orders", Revision: 3}
	cache.put(first)
	cache.put(second)
	cache.get(revisionCacheKey{tenantScope: "tenant-a", ruleSet: "orders", revision: 1})
	cache.put(third)

	if _, ok := cache.get(revisionCacheKey{tenantScope: "tenant-a", ruleSet: "orders", revision: 2}); ok {
		t.Fatal("least recently used revision 2 should have been evicted")
	}
	if _, ok := cache.get(revisionCacheKey{tenantScope: "tenant-a", ruleSet: "orders", revision: 1}); !ok {
		t.Fatal("recently used revision 1 should remain cached")
	}
}
