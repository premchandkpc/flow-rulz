package events

import (
	"container/list"
	"context"
	"sync"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// immutableRevisionCache stores immutable, versioned rule revisions in worker
// memory. Activation pointers still come from the transaction, so publishing a
// new revision takes effect immediately without invalidating old cache entries.
type immutableRevisionCache struct {
	mu       sync.Mutex
	capacity int
	items    map[revisionCacheKey]*list.Element
	order    *list.List
}

type revisionCacheKey struct {
	tenantScope string
	ruleSet     string
	revision    int64
}

type revisionCacheItem struct {
	key      revisionCacheKey
	revision *domain.RuleRevision
}

func newImmutableRevisionCache(capacity int) *immutableRevisionCache {
	if capacity <= 0 {
		capacity = 4096
	}
	return &immutableRevisionCache{
		capacity: capacity,
		items:    make(map[revisionCacheKey]*list.Element, capacity),
		order:    list.New(),
	}
}

func (c *immutableRevisionCache) get(key revisionCacheKey) (*domain.RuleRevision, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	element, ok := c.items[key]
	if !ok {
		return nil, false
	}
	c.order.MoveToFront(element)
	return element.Value.(revisionCacheItem).revision, true
}

func (c *immutableRevisionCache) put(revision *domain.RuleRevision) {
	if revision == nil {
		return
	}
	key := revisionCacheKey{tenantScope: revision.TenantScope, ruleSet: revision.RuleID, revision: revision.Revision}
	c.mu.Lock()
	defer c.mu.Unlock()
	if element, ok := c.items[key]; ok {
		c.order.MoveToFront(element)
		return
	}
	c.items[key] = c.order.PushFront(revisionCacheItem{key: key, revision: revision})
	c.evictOverflow()
}

// evictOverflow requires the caller to hold c.mu.
func (c *immutableRevisionCache) evictOverflow() {
	if c.order.Len() <= c.capacity {
		return
	}
	oldest := c.order.Back()
	item := oldest.Value.(revisionCacheItem)
	delete(c.items, item.key)
	c.order.Remove(oldest)
}

func (s *Service) getRevision(ctx context.Context, repository ports.RuleRepository, tenantScope string, ruleSet string, revision int64) (*domain.RuleRevision, error) {
	key := revisionCacheKey{tenantScope: tenantScope, ruleSet: ruleSet, revision: revision}
	if cached, ok := s.revisions.get(key); ok {
		return cached, nil
	}
	loaded, err := repository.Get(ctx, tenantScope, ruleSet, revision)
	if err != nil {
		return nil, err
	}
	return s.cacheLoadedRevision(key, loaded)
}

func (s *Service) cacheLoadedRevision(key revisionCacheKey, loaded *domain.RuleRevision) (*domain.RuleRevision, error) {
	if loaded == nil {
		return nil, nil
	}
	if loaded.TenantScope != key.tenantScope || loaded.RuleID != key.ruleSet || loaded.Revision != key.revision {
		return nil, domain.ErrRuleNotFound
	}
	s.revisions.put(loaded)
	return loaded, nil
}
