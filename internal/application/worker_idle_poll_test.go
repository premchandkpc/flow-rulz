package application

import (
	"context"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type idlePollShardService struct {
	owned []uint32
}

func (s idlePollShardService) Start(context.Context) error { return nil }
func (s idlePollShardService) Stop(context.Context) error  { return nil }
func (s idlePollShardService) OwnedShards() []uint32       { return s.owned }
func (s idlePollShardService) IsOwner(uint32) bool         { return false }
func (s idlePollShardService) GetFencingToken(uint32) (int64, bool) {
	return 0, false
}
func (s idlePollShardService) VirtualShardForEvent(*domain.EventEnvelope) uint32 {
	return 0
}

type idlePollConsumer struct {
	deliveries []ports.Delivery
	fetchCalls int
}

func (c *idlePollConsumer) EnsureShardConsumer(context.Context, uint32) error { return nil }
func (c *idlePollConsumer) RemoveShardConsumer(context.Context, uint32) error { return nil }
func (c *idlePollConsumer) OwnedShards() []uint32                             { return nil }
func (c *idlePollConsumer) FetchShards(context.Context, int, []uint32) ([]ports.Delivery, error) {
	c.fetchCalls++
	return c.deliveries, nil
}

func TestFetchAndProcessDeliveriesReportsIdleWithoutOwnedShards(t *testing.T) {
	consumer := &idlePollConsumer{}
	worker := &Worker{
		shardService:    idlePollShardService{},
		shardedConsumer: consumer,
	}

	didWork, err := worker.fetchAndProcessDeliveries(context.Background())
	if err != nil {
		t.Fatalf("fetchAndProcessDeliveries() error = %v", err)
	}
	if didWork {
		t.Fatal("fetchAndProcessDeliveries() didWork = true, want false")
	}
	if consumer.fetchCalls != 0 {
		t.Fatalf("FetchShards() calls = %d, want 0 when no shards are owned", consumer.fetchCalls)
	}
}

func TestFetchAndProcessDeliveriesReportsIdleWhenNoMessages(t *testing.T) {
	consumer := &idlePollConsumer{}
	worker := &Worker{
		shardService:    idlePollShardService{owned: []uint32{7}},
		shardedConsumer: consumer,
	}

	didWork, err := worker.fetchAndProcessDeliveries(context.Background())
	if err != nil {
		t.Fatalf("fetchAndProcessDeliveries() error = %v", err)
	}
	if didWork {
		t.Fatal("fetchAndProcessDeliveries() didWork = true, want false")
	}
	if consumer.fetchCalls != 1 {
		t.Fatalf("FetchShards() calls = %d, want 1", consumer.fetchCalls)
	}
}
