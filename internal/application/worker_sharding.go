package application

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
)

// syncConsumers reconciles broker consumers with the shards this worker owns.
func (w *Worker) syncConsumers(ctx context.Context) {
	if w.shardService == nil || w.shardedConsumer == nil {
		return
	}

	owned := w.shardService.OwnedShards()
	ownedSet := shardSet(owned)
	active := w.shardedConsumer.OwnedShards()
	activeSet := shardSet(active)

	w.removeUnownedShardConsumers(ctx, active, ownedSet, activeSet)
	w.ensureOwnedShardConsumers(ctx, owned, activeSet)
}

func shardSet(shards []uint32) map[uint32]struct{} {
	set := make(map[uint32]struct{}, len(shards))
	for _, shardID := range shards {
		set[shardID] = struct{}{}
	}
	return set
}

func (w *Worker) removeUnownedShardConsumers(
	ctx context.Context,
	active []uint32,
	ownedSet map[uint32]struct{},
	activeSet map[uint32]struct{},
) {
	for _, shardID := range active {
		if _, stillOwned := ownedSet[shardID]; stillOwned {
			continue
		}
		if err := w.shardedConsumer.RemoveShardConsumer(ctx, shardID); err != nil {
			log.Printf("remove consumer for unowned shard %d: %v", shardID, err)
			continue
		}
		delete(activeSet, shardID)
	}
}

func (w *Worker) ensureOwnedShardConsumers(
	ctx context.Context,
	owned []uint32,
	activeSet map[uint32]struct{},
) {
	// Only consumers confirmed active are skipped; failed ensures are retried
	// during the next reconciliation pass.
	for _, shardID := range owned {
		if _, exists := activeSet[shardID]; exists {
			continue
		}
		if err := w.shardedConsumer.EnsureShardConsumer(ctx, shardID); err != nil {
			log.Printf("ensure consumer for shard %d: %v", shardID, err)
		}
	}
}

// runSharded runs the worker in sharded mode with lease management.
func (w *Worker) runSharded(ctx context.Context) {
	// Synchronize initial ownership through the same retryable path used later.
	w.syncConsumers(ctx)
	for ctx.Err() == nil {
		if !w.runShardedIteration(ctx) {
			return
		}
	}
	log.Println("worker stopping")
}

func (w *Worker) runShardedIteration(ctx context.Context) bool {
	w.syncConsumers(ctx)

	didWork, err := w.fetchAndProcessDeliveries(ctx)
	if err != nil {
		if !errors.Is(err, ctx.Err()) {
			log.Printf("fetch and process error: %v", err)
		}
		return waitForRetry(ctx, time.Second)
	}
	if !didWork {
		return waitForRetry(ctx, 100*time.Millisecond)
	}
	return true
}

// fetchAndProcessDeliveries fetches and processes deliveries for owned shards.
func (w *Worker) fetchAndProcessDeliveries(ctx context.Context) (bool, error) {
	ownedShards := w.shardService.OwnedShards()
	if len(ownedShards) == 0 {
		return false, nil
	}

	deliveries, err := w.shardedConsumer.FetchShards(ctx, w.config.FetchBatchSize, ownedShards)
	if err != nil {
		return false, err
	}
	return w.processFetchedDeliveries(ctx, deliveries), nil
}

func (w *Worker) processFetchedDeliveries(ctx context.Context, deliveries []ports.Delivery) bool {
	if len(deliveries) == 0 {
		return false
	}
	for _, delivery := range deliveries {
		if err := w.processSingleDelivery(ctx, delivery); err != nil {
			log.Printf("process delivery error: %v", err)
		}
	}
	return true
}

func waitForRetry(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
