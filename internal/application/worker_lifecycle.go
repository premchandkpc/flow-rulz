package application

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/runtime/keyqueue"
)

const workerShutdownTimeout = 5 * time.Second

// Run starts the shard lease service, queue, and background loops, then polls
// the broker until cancellation. Startup and shutdown ownership stays here.
func (w *Worker) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("worker context is required")
	}
	if err := w.shardService.Start(ctx); err != nil {
		return fmt.Errorf("start shard service: %w", err)
	}
	defer w.stopShardService(ctx)
	return w.runWithKeyQueue(ctx)
}

func (w *Worker) stopShardService(ctx context.Context) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerShutdownTimeout)
	defer cancel()
	if err := w.shardService.Stop(shutdownCtx); err != nil {
		log.Printf("stop shard service: %v", err)
	}
}

func (w *Worker) startKeyQueue(ctx context.Context) error {
	w.keyQueue = keyqueue.NewKeyQueue(
		w.config.MaxGlobalInFlight,
		w.config.MaxPerKeyQueue,
		w.config.KeyQueueWorkers,
		w.processDelivery,
		w.clock.Now,
		w.config.HotKeyThreshold,
		w.config.HotKeyCheckInterval,
		w.config.HotKeyCallback,
	)
	return w.startConfiguredQueue(ctx)
}

func (w *Worker) stopKeyQueue(ctx context.Context) {
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerShutdownTimeout)
	defer cancel()
	if err := w.keyQueue.Stop(shutdownCtx); err != nil {
		log.Printf("stop key queue: %v", err)
	}
}

func (w *Worker) startBackgroundLoops(ctx context.Context, background *sync.WaitGroup) {
	background.Add(1)
	go func() {
		defer background.Done()
		w.publishLoop(ctx)
	}()
	if w.batches != nil && w.batchInterval > 0 {
		background.Add(1)
		go func() {
			defer background.Done()
			w.batchLoop(ctx)
		}()
	}
}

func (w *Worker) publishLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.EffectPublishInterval)
	defer ticker.Stop()
	// Drain available effects at startup, then poll only when idle.
	for {
		if err := w.publishAvailable(ctx); err != nil && ctx.Err() == nil {
			log.Printf("publish effects: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// publishAvailable drains bounded bursts while work is available. The cap keeps
// retry and shutdown checks responsive even under a sustained backlog.
func (w *Worker) publishAvailable(ctx context.Context) error {
	const maxConsecutiveBatches = 16
	counter, ok := w.effects.(interface {
		PublishBatchCount(context.Context, int) (int, error)
	})
	if !ok {
		return w.effects.PublishBatch(ctx, w.config.EffectPublishBatchSize)
	}
	for n := 0; n < maxConsecutiveBatches && ctx.Err() == nil; n++ {
		count, err := counter.PublishBatchCount(ctx, w.config.EffectPublishBatchSize)
		if err != nil || count < w.config.EffectPublishBatchSize {
			return err
		}
	}
	return ctx.Err()
}

func (w *Worker) batchLoop(ctx context.Context) {
	interval := w.batchInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tickBatches(ctx)
		}
	}
}

func (w *Worker) runWithKeyQueue(ctx context.Context) error {
	if err := w.startKeyQueue(ctx); err != nil {
		return err
	}
	defer w.stopKeyQueue(ctx)
	var background sync.WaitGroup
	w.startBackgroundLoops(ctx, &background)
	defer background.Wait()
	w.runSharded(ctx)
	return nil
}

func (w *Worker) startConfiguredQueue(ctx context.Context) error {
	if err := w.keyQueue.Start(ctx); err != nil {
		return fmt.Errorf("start key queue: %w", err)
	}
	return nil
}

func (w *Worker) tickBatches(ctx context.Context) {
	if n, err := w.batches.Tick(ctx); err != nil {
		log.Printf("batch tick: %v", err)
	} else if n > 0 {
		log.Printf("batch tick formed %d batch runs", n)
	}
}
