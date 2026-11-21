package keyqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type shutdownDelivery struct {
	mockDelivery
	retried *atomic.Int32
}

func (d *shutdownDelivery) Retry(context.Context, time.Duration) error {
	d.retried.Add(1)
	return nil
}

func TestStopRetriesUnprocessedQueueDeliveries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	processor := func(ctx context.Context, _ *domain.EventEnvelope, _ ports.Delivery, _ int64, _ uint32, _ string) error {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
		return ctx.Err()
	}
	q := NewKeyQueue(4, 4, 1, processor, time.Now, 2, time.Second, nil)
	if err := q.Start(ctx); err != nil {
		t.Fatal(err)
	}
	retries := &atomic.Int32{}
	submit := func(id string) {
		t.Helper()
		err := q.Submit(ctx, ports.QueueItem{
			Event:    &domain.EventEnvelope{ID: id, PartitionKey: "same"},
			Delivery: &shutdownDelivery{retried: retries},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	submit("active")
	select {
	case <-entered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("processor did not start")
	}
	submit("queued-1")
	submit("queued-2")
	cancel()
	if err := q.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := retries.Load(); got != 2 {
		t.Fatalf("retried %d queued deliveries, want 2", got)
	}
}
