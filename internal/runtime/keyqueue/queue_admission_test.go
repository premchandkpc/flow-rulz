package keyqueue

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

func TestTrySubmitHotKeyDoesNotBlockOtherKeys(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var processed sync.WaitGroup
	processed.Add(4)
	processor := func(ctx context.Context, _ *domain.EventEnvelope, _ ports.Delivery, _ int64, _ uint32, _ string) error {
		defer processed.Done()
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	}
	queue := NewKeyQueue(4, 4, 1, processor, time.Now, 2, time.Second, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := queue.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = queue.Stop(ctx) }()
	submit := func(key, id string) error {
		return queue.TrySubmit(ctx, ports.QueueItem{
			Event:    &domain.EventEnvelope{TenantID: "tenant-a", PartitionKey: key, ID: id},
			Delivery: &mockDelivery{},
		})
	}
	if err := submit("hot", "1"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("first event did not start")
	}
	for _, id := range []string{"2", "3"} {
		if err := submit("hot", id); err != nil {
			close(release)
			t.Fatal(err)
		}
	}
	if err := submit("hot", "4"); !errors.Is(err, ErrQueueFull) {
		close(release)
		t.Fatalf("hot key capacity error = %v", err)
	}
	if err := submit("cold", "5"); err != nil {
		close(release)
		t.Fatalf("cold key admission blocked: %v", err)
	}
	close(release)
	done := make(chan struct{})
	go func() { processed.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("accepted events did not finish")
	}
}
