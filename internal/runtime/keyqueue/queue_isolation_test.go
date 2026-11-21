package keyqueue

import (
	"context"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

func TestQueueIdentityIsTenantScopedAndUnambiguous(t *testing.T) {
	a := &domain.EventEnvelope{TenantID: "tenant-a", PartitionKey: "account-1"}
	b := &domain.EventEnvelope{TenantID: "tenant-b", PartitionKey: "account-1"}
	if queueIdentity(a) == queueIdentity(b) {
		t.Fatal("different tenants share a queue identity")
	}
	c := &domain.EventEnvelope{TenantID: "ab", PartitionKey: "c"}
	d := &domain.EventEnvelope{TenantID: "a", PartitionKey: "bc"}
	if queueIdentity(c) == queueIdentity(d) {
		t.Fatal("ambiguous tenant and partition concatenation")
	}
}

func TestHotKeyThresholdNeverExceedsQueueCapacity(t *testing.T) {
	processor := func(_ context.Context, _ *domain.EventEnvelope, _ ports.Delivery, _ int64, _ uint32, _ string) error {
		return nil
	}
	queue := NewKeyQueue(10, 4, 2, processor, time.Now, 1000, time.Second, nil)
	if queue.hotKeyThreshold != 4 {
		t.Fatalf("threshold = %d, want 4", queue.hotKeyThreshold)
	}
}
