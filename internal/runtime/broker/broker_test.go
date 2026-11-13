package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func newTestBroker(t *testing.T, limit int) *Broker {
	t.Helper()
	b, err := New(Config{Partitions: 4, MaxMessages: limit, MaxBytes: 1 << 20, AckWait: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func publishTest(t *testing.T, b *Broker, tenant, key string) Record {
	t.Helper()
	r, err := b.Publish("events", tenant, key, []byte(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func nextTest(t *testing.T, b *Broker, tenant, group string) *Claim {
	t.Helper()
	c, err := b.Next(context.Background(), tenant, "events", group, 0)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func ackTest(t *testing.T, b *Broker, tenant string, c *Claim) {
	t.Helper()
	if c == nil {
		t.Fatal("missing delivery")
	}
	if err := b.Settle(tenant, c.Record.Topic, c.Group, c.Record.Partition, c.Receipt, false, 0); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionOrderGroupReplayAndTenantIsolation(t *testing.T) {
	b := newTestBroker(t, 100)
	first := publishTest(t, b, "a", "key")
	second := publishTest(t, b, "a", "key")
	publishTest(t, b, "b", "key")
	if first.Partition != second.Partition || second.Offset != first.Offset+1 {
		t.Fatal("routing or offsets changed")
	}
	c := nextTest(t, b, "a", "service-b")
	if c == nil || c.Record.Offset != first.Offset || c.Record.TenantID != "a" {
		t.Fatalf("unexpected first delivery: %+v", c)
	}
	if got := nextTest(t, b, "a", "service-b"); got != nil {
		t.Fatal("same partition was delivered concurrently")
	}
	if err := b.Settle("b", c.Record.Topic, c.Group, c.Record.Partition, c.Receipt, false, 0); !errors.Is(err, ErrReceipt) {
		t.Fatal("cross-tenant ACK accepted")
	}
	ackTest(t, b, "a", c)
	c = nextTest(t, b, "a", "service-b")
	if c == nil || c.Record.Offset != second.Offset {
		t.Fatal("partition order violated")
	}
	ackTest(t, b, "a", c)
	if got := nextTest(t, b, "a", "service-b"); got != nil {
		t.Fatal("acknowledged event redelivered")
	}
	if got := nextTest(t, b, "a", "replay"); got == nil || got.Record.Offset != first.Offset {
		t.Fatal("new group did not replay retained log")
	}
	if got := nextTest(t, b, "b", "service-b"); got == nil || got.Record.TenantID != "b" {
		t.Fatal("tenant delivery failed")
	}
}

func TestRetryAndExpiredClaimFenceOldConsumer(t *testing.T) {
	b := newTestBroker(t, 10)
	publishTest(t, b, "a", "key")
	c := nextTest(t, b, "a", "g")
	if err := b.Settle("a", "events", "g", c.Record.Partition, c.Receipt, true, 20*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if got := nextTest(t, b, "a", "g"); got != nil {
		t.Fatal("retry delay was ignored")
	}
	replacement, err := b.Next(context.Background(), "a", "events", "g", time.Second)
	if err != nil || replacement == nil {
		t.Fatalf("retry was not redelivered: %v", err)
	}
	if err := b.Settle("a", "events", "g", c.Record.Partition, c.Receipt, false, 0); !errors.Is(err, ErrReceipt) {
		t.Fatal("old claim settled replacement")
	}
	ackTest(t, b, "a", replacement)
	publishTest(t, b, "a", "key")
	c = nextTest(t, b, "a", "g")
	b.mu.Lock()
	g := b.state.Groups[groupKey("a", "events", "g")]
	lease := g.Leases[c.Record.Partition]
	lease.ExpiresAt = time.Now().Add(-time.Second)
	g.Leases[c.Record.Partition] = lease
	b.mu.Unlock()
	replacement = nextTest(t, b, "a", "g")
	if replacement == nil || replacement.Receipt == c.Receipt {
		t.Fatal("expired claim not replaced")
	}
	if err := b.Settle("a", "events", "g", c.Record.Partition, c.Receipt, false, 0); !errors.Is(err, ErrReceipt) {
		t.Fatal("expired receipt accepted")
	}
}

func TestConcurrentConsumersShareEachPartition(t *testing.T) {
	b := newTestBroker(t, 100)
	publishTest(t, b, "a", "key")
	var wg sync.WaitGroup
	var mu sync.Mutex
	claims := 0
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := b.Next(context.Background(), "a", "events", "g", 0)
			if err != nil {
				t.Error(err)
			}
			if c != nil {
				mu.Lock()
				claims++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if claims != 1 {
		t.Fatalf("concurrent claims=%d want 1", claims)
	}
}

func TestRetentionBackpressureAndSafeReclamation(t *testing.T) {
	b := newTestBroker(t, 2)
	publishTest(t, b, "a", "key")
	publishTest(t, b, "a", "key")
	if _, err := b.Publish("events", "a", "key", []byte(`{}`)); !errors.Is(err, ErrFull) {
		t.Fatal("unconsumed data was evicted")
	}
	// A slow registered group must hold retention even after another group ACKs.
	slow := nextTest(t, b, "a", "slow")
	ackTest(t, b, "a", nextTest(t, b, "a", "fast"))
	if _, err := b.Publish("events", "a", "key", []byte(`{}`)); !errors.Is(err, ErrFull) {
		t.Fatal("slow consumer data was evicted")
	}
	ackTest(t, b, "a", slow)
	r, err := b.Publish("events", "a", "key", []byte(`{}`))
	if err != nil || r.Offset != 2 {
		t.Fatalf("reclaim failed or offset reused: %+v %v", r, err)
	}
	if replay := nextTest(t, b, "a", "new-group"); replay == nil || replay.Record.Offset != 1 {
		t.Fatal("replay did not start at retained prefix")
	}
}

func TestLongPollWakesOnPublishAndCancellation(t *testing.T) {
	b := newTestBroker(t, 10)
	done := make(chan *Claim, 1)
	go func() {
		c, err := b.Next(context.Background(), "a", "events", "g", time.Second)
		if err != nil {
			t.Error(err)
		}
		done <- c
	}()
	publishTest(t, b, "a", "key")
	select {
	case c := <-done:
		if c == nil {
			t.Fatal("long poll missed event")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("long poll did not wake")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Next(ctx, "a", "events", "g", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

type failingStore struct{}

func (failingStore) Load() ([]byte, error) { return nil, nil }
func (failingStore) Save([]byte) error     { return errors.New("disk unavailable") }

func TestPersistenceFailureStopsAcceptance(t *testing.T) {
	b, err := New(Config{Partitions: 1, MaxMessages: 10, MaxBytes: 1024, AckWait: time.Second}, failingStore{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("events", "a", "k", []byte(`{}`)); err == nil {
		t.Fatal("failed persistence reported success")
	}
	if _, err := b.Next(context.Background(), "a", "events", "g", 0); err == nil {
		t.Fatal("uncertain state was delivered")
	}
}
