package effects

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/google/uuid"
)

// FakeDestination is an in-memory effect sender for testing.
// It tracks sent effects and can simulate failures.
type FakeDestination struct {
	mu        sync.Mutex
	sent      map[string]bool
	failOn    map[string]bool
	failCount int
}

// NewFakeDestination creates a new FakeDestination.
func NewFakeDestination() *FakeDestination {
	return &FakeDestination{
		sent:   make(map[string]bool),
		failOn: make(map[string]bool),
	}
}

// Send records an effect as sent and can simulate failures.
func (f *FakeDestination) Send(_ context.Context, effect *domain.Effect, idempotencyKey string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failOn[effect.ID] {
		f.failCount++
		if f.failCount > 3 {
			delete(f.failOn, effect.ID)
			f.failCount = 0
			return nil
		}
		return fmt.Errorf("fake destination error for effect %s", effect.ID)
	}

	f.sent[effect.ID] = true
	log.Printf("FakeDestination: sent effect %s to %s (idempotency: %s)", effect.ID, effect.Destination, idempotencyKey)
	return nil
}

// WasSent returns true if the effect ID was sent.
func (f *FakeDestination) WasSent(effectID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent[effectID]
}

// SentCount returns the number of effects sent.
func (f *FakeDestination) SentCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// Reset clears the sent effects and failure configuration.
func (f *FakeDestination) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = make(map[string]bool)
	f.failOn = make(map[string]bool)
	f.failCount = 0
}

// FailOn configures the destination to fail when sending the given effect ID.
func (f *FakeDestination) FailOn(effectID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failOn[effectID] = true
}

// FakeIDProvider provides deterministic IDs for testing.
type FakeIDProvider struct{}

// New generates a new UUID.
func (FakeIDProvider) New() string {
	return uuid.New().String()
}

// FakeClock is a controllable clock for testing.
type FakeClock struct {
	now time.Time
}

// NewFakeClock creates a new FakeClock starting at the given time.
func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now}
}

// Now returns the current time.
func (f *FakeClock) Now() time.Time {
	return f.now
}

// Advance moves the clock forward by the given duration.
func (f *FakeClock) Advance(d time.Duration) {
	f.now = f.now.Add(d)
}
