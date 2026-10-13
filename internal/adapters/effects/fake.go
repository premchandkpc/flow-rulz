package effects

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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

// HTTPDestination sends effects to an HTTP endpoint.
type HTTPDestination struct {
	endpoint string
	client   *http.Client
}

// NewHTTPDestination creates a new HTTPDestination for the given endpoint.
func NewHTTPDestination(endpoint string) *HTTPDestination {
	return &HTTPDestination{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// Send delivers an effect to the HTTP endpoint.
func (h *HTTPDestination) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	data, err := json.Marshal(effect)
	if err != nil {
		return fmt.Errorf("marshal effect: %w", err)
	}

	url := h.endpoint + "/" + effect.Destination + "/" + effect.Name
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Effect-ID", effect.ID)
	req.Header.Set("X-Execution-ID", effect.ExecutionID)
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("send effect: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		// Drain only a bounded amount for connection reuse. Never copy a
		// destination response body into the returned error or logs: it may
		// contain credentials or sensitive downstream data.
		const maxDrainBytes = 4 << 10
		if _, drainErr := io.Copy(io.Discard, io.LimitReader(resp.Body, maxDrainBytes)); drainErr != nil {
			return fmt.Errorf("destination returned status %d; drain error response: %w", resp.StatusCode, drainErr)
		}
		return fmt.Errorf("destination returned non-success status %d", resp.StatusCode)
	}

	log.Printf("HTTPDestination: sent effect %s to %s/%s (idempotency: %s)", effect.ID, effect.Destination, effect.Name, idempotencyKey)
	return nil
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
