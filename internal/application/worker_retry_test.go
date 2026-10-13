package application

import (
	"context"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type retryTestDelivery struct {
	attempt uint64
}

func (d retryTestDelivery) Event() (*domain.EventEnvelope, error) { return nil, nil }
func (d retryTestDelivery) Ack(context.Context) error             { return nil }
func (d retryTestDelivery) Nak(context.Context) error             { return nil }
func (d retryTestDelivery) Retry(context.Context, time.Duration) error {
	return nil
}
func (d retryTestDelivery) Raw() []byte                { return nil }
func (d retryTestDelivery) Headers() map[string]string { return nil }
func (d retryTestDelivery) Subject() string            { return "" }
func (d retryTestDelivery) Attempt() uint64            { return d.attempt }

var _ ports.Delivery = retryTestDelivery{}
var _ ports.DeliveryAttempt = retryTestDelivery{}

func TestRetryDelayForDeliveryUsesBoundedExponentialBackoff(t *testing.T) {
	base := 2 * time.Second
	tests := []struct {
		name    string
		attempt uint64
		min     time.Duration
		max     time.Duration
	}{
		{name: "first attempt", attempt: 1, min: time.Second, max: 2 * time.Second},
		{name: "second attempt", attempt: 2, min: 2 * time.Second, max: 4 * time.Second},
		{name: "third attempt", attempt: 3, min: 4 * time.Second, max: 8 * time.Second},
		{name: "capped attempt", attempt: 100, min: maxDeliveryRetryDelay / 2, max: maxDeliveryRetryDelay},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := retryDelayForDelivery(base, retryTestDelivery{attempt: tt.attempt})
			if got < tt.min || got > tt.max {
				t.Fatalf("retry delay = %s, want within [%s, %s]", got, tt.min, tt.max)
			}
		})
	}
}

func TestRetryDelayForDeliveryFallsBackWithoutAttemptMetadata(t *testing.T) {
	got := retryDelayForDelivery(2*time.Second, nil)
	if got < time.Second || got > 2*time.Second {
		t.Fatalf("retry delay = %s, want within [1s, 2s]", got)
	}
}

func TestRetryDelayForDeliveryUsesDefaultForNonPositiveBase(t *testing.T) {
	got := retryDelayForDelivery(0, nil)
	if got < 2500*time.Millisecond || got > 5*time.Second {
		t.Fatalf("retry delay = %s, want within [2.5s, 5s]", got)
	}
}
