package application

import (
	"testing"

	"github.com/flowrulz/flow-rulz/internal/ports"
)

func TestDeliveryAttemptsExhausted(t *testing.T) {
	worker := &Worker{config: WorkerConfig{MaxDeliveryAttempts: 3}}
	for _, tc := range []struct {
		attempt   uint64
		exhausted bool
	}{
		{attempt: 1, exhausted: false},
		{attempt: 2, exhausted: false},
		{attempt: 3, exhausted: true},
		{attempt: 4, exhausted: true},
	} {
		if got := worker.deliveryAttemptsExhausted(retryTestDelivery{attempt: tc.attempt}); got != tc.exhausted {
			t.Fatalf("attempt=%d got=%v want=%v", tc.attempt, got, tc.exhausted)
		}
	}
}

type missingAttemptDelivery struct{ ports.Delivery }

func TestMissingAttemptMetadataFailsOpenToRetry(t *testing.T) {
	worker := &Worker{config: WorkerConfig{MaxDeliveryAttempts: 1}}
	if worker.deliveryAttemptsExhausted(&missingAttemptDelivery{}) {
		t.Fatal("do not quarantine without attempt metadata")
	}
}
