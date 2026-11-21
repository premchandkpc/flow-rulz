package application

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type observedEventProcessor struct {
	processErr    error
	quarantineErr error
}

func (p observedEventProcessor) Process(context.Context, *domain.EventEnvelope, int64, uint32, string) (*domain.Execution, error) {
	if p.processErr != nil {
		return nil, p.processErr
	}
	return &domain.Execution{ID: "execution"}, nil
}

func (p observedEventProcessor) QuarantineEvent(context.Context, string, string, string, domain.ErrorClass, string) error {
	return p.quarantineErr
}

type observedDelivery struct {
	retryTestDelivery
	ackErr   error
	retryErr error
}

func (d observedDelivery) Ack(context.Context) error { return d.ackErr }
func (d observedDelivery) Retry(context.Context, time.Duration) error {
	return d.retryErr
}

func TestProcessDeliveryObservesProcessingAndSettlementSeparately(t *testing.T) {
	ackErr := errors.New("ack failed")
	retryErr := errors.New("retry failed")
	quarantineErr := errors.New("quarantine failed")
	type observation struct {
		stage string
		err   error
	}
	for _, tc := range []struct {
		name      string
		processor observedEventProcessor
		delivery  observedDelivery
		want      []observation
		returnErr error
	}{
		{
			name: "successful processing and acknowledgement",
			want: []observation{{"process", nil}, {"ack", nil}},
		},
		{
			name:      "successful processing with failed acknowledgement",
			delivery:  observedDelivery{ackErr: ackErr},
			want:      []observation{{"process", nil}, {"ack", ackErr}},
			returnErr: ackErr,
		},
		{
			name:      "transient processing error with failed retry request",
			processor: observedEventProcessor{processErr: domain.ErrDatabaseError},
			delivery:  observedDelivery{retryErr: retryErr},
			want:      []observation{{"process", domain.ErrDatabaseError}, {"retry", retryErr}},
			returnErr: retryErr,
		},
		{
			name:      "permanent error quarantined and acknowledged",
			processor: observedEventProcessor{processErr: domain.ErrInvalidEventEnvelope},
			want:      []observation{{"process", domain.ErrInvalidEventEnvelope}, {"quarantine", nil}, {"ack", nil}},
		},
		{
			name:      "failed quarantine requests a retry",
			processor: observedEventProcessor{processErr: domain.ErrInvalidEventEnvelope, quarantineErr: quarantineErr},
			want:      []observation{{"process", domain.ErrInvalidEventEnvelope}, {"quarantine", quarantineErr}, {"retry", nil}},
			returnErr: quarantineErr,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got []observation
			worker := Worker{
				events: tc.processor,
				config: WorkerConfig{ObserveDeliveryStage: func(stage string, err error, elapsed time.Duration) {
					if elapsed < 0 {
						t.Errorf("negative elapsed time for stage %s: %v", stage, elapsed)
					}
					got = append(got, observation{stage, err})
				}},
			}
			err := worker.processDelivery(context.Background(), &domain.EventEnvelope{ID: "event"}, tc.delivery, 1, 0, "worker")
			if !errors.Is(err, tc.returnErr) {
				t.Errorf("processDelivery() error = %v, want %v", err, tc.returnErr)
			}
			if !slices.Equal(got, tc.want) {
				t.Errorf("observations = %v, want %v", got, tc.want)
			}
		})
	}
}
