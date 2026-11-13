package application

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/rules"
)

type directTestConsumer struct{}

func (directTestConsumer) Next(context.Context) (ports.Delivery, error) { return nil, nil }

type directTestSender struct {
	steps  *[]string
	failAt int
	calls  int
	ids    []string
}

func (s *directTestSender) Send(_ context.Context, e *domain.Effect, key string) error {
	s.calls++
	*s.steps = append(*s.steps, "send")
	s.ids = append(s.ids, key)
	if key == "" || key != e.ID {
		return errors.New("missing stable identity")
	}
	if s.calls == s.failAt {
		return errors.New("broker unavailable")
	}
	return nil
}

type directTestDelivery struct {
	retryTestDelivery
	steps    *[]string
	invalid  bool
	ackErr   error
	retryErr error
}

func (d directTestDelivery) Event() (*domain.EventEnvelope, error) {
	if d.invalid {
		return nil, errors.New("invalid JSON")
	}
	return &domain.EventEnvelope{ID: "evt-1", TenantID: "tenant-1", PartitionKey: "key-1", Type: "a", OccurredAt: time.Now(), Data: []byte(`{"id":"1"}`)}, nil
}
func (d directTestDelivery) Ack(context.Context) error {
	*d.steps = append(*d.steps, "ack")
	return d.ackErr
}
func (d directTestDelivery) Retry(context.Context, time.Duration) error {
	*d.steps = append(*d.steps, "retry")
	return d.retryErr
}

func TestDirectPublishBeforeAckAndRetryPartialFailure(t *testing.T) {
	revision, err := rules.NewCompiler(rules.DefaultLimits()).Compile([]byte(`{"rule_set":"a","revision":1,"mode":"first_match","rules":[{"id":"forward","when":{"path":"$.id","op":"exists"},"then":[{"emit":{"topic":"b","data":{}}},{"emit_event":{"type":"next","partition_key_policy":"inherit","data":{}}}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		failAt  int
		invalid bool
		want    []string
	}{
		{"success", 0, false, []string{"send", "send", "ack"}},
		{"partial publish", 2, false, []string{"send", "send", "retry"}},
		{"malformed input", 0, true, []string{"retry"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var steps []string
			sender := &directTestSender{steps: &steps, failAt: tc.failAt}
			worker, err := NewDirectWorker(directTestConsumer{}, rules.NewEvaluator(domain.SystemClock{}), sender, []*domain.RuleRevision{revision}, time.Second, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			delivery := directTestDelivery{steps: &steps, invalid: tc.invalid}
			err = worker.ProcessDelivery(context.Background(), delivery)
			if (err != nil) != (tc.failAt > 0 || tc.invalid) {
				t.Fatalf("unexpected processing error: %v", err)
			}
			if !reflect.DeepEqual(steps, tc.want) {
				t.Fatalf("steps=%v want=%v", steps, tc.want)
			}
			if tc.failAt > 0 {
				firstIDs := append([]string{}, sender.ids...)
				if err := worker.ProcessDelivery(context.Background(), delivery); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(firstIDs, sender.ids[2:]) {
					t.Fatalf("redelivery changed output identities")
				}
			}
		})
	}
}

func TestDirectRejectsUnsupportedRules(t *testing.T) {
	for _, revision := range []*domain.RuleRevision{
		nil,
		{RuleID: "a", InputContract: &domain.ContractRef{Name: "contract"}},
		{RuleID: "a", Compiled: []domain.CompiledRule{{Otherwise: []domain.Action{{ScheduleEvent: &domain.ScheduleEventAction{}}}}}},
	} {
		var steps []string
		if _, err := NewDirectWorker(directTestConsumer{}, rules.NewEvaluator(domain.SystemClock{}), &directTestSender{steps: &steps}, []*domain.RuleRevision{revision}, time.Second, time.Second); err == nil {
			t.Fatal("accepted unsupported rule")
		}
	}
}

func TestDirectPreservesSettlementErrors(t *testing.T) {
	var steps []string
	w, err := NewDirectWorker(directTestConsumer{}, rules.NewEvaluator(domain.SystemClock{}), &directTestSender{steps: &steps}, []*domain.RuleRevision{{RuleID: "a"}}, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := errors.New("settlement failed")
	if err := w.ProcessDelivery(context.Background(), directTestDelivery{steps: &steps, ackErr: sentinel}); !errors.Is(err, sentinel) {
		t.Fatalf("ack error lost: %v", err)
	}
	if err := w.ProcessDelivery(context.Background(), directTestDelivery{steps: &steps, invalid: true, retryErr: sentinel}); !errors.Is(err, sentinel) {
		t.Fatalf("retry error lost: %v", err)
	}
}
