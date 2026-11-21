package effects

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type testOutboxRepository struct {
	effects         []domain.OutboxEffect
	claimToken      string
	deliveredToken  string
	retryToken      string
	quarantineToken string
	deliveredErr    error
	retryErr        error
	quarantineErr   error
	quarantineCalls int
	retryCalls      int
	deliveredCalls  int
	retryAttempts   int
	claimTTL        time.Duration
}

func (r *testOutboxRepository) Insert(context.Context, []domain.OutboxEffect) error { return nil }
func (r *testOutboxRepository) ClaimPending(_ context.Context, _ int, claimant string, ttl time.Duration) ([]domain.OutboxEffect, error) {
	r.claimToken = claimant
	r.claimTTL = ttl
	effects := append([]domain.OutboxEffect(nil), r.effects...)
	for i := range effects {
		effects[i].Attempts++
	}
	return effects, nil
}
func (r *testOutboxRepository) MarkDelivered(_ context.Context, _ []string, claimant string) error {
	r.deliveredCalls++
	r.deliveredToken = claimant
	return r.deliveredErr
}
func (r *testOutboxRepository) ScheduleRetry(_ context.Context, _ string, claimant string, _ time.Duration, attempts int, _ string) error {
	r.retryCalls++
	r.retryToken = claimant
	r.retryAttempts = attempts
	return r.retryErr
}
func (r *testOutboxRepository) Quarantine(_ context.Context, _ string, claimant, _ string) error {
	r.quarantineCalls++
	r.quarantineToken = claimant
	return r.quarantineErr
}
func (r *testOutboxRepository) GetPending(context.Context, int) ([]domain.OutboxEffect, error) {
	return nil, nil
}
func (r *testOutboxRepository) GetByExecution(context.Context, string) ([]domain.OutboxEffect, error) {
	return nil, nil
}

type testEffectSender struct {
	err   error
	calls int
}

type blockingEffectSender struct{ calls int }

func (s *blockingEffectSender) Send(ctx context.Context, _ *domain.Effect, _ string) error {
	s.calls++
	<-ctx.Done()
	return ctx.Err()
}

func (s *testEffectSender) Send(context.Context, *domain.Effect, string) error {
	s.calls++
	return s.err
}

type testQuarantineRepository struct {
	saved     []*domain.QuarantineEntry
	deleted   []string
	saveErr   error
	deleteErr error
}

func (r *testQuarantineRepository) Save(_ context.Context, entry *domain.QuarantineEntry) error {
	entryCopy := *entry
	r.saved = append(r.saved, &entryCopy)
	return r.saveErr
}
func (r *testQuarantineRepository) Get(context.Context, string, string) (*domain.QuarantineEntry, error) {
	return nil, nil
}
func (r *testQuarantineRepository) List(context.Context, ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	return nil, nil
}
func (r *testQuarantineRepository) Replay(context.Context, string, string) error { return nil }
func (r *testQuarantineRepository) Delete(_ context.Context, _, id string) error {
	r.deleted = append(r.deleted, id)
	return r.deleteErr
}

func TestPublishBatchUsesUniqueClaimTokenForDelivery(t *testing.T) {
	outbox := &testOutboxRepository{effects: []domain.OutboxEffect{{
		ID: "effect-1", ExecutionID: "exec-1", Destination: "orders", Name: "created",
		Payload: []byte(`{}`), EffectType: domain.EffectTypeCommand, MaxAttempts: 3,
	}}}
	svc := NewService(outbox, &testEffectSender{}, &testQuarantineRepository{}, domain.SystemClock{}, "publisher")
	if err := svc.PublishBatch(context.Background(), 10); err != nil {
		t.Fatalf("PublishBatch() error = %v", err)
	}
	if outbox.claimToken == "" || outbox.claimToken == "publisher" {
		t.Fatalf("claim token = %q, want unique token", outbox.claimToken)
	}
	if outbox.deliveredToken != outbox.claimToken || outbox.deliveredCalls != 1 {
		t.Fatalf("MarkDelivered token=%q claim token=%q calls=%d", outbox.deliveredToken, outbox.claimToken, outbox.deliveredCalls)
	}
}

func TestPublishBatchPropagatesRetrySettlementFailure(t *testing.T) {
	retryErr := errors.New("database unavailable")
	outbox := singleEffectOutbox(domain.OutboxEffect{
		ID: "effect-2", Destination: "orders", Name: "created", Payload: []byte(`{}`),
		EffectType: domain.EffectTypeCommand, Attempts: 0, MaxAttempts: 3,
	})
	outbox.retryErr = retryErr
	svc := NewService(outbox, &testEffectSender{err: errors.New("destination down")}, &testQuarantineRepository{}, domain.SystemClock{}, "publisher")
	err := svc.PublishBatch(context.Background(), 10)
	if !errors.Is(err, retryErr) {
		t.Fatalf("PublishBatch() error = %v, want retry error", err)
	}
	if outbox.retryToken != outbox.claimToken || outbox.retryCalls != 1 {
		t.Fatalf("ScheduleRetry token=%q claim token=%q calls=%d", outbox.retryToken, outbox.claimToken, outbox.retryCalls)
	}
}

// singleEffectOutbox returns an outbox repository that claims exactly one effect.
func singleEffectOutbox(effect domain.OutboxEffect) *testOutboxRepository {
	return &testOutboxRepository{effects: []domain.OutboxEffect{effect}}
}

func TestPublishBatchRemovesQuarantineRecordWhenClaimWasLost(t *testing.T) {
	claimErr := domain.ErrOutboxClaimLost
	outbox := singleEffectOutbox(domain.OutboxEffect{ID: "effect-3", Destination: "orders", Name: "created", Payload: []byte(`{}`), EffectType: domain.EffectTypeCommand, Attempts: 0, MaxAttempts: 1})
	outbox.quarantineErr = claimErr
	quarantine := &testQuarantineRepository{}
	svc := NewService(outbox, &testEffectSender{err: errors.New("permanent destination error")}, quarantine, domain.SystemClock{}, "publisher")
	err := svc.PublishBatch(context.Background(), 10)
	if !errors.Is(err, claimErr) {
		t.Fatalf("PublishBatch() error = %v, want claim error", err)
	}
	if outbox.quarantineToken != outbox.claimToken || outbox.quarantineCalls != 1 {
		t.Fatalf("Quarantine token=%q claim token=%q calls=%d", outbox.quarantineToken, outbox.claimToken, outbox.quarantineCalls)
	}
	assertClaimSpecificQuarantineCleanup(t, quarantine, "effect-effect-3-")
}

// assertClaimSpecificQuarantineCleanup verifies the claim-specific record was
// saved and deleted again after the outbox claim was lost.
func assertClaimSpecificQuarantineCleanup(t *testing.T, quarantine *testQuarantineRepository, effectIDPrefix string) {
	t.Helper()
	if len(quarantine.saved) != 1 || len(quarantine.deleted) != 1 || quarantine.deleted[0] != quarantine.saved[0].ID {
		t.Fatalf("quarantine saved=%v deleted=%v; want claim-specific record removed after lost claim", quarantine.saved, quarantine.deleted)
	}
	if !strings.HasPrefix(quarantine.saved[0].ID, effectIDPrefix) {
		t.Fatalf("quarantine ID = %q, expected claim-specific effect ID", quarantine.saved[0].ID)
	}
}

func TestPublishBatchPropagatesDeliverySettlementFailure(t *testing.T) {
	settleErr := errors.New("claim no longer owned")
	outbox := singleEffectOutbox(domain.OutboxEffect{
		ID: "effect-4", Destination: "orders", Name: "created", Payload: []byte(`{}`),
		EffectType: domain.EffectTypeCommand, MaxAttempts: 3,
	})
	outbox.deliveredErr = settleErr
	svc := NewService(outbox, &testEffectSender{}, &testQuarantineRepository{}, domain.SystemClock{}, "publisher")
	err := svc.PublishBatch(context.Background(), 10)
	if !errors.Is(err, settleErr) {
		t.Fatalf("PublishBatch() error = %v, want delivery settlement error", err)
	}
	if outbox.deliveredToken != outbox.claimToken {
		t.Fatalf("MarkDelivered token=%q claim token=%q", outbox.deliveredToken, outbox.claimToken)
	}
}

func TestExpiredFinalClaimQuarantinesWithoutSendingAgain(t *testing.T) {
	outbox := singleEffectOutbox(domain.OutboxEffect{
		ID: "effect-crashed", TenantID: "tenant-a", MaxAttempts: 1, Attempts: 1,
	})
	sender := &testEffectSender{}
	svc := NewService(outbox, sender, &testQuarantineRepository{}, nil, "worker")
	if err := svc.PublishBatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 0 || outbox.quarantineCalls != 1 {
		t.Fatalf("send calls=%d quarantine calls=%d", sender.calls, outbox.quarantineCalls)
	}
}

func TestDestinationSendUsesBoundedContext(t *testing.T) {
	outbox := singleEffectOutbox(domain.OutboxEffect{ID: "effect-timeout", MaxAttempts: 3})
	sender := &blockingEffectSender{}
	svc := NewService(outbox, sender, nil, nil, "worker")
	svc.sendTimeout = time.Millisecond
	if err := svc.PublishBatch(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	if sender.calls != 1 || outbox.retryCalls != 1 || outbox.retryAttempts != 1 {
		t.Fatalf("send=%d retry=%d attempts=%d", sender.calls, outbox.retryCalls, outbox.retryAttempts)
	}
}

func TestClaimTTLCoversSequentialBatch(t *testing.T) {
	outbox := &testOutboxRepository{}
	svc := NewService(outbox, &testEffectSender{}, nil, nil, "worker")
	svc.sendTimeout, svc.minimumTTL = 2*time.Second, time.Second
	if _, err := svc.PublishBatchCount(context.Background(), 3); err != nil {
		t.Fatal(err)
	}
	if want := 36 * time.Second; outbox.claimTTL != want {
		t.Fatalf("claim TTL=%s want=%s", outbox.claimTTL, want)
	}
}
