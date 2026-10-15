package events

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type eventTxStub struct {
	ctx       context.Context
	committed bool
}

func (tx *eventTxStub) Commit(context.Context) error   { tx.committed = true; return nil }
func (tx *eventTxStub) Rollback(context.Context) error { return nil }
func (tx *eventTxStub) Context() context.Context       { return tx.ctx }
func (*eventTxStub) FencingToken() int64               { return 1 }
func (*eventTxStub) SetFencingToken(int64)             {}

type transactionManagerStub struct{ tx ports.Transaction }

func (m transactionManagerStub) Begin(context.Context) (ports.Transaction, error) {
	return m.tx, nil
}
func (transactionManagerStub) WithTransaction(context.Context, func(context.Context) error) error {
	return nil
}

type inboxStub struct {
	ports.InboxRepository
	inserted  bool
	committed bool
}

func (*inboxStub) Get(context.Context, string, string) (*domain.InboxEntry, error) {
	return nil, nil
}
func (r *inboxStub) Insert(context.Context, *domain.InboxEntry) (bool, error) {
	r.inserted = true
	return true, nil
}
func (r *inboxStub) MarkCommitted(context.Context, string, string, string) error {
	r.committed = true
	return nil
}

type activationStub struct {
	ports.ActivationRepository
	active bool
}

func (r activationStub) Get(context.Context, string, string) (*domain.RuleActivation, error) {
	if !r.active {
		return nil, nil
	}
	return &domain.RuleActivation{Revision: 1}, nil
}

type ruleRepositoryStub struct{ ports.RuleRepository }

func (ruleRepositoryStub) Get(_ context.Context, tenantScope, ruleSet string, revision int64) (*domain.RuleRevision, error) {
	return &domain.RuleRevision{TenantScope: tenantScope, RuleID: ruleSet, Revision: revision}, nil
}

func (ruleRepositoryStub) GetActive(context.Context, string, string) (*domain.RuleRevision, error) {
	return &domain.RuleRevision{Revision: 1}, nil
}

type executionStub struct {
	ports.ExecutionRepository
	saved *domain.Execution
}

func (*executionStub) Get(context.Context, string) (*domain.Execution, error) { return nil, nil }
func (r *executionStub) Save(_ context.Context, execution *domain.Execution) error {
	r.saved = execution
	return nil
}

type outboxStub struct {
	ports.OutboxRepository
	inserted []domain.OutboxEffect
}

func (r *outboxStub) Insert(_ context.Context, effects []domain.OutboxEffect) error {
	r.inserted = append(r.inserted, effects...)
	return nil
}

type shardLeaseStub struct{ ports.ShardLeaseRepository }

func (shardLeaseStub) AssertHeld(context.Context, uint32, string, int64) error { return nil }

type eventEvaluatorStub struct{}

func (eventEvaluatorStub) Evaluate(*domain.RuleRevision, *domain.EventEnvelope, map[string]json.RawMessage) (*domain.Decision, error) {
	return &domain.Decision{Hash: "decision-hash"}, nil
}

func setupEventService(activeRule bool) (*Service, *inboxStub, *executionStub, *outboxStub, *eventTxStub) {
	ctx := context.Background()
	tx := &eventTxStub{ctx: ctx}
	inbox := &inboxStub{}
	executions := &executionStub{}
	outbox := &outboxStub{}
	repos := ports.EventTxRepos{
		Inbox: inbox, Activations: activationStub{active: activeRule},
		RuleRepo: ruleRepositoryStub{}, Executions: executions, Outbox: outbox,
		ShardLeases: shardLeaseStub{},
	}
	service := NewService(
		nil, eventEvaluatorStub{}, nil, domain.SystemClock{}, transactionManagerStub{tx: tx},
		func(ports.Transaction) (ports.EventTxRepos, error) { return repos, nil },
	)
	return service, inbox, executions, outbox, tx
}

func testEnvelope() *domain.EventEnvelope {
	return &domain.EventEnvelope{
		ID: "event-1", Type: "orders.created", TenantID: "tenant-a",
		PartitionKey: "order-1", OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"order_id":"order-1"}`),
	}
}

func TestProcessDoesNotNotifySubscribersWithoutActiveRule(t *testing.T) {
	service, inbox, _, outbox, tx := setupEventService(false)
	_, err := service.Process(context.Background(), testEnvelope(), 1, 0, "worker-a")
	if !errors.Is(err, domain.ErrRuleNotReady) {
		t.Fatalf("Process() error = %v, want ErrRuleNotReady", err)
	}
	if len(outbox.inserted) != 0 || tx.committed {
		t.Fatalf("no-active-rule event had outbox=%d committed=%t", len(outbox.inserted), tx.committed)
	}
	// New behavior: inbox is not inserted when no active rule exists (checked before dedup)
	if inbox.inserted {
		t.Fatal("unexpected inbox insert when no active rule")
	}
}

func TestProcessWritesOnlyRuleDeclaredEffectsToOutbox(t *testing.T) {
	service, inbox, executions, outbox, tx := setupEventService(true)
	execution, err := service.Process(context.Background(), testEnvelope(), 1, 0, "worker-a")
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if !tx.committed || !inbox.committed || executions.saved == nil || execution == nil {
		t.Fatalf("transaction committed=%t inbox committed=%t execution saved=%t returned=%t",
			tx.committed, inbox.committed, executions.saved != nil, execution != nil)
	}
	if len(outbox.inserted) != 0 {
		t.Fatalf("outbox effects=%d, want 0 because the ruleset declared no effects", len(outbox.inserted))
	}
}

type quarantineSaveStub struct {
	ports.QuarantineRepository
	entries []*domain.QuarantineEntry
}

func (q *quarantineSaveStub) Save(_ context.Context, entry *domain.QuarantineEntry) error {
	copyEntry := *entry
	q.entries = append(q.entries, &copyEntry)
	return nil
}

func TestQuarantineEventUsesStableIDForRedelivery(t *testing.T) {
	quarantine := &quarantineSaveStub{}
	service := NewService(nil, nil, quarantine, domain.SystemClock{}, nil, nil)
	for i := 0; i < 2; i++ {
		if err := service.QuarantineEvent(context.Background(), "source-1", "event-1", "tenant-a", domain.ErrorClass("permanent"), "invalid event"); err != nil {
			t.Fatalf("QuarantineEvent() error = %v", err)
		}
	}
	if len(quarantine.entries) != 2 {
		t.Fatalf("saved %d quarantine entries, want 2 attempts", len(quarantine.entries))
	}
	if quarantine.entries[0].ID == "" || quarantine.entries[0].ID != quarantine.entries[1].ID {
		t.Fatalf("quarantine IDs differ across redelivery: %q vs %q", quarantine.entries[0].ID, quarantine.entries[1].ID)
	}
}
