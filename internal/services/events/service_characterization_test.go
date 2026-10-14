package events

import (
	"context"
	"errors"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// countingTransactionManager records whether Process crossed the transaction
// boundary. These tests capture the current public contract before refactoring.
type countingTransactionManager struct {
	calls int
	tx    ports.Transaction
	err   error
}

func (m *countingTransactionManager) Begin(context.Context) (ports.Transaction, error) {
	m.calls++
	return m.tx, m.err
}

func (*countingTransactionManager) WithTransaction(context.Context, func(context.Context) error) error {
	return nil
}

// bindingStub is a no-op binding repository for legacy test compatibility.
type bindingStub struct{ ports.BindingRepository }

func (bindingStub) ListByContract(context.Context, string, string, string) ([]*domain.ContractBinding, error) {
	return nil, nil
}

func (bindingStub) ListByRuleSet(context.Context, string, string) ([]*domain.ContractBinding, error) {
	return nil, nil
}

func (bindingStub) Save(context.Context, *domain.ContractBinding) error { return nil }
func (bindingStub) Get(context.Context, string, string) (*domain.ContractBinding, error) { return nil, nil }
func (bindingStub) Delete(context.Context, string, string) error { return nil }

type failingShardLeaseRepository struct {
	ports.ShardLeaseRepository
	err error
}

func (r failingShardLeaseRepository) AssertHeld(context.Context, uint32, string, int64) error {
	return r.err
}

func TestProcessReturnsBeginTransactionError(t *testing.T) {
	beginErr := errors.New("database unavailable")
	manager := &countingTransactionManager{err: beginErr}
	service := NewService(nil, nil, nil, domain.SystemClock{}, manager, nil)

	_, err := service.Process(context.Background(), testEnvelope(), 1, 0, "worker-a")
	if !errors.Is(err, beginErr) {
		t.Fatalf("Process() error = %v, want wrapped begin error", err)
	}
	if manager.calls != 1 {
		t.Fatalf("Begin() calls = %d, want 1", manager.calls)
	}
}

func TestProcessRejectsNonPositiveFencingTokenBeforeBeginningTransaction(t *testing.T) {
	manager := &countingTransactionManager{tx: &eventTxStub{ctx: context.Background()}}
	service := NewService(nil, nil, nil, domain.SystemClock{}, manager, nil)

	_, err := service.Process(context.Background(), testEnvelope(), 0, 0, "worker-a")
	if !errors.Is(err, domain.ErrFencingTokenMismatch) {
		t.Fatalf("Process() error = %v, want ErrFencingTokenMismatch", err)
	}
	if manager.calls != 0 {
		t.Fatalf("Begin() called %d times, want 0 for invalid fencing token", manager.calls)
	}
}

func TestProcessValidatesShardLeaseBeforeMutatingInbox(t *testing.T) {
	ctx := context.Background()
	tx := &eventTxStub{ctx: ctx}
	inbox := &inboxStub{}
	leaseErr := errors.New("lease lost")
	repos := ports.EventTxRepos{
		Inbox:       inbox,
		ShardLeases: failingShardLeaseRepository{err: leaseErr},
		Bindings:    bindingStub{},
	}
	manager := &countingTransactionManager{tx: tx}
	service := NewService(
		nil,
		eventEvaluatorStub{},
		nil,
		domain.SystemClock{},
		manager,
		func(ports.Transaction) (ports.EventTxRepos, error) {
			return repos, nil
		},
	)

	_, err := service.Process(ctx, testEnvelope(), 7, 12, "worker-a")
	if !errors.Is(err, leaseErr) {
		t.Fatalf("Process() error = %v, want wrapped lease error", err)
	}
	if tx.committed {
		t.Fatal("transaction committed after shard lease validation failed")
	}
	if inbox.inserted {
		t.Fatal("inbox was mutated before shard lease validation succeeded")
	}
}

type duplicateInboxRepository struct {
	ports.InboxRepository
	entry   *domain.InboxEntry
	inserts int
	marks   int
}

func (r *duplicateInboxRepository) Get(context.Context, string, string) (*domain.InboxEntry, error) {
	return r.entry, nil
}

func (r *duplicateInboxRepository) Insert(context.Context, *domain.InboxEntry) (bool, error) {
	r.inserts++
	return false, nil
}

func (r *duplicateInboxRepository) MarkCommitted(context.Context, string, string, string) error {
	r.marks++
	return nil
}

type existingExecutionRepository struct {
	ports.ExecutionRepository
	execution *domain.Execution
	saves     int
}

func (r *existingExecutionRepository) Get(context.Context, string) (*domain.Execution, error) {
	return r.execution, nil
}

func (r *existingExecutionRepository) Save(context.Context, *domain.Execution) error {
	r.saves++
	return nil
}

func TestProcessReturnsExistingExecutionForRedelivery(t *testing.T) {
	ctx := context.Background()
	existing := &domain.Execution{ID: "execution-existing", EventID: "event-1", TenantID: "tenant-a"}
	inbox := &duplicateInboxRepository{
		entry: &domain.InboxEntry{TenantID: "tenant-a", EventID: "event-1", ExecutionID: existing.ID, RuleSet: "orders.created"},
	}
	executions := &existingExecutionRepository{execution: existing}
	tx := &eventTxStub{ctx: ctx}
	repos := ports.EventTxRepos{
		Inbox: inbox, Executions: executions, ShardLeases: shardLeaseStub{},
		Activations: activationStub{active: true}, RuleRepo: ruleRepositoryStub{}, Bindings: bindingStub{},
	}
	service := NewService(
		nil, nil, nil, domain.SystemClock{}, transactionManagerStub{tx: tx},
		func(ports.Transaction) (ports.EventTxRepos, error) { return repos, nil },
	)

	got, err := service.Process(ctx, testEnvelope(), 1, 0, "worker-a")
	if err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	if got != existing {
		t.Fatalf("Process() execution = %#v, want existing execution %#v", got, existing)
	}
	if inbox.inserts != 0 || inbox.marks != 0 || executions.saves != 0 {
		t.Fatalf("redelivery mutated state: inbox inserts=%d marks=%d execution saves=%d",
			inbox.inserts, inbox.marks, executions.saves)
	}
	if tx.committed {
		t.Fatal("duplicate read path unexpectedly committed a transaction")
	}
}

func TestProcessReturnsDuplicateErrorWhenInboxInsertLosesRaceWithoutExecution(t *testing.T) {
	ctx := context.Background()
	inbox := &duplicateInboxRepository{}
	tx := &eventTxStub{ctx: ctx}
	repos := ports.EventTxRepos{
		Inbox: inbox, ShardLeases: shardLeaseStub{},
		Activations: activationStub{active: true}, RuleRepo: ruleRepositoryStub{}, Bindings: bindingStub{},
	}
	service := NewService(
		nil, nil, nil, domain.SystemClock{}, transactionManagerStub{tx: tx},
		func(ports.Transaction) (ports.EventTxRepos, error) { return repos, nil },
	)

	_, err := service.Process(ctx, testEnvelope(), 1, 0, "worker-a")
	if !errors.Is(err, domain.ErrInboxDuplicate) {
		t.Fatalf("Process() error = %v, want ErrInboxDuplicate", err)
	}
	if inbox.inserts != 1 {
		t.Fatalf("inbox Insert() calls = %d, want 1", inbox.inserts)
	}
	if tx.committed {
		t.Fatal("transaction committed after an unresolved inbox deduplication conflict")
	}
}

type failingCommitTransaction struct {
	eventTxStub
	commitErr     error
	rollbackCalls int
}

func (tx *failingCommitTransaction) Commit(context.Context) error {
	return tx.commitErr
}

func (tx *failingCommitTransaction) Rollback(context.Context) error {
	tx.rollbackCalls++
	return nil
}

func TestProcessRollsBackWhenCommitFails(t *testing.T) {
	ctx := context.Background()
	commitErr := errors.New("database commit failed")
	tx := &failingCommitTransaction{
		eventTxStub: eventTxStub{ctx: ctx},
		commitErr:   commitErr,
	}
	inbox := &inboxStub{}
	executions := &executionStub{}
	outbox := &outboxStub{}
	subscribers := &subscriberRepositoryStub{}
	repos := ports.EventTxRepos{
		Inbox: inbox, Activations: activationStub{active: true},
		RuleRepo: ruleRepositoryStub{}, Executions: executions, Outbox: outbox,
		ShardLeases: shardLeaseStub{}, DomainServices: subscribers,
	}
	service := NewService(
		nil, eventEvaluatorStub{}, nil, domain.SystemClock{},
		transactionManagerStub{tx: tx},
		func(ports.Transaction) (ports.EventTxRepos, error) { return repos, nil },
	)

	_, err := service.Process(ctx, testEnvelope(), 1, 0, "worker-a")
	if !errors.Is(err, commitErr) {
		t.Fatalf("Process() error = %v, want wrapped commit error", err)
	}
	if tx.rollbackCalls != 1 {
		t.Fatalf("Rollback() calls = %d, want 1 after commit failure", tx.rollbackCalls)
	}
}
