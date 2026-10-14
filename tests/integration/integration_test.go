// Package integration provides integration tests for event and rule processing.
package integration

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/rules"
	svcevents "github.com/flowrulz/flow-rulz/internal/services/events"
	svcrules "github.com/flowrulz/flow-rulz/internal/services/rules"
)

type mockInbox struct {
	entries map[string]*domain.InboxEntry
}

func newMockInbox() *mockInbox {
	return &mockInbox{entries: make(map[string]*domain.InboxEntry)}
}

// Insert persists the supplied value.
func (m *mockInbox) Insert(_ context.Context, entry *domain.InboxEntry) (bool, error) {
	key := entry.TenantID + ":" + entry.EventID
	if _, exists := m.entries[key]; exists {
		return false, nil
	}
	m.entries[key] = entry
	return true, nil
}

// Get retrieves the requested value.
func (m *mockInbox) Get(_ context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	key := tenantID + ":" + eventID
	return m.entries[key], nil
}

// GetByRuleSet retrieves the requested value by tenant, event ID, and ruleset.
func (m *mockInbox) GetByRuleSet(_ context.Context, tenantID, eventID, ruleSet string) (*domain.InboxEntry, error) {
	key := tenantID + ":" + eventID + ":" + ruleSet
	return m.entries[key], nil
}

// MarkCommitted persists the supplied value.
func (m *mockInbox) MarkCommitted(_ context.Context, tenantID string, eventID string, executionID string) error {
	key := tenantID + ":" + eventID
	if entry, ok := m.entries[key]; ok {
		entry.Status = domain.InboxStatusCommitted
		entry.ExecutionID = executionID
		now := time.Now().UTC()
		entry.CommittedAt = &now
	}
	return nil
}

type mockActivation struct {
	activations map[string]*domain.RuleActivation
}

func newMockActivation() *mockActivation {
	return &mockActivation{activations: make(map[string]*domain.RuleActivation)}
}

// Get retrieves the requested value.
func (m *mockActivation) Get(_ context.Context, tenantScope string, ruleSet string) (*domain.RuleActivation, error) {
	key := tenantScope + ":" + ruleSet
	return m.activations[key], nil
}

// Set persists the supplied value.
func (m *mockActivation) Set(_ context.Context, activation *domain.RuleActivation) error {
	key := activation.TenantScope + ":" + activation.RuleSet
	m.activations[key] = activation
	return nil
}

// Delete deletes the requested record.
func (m *mockActivation) Delete(_ context.Context, tenantScope string, ruleSet string) error {
	key := tenantScope + ":" + ruleSet
	delete(m.activations, key)
	return nil
}

// SetWithVersion provides the corresponding repository operation.
func (m *mockActivation) SetWithVersion(_ context.Context, activation *domain.RuleActivation, expectedVersion int64) error {
	key := activation.TenantScope + ":" + activation.RuleSet
	current := m.activations[key]
	if current != nil && current.Version != expectedVersion {
		return domain.ErrActivationConflict
	}
	if current == nil && expectedVersion != 0 {
		return domain.ErrActivationConflict
	}
	activation.Version = expectedVersion + 1
	m.activations[key] = activation
	return nil
}

// List returns matching records.
func (m *mockActivation) List(_ context.Context, tenantScope string) ([]*domain.RuleActivation, error) {
	var result []*domain.RuleActivation
	for _, act := range m.activations {
		if act.TenantScope == tenantScope {
			result = append(result, act)
		}
	}
	return result, nil
}

type mockRuleRepo struct {
	revisions map[string]*domain.RuleRevision
}

func newMockRuleRepo() *mockRuleRepo {
	return &mockRuleRepo{revisions: make(map[string]*domain.RuleRevision)}
}

// GetActive retrieves the requested value.
func (m *mockRuleRepo) GetActive(_ context.Context, tenantScope string, ruleSet string) (*domain.RuleRevision, error) {
	key := tenantScope + ":" + ruleSet
	return m.revisions[key], nil
}

// Save persists the supplied value.
func (m *mockRuleRepo) Save(_ context.Context, revision *domain.RuleRevision) error {
	key := revision.TenantScope + ":" + revision.RuleID
	m.revisions[key] = revision
	return nil
}

// Delete deletes the requested record.
func (m *mockRuleRepo) Delete(_ context.Context, tenantScope, ruleSet string, _ int64) error {
	key := tenantScope + ":" + ruleSet
	delete(m.revisions, key)
	return nil
}

// Get retrieves the requested value.
func (m *mockRuleRepo) Get(_ context.Context, tenantScope, ruleSet string, _ int64) (*domain.RuleRevision, error) {
	key := tenantScope + ":" + ruleSet
	return m.revisions[key], nil
}

// List returns matching records.
func (m *mockRuleRepo) List(_ context.Context, tenantScope string) ([]*domain.RuleRevision, error) {
	var result []*domain.RuleRevision
	for _, rev := range m.revisions {
		if rev.TenantScope == tenantScope {
			result = append(result, rev)
		}
	}
	return result, nil
}

type mockExecutionRepo struct {
	executions map[string]*domain.Execution
}

func newMockExecutionRepo() *mockExecutionRepo {
	return &mockExecutionRepo{executions: make(map[string]*domain.Execution)}
}

// Save persists the supplied value.
func (m *mockExecutionRepo) Save(_ context.Context, execution *domain.Execution) error {
	m.executions[execution.ID] = execution
	return nil
}

// Get retrieves the requested value.
func (m *mockExecutionRepo) Get(_ context.Context, executionID string) (*domain.Execution, error) {
	return m.executions[executionID], nil
}

// UpdateStatus persists the supplied value.
func (m *mockExecutionRepo) UpdateStatus(_ context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error {
	if exec, ok := m.executions[executionID]; ok {
		exec.Status = status
		exec.Error = errMsg
		if status == domain.ExecutionStatusCompleted || status == domain.ExecutionStatusFailed {
			now := time.Now().UTC()
			exec.CompletedAt = &now
		}
	}
	return nil
}

type mockOutbox struct {
	effects map[string]*domain.OutboxEffect
}

func newMockOutbox() *mockOutbox {
	return &mockOutbox{effects: make(map[string]*domain.OutboxEffect)}
}

// Insert persists the supplied value.
func (m *mockOutbox) Insert(_ context.Context, effects []domain.OutboxEffect) error {
	for _, ef := range effects {
		m.effects[ef.ID] = &ef
	}
	return nil
}

// ClaimPending provides the corresponding repository operation.
func (m *mockOutbox) ClaimPending(_ context.Context, batchSize int, _ string, _ time.Duration) ([]domain.OutboxEffect, error) {
	var claimed []domain.OutboxEffect
	for _, ef := range m.effects {
		if ef.Status == domain.OutboxStatusPending && !ef.AvailableAt.After(time.Now().UTC()) {
			claimed = append(claimed, *ef)
			if len(claimed) >= batchSize {
				break
			}
		}
	}
	return claimed, nil
}

// MarkDelivered provides the corresponding repository operation.
func (m *mockOutbox) MarkDelivered(_ context.Context, effectIDs []string, _ string) error {
	for _, effectID := range effectIDs {
		if ef, ok := m.effects[effectID]; ok {
			ef.Status = domain.OutboxStatusDelivered
		}
	}
	return nil
}

// ScheduleRetry provides the corresponding repository operation.
func (m *mockOutbox) ScheduleRetry(_ context.Context, effectID, _ string, delay time.Duration, attempts int, errMsg string) error {
	if ef, ok := m.effects[effectID]; ok {
		ef.Attempts = attempts
		ef.LastError = errMsg
		ef.AvailableAt = time.Now().UTC().Add(delay)
	}
	return nil
}

// Quarantine provides the corresponding repository operation.
func (m *mockOutbox) Quarantine(_ context.Context, effectID, _ string, errMsg string) error {
	if ef, ok := m.effects[effectID]; ok {
		ef.Status = domain.OutboxStatusQuarantined
		ef.LastError = errMsg
	}
	return nil
}

// GetPending provides the corresponding repository operation.
func (m *mockOutbox) GetPending(_ context.Context, limit int) ([]domain.OutboxEffect, error) {
	var pending []domain.OutboxEffect
	for _, ef := range m.effects {
		if ef.Status == domain.OutboxStatusPending {
			pending = append(pending, *ef)
			if len(pending) >= limit {
				break
			}
		}
	}
	return pending, nil
}

// GetByExecution provides the corresponding repository operation.
func (m *mockOutbox) GetByExecution(_ context.Context, executionID string) ([]domain.OutboxEffect, error) {
	var result []domain.OutboxEffect
	for _, ef := range m.effects {
		if ef.ExecutionID == executionID {
			result = append(result, *ef)
		}
	}
	return result, nil
}

type mockQuarantine struct {
	entries map[string]*domain.QuarantineEntry
}

func newMockQuarantine() *mockQuarantine {
	return &mockQuarantine{entries: make(map[string]*domain.QuarantineEntry)}
}

// Save persists the supplied value.
func (m *mockQuarantine) Save(_ context.Context, entry *domain.QuarantineEntry) error {
	m.entries[entry.ID] = entry
	return nil
}

// Get retrieves the requested value.
func (m *mockQuarantine) Get(_ context.Context, _, id string) (*domain.QuarantineEntry, error) {
	return m.entries[id], nil
}

// List returns matching records.
func (m *mockQuarantine) List(_ context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	var result []*domain.QuarantineEntry
	for _, entry := range m.entries {
		result = append(result, entry)
		if filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}

// Replay replays the selected quarantined entry.
func (m *mockQuarantine) Replay(_ context.Context, _, _ string) error {
	return nil
}

// Delete deletes the requested record.
func (m *mockQuarantine) Delete(_ context.Context, _, id string) error {
	delete(m.entries, id)
	return nil
}

func setupTestUseCase(t *testing.T) (*svcevents.Service, *mockInbox, *mockExecutionRepo, *mockOutbox) {
	t.Helper()
	compiler := rules.NewCompiler(rules.DefaultLimits())
	clock := domain.SystemClock{}
	evaluator := rules.NewEvaluator(clock)

	inbox := newMockInbox()
	activations := newMockActivation()
	ruleRepo := newMockRuleRepo()
	executions := newMockExecutionRepo()
	outbox := newMockOutbox()
	quarantine := newMockQuarantine()

	mockTx := &mockTx{
		inbox:       inbox,
		activations: activations,
		ruleRepo:    ruleRepo,
		executions:  executions,
		outbox:      outbox,
	}
	txManager := newMockTxManager(mockTx)

	newRepos := func(_ ports.Transaction) (ports.EventTxRepos, error) {
		return ports.EventTxRepos{
			Inbox:          inbox,
			Activations:    activations,
			RuleRepo:       ruleRepo,
			Executions:     executions,
			Outbox:         outbox,
			ShardLeases:    newMockShardLease(),
			DomainServices: noSubscribers{},
			Bindings:       newMockBinding(),
		}, nil
	}

	uc := svcevents.NewService(
		compiler, evaluator, quarantine, clock, txManager, newRepos,
	)

	revision, err := compiler.Compile(json.RawMessage(`{
		"rule_set": "order.created",
		"revision": 1,
		"mode": "first_match",
		"rules": [{
			"id": "high-value",
			"priority": 100,
			"when": {
				"path": "$.total",
				"op": "gte",
				"value": 1000
			},
			"then": [{
				"emit": {
					"topic": "orders.review",
					"data": {"reason": "high_value"}
				}
			}]
		}]
	}`))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	if err := activations.Set(context.Background(), &domain.RuleActivation{
		TenantScope: "test-tenant",
		RuleSet:     "order.created",
		Revision:    1,
		Version:     1,
		Actor:       "test",
	}); err != nil {
		t.Fatalf("set activation: %v", err)
	}
	ruleRepo.revisions["test-tenant:order.created"] = revision

	return uc, inbox, executions, outbox
}

type mockTx struct {
	inbox       *mockInbox
	activations *mockActivation
	ruleRepo    *mockRuleRepo
	executions  *mockExecutionRepo
	outbox      *mockOutbox
	ctx         context.Context
}

// Commit provides the corresponding repository operation.
func (m *mockTx) Commit(_ context.Context) error { return nil }

// Rollback provides the corresponding repository operation.
func (m *mockTx) Rollback(_ context.Context) error { return nil }

// Context provides the corresponding repository operation.
func (m *mockTx) Context() context.Context { return m.ctx }

// FencingToken provides the corresponding repository operation.
func (m *mockTx) FencingToken() int64 { return 0 }

// SetFencingToken provides the corresponding repository operation.
func (m *mockTx) SetFencingToken(_ int64) {}

type mockTxManager struct {
	tx *mockTx
}

func newMockTxManager(tx *mockTx) *mockTxManager {
	return &mockTxManager{tx: tx}
}

// Begin provides the corresponding repository operation.
func (m *mockTxManager) Begin(ctx context.Context) (ports.Transaction, error) {
	m.tx.ctx = ctx
	return m.tx, nil
}

// WithTransaction provides the corresponding repository operation.
func (m *mockTxManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type mockShardLease struct{}

func newMockShardLease() *mockShardLease {
	return &mockShardLease{}
}

// Acquire provides the corresponding repository operation.
func (m *mockShardLease) Acquire(_ context.Context, shard uint32, owner string, _ time.Duration) (*domain.ShardLease, error) {
	return &domain.ShardLease{VirtualShard: shard, Owner: owner, FencingToken: 1}, nil
}

// Renew provides the corresponding repository operation.
func (m *mockShardLease) Renew(_ context.Context, shard uint32, owner string, fencingToken int64, _ time.Duration) (*domain.ShardLease, error) {
	return &domain.ShardLease{VirtualShard: shard, Owner: owner, FencingToken: fencingToken}, nil
}

// Release provides the corresponding repository operation.
func (m *mockShardLease) Release(_ context.Context, _ uint32, _ string) error {
	return nil
}

// GetOwner provides the corresponding repository operation.
func (m *mockShardLease) GetOwner(_ context.Context, shard uint32) (*domain.ShardLease, error) {
	return &domain.ShardLease{VirtualShard: shard, Owner: "test", FencingToken: 1}, nil
}

// ValidateFencingToken provides the corresponding repository operation.
func (m *mockShardLease) ValidateFencingToken(_ context.Context, _ uint32, _ string, _ int64) error {
	return nil
}

// AssertHeld provides the corresponding repository operation.
func (m *mockShardLease) AssertHeld(_ context.Context, _ uint32, _ string, _ int64) error {
	return nil
}

// TestDuplicateRedeliveryProducesOneExecution provides the corresponding repository operation.
func TestDuplicateRedeliveryProducesOneExecution(t *testing.T) {
	uc, _, executions, _ := setupTestUseCase(t)

	env := &domain.EventEnvelope{
		ID:           "evt-dup-1",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-1",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"total": 1500, "id": "order-1"}`),
	}

	exec1, err := uc.Process(context.Background(), env, 1, 1, "test-worker")
	if err != nil {
		t.Fatalf("first process: %v", err)
	}
	if exec1 == nil {
		t.Fatal("expected execution from first process")
	}

	exec2, err := uc.Process(context.Background(), env, 1, 1, "test-worker")
	if err != nil {
		t.Fatalf("second process: %v", err)
	}
	if exec2 == nil {
		t.Fatal("expected execution from second process")
	}

	if exec1.ID != exec2.ID {
		t.Errorf("expected same execution ID, got %s and %s", exec1.ID, exec2.ID)
	}

	totalExecutions := 0
	for _, exec := range executions.executions {
		if exec.EventID == "evt-dup-1" {
			totalExecutions++
		}
	}
	if totalExecutions != 1 {
		t.Errorf("expected 1 execution for event, got %d", totalExecutions)
	}
}

// TestDeterministicHashConsistency provides the corresponding repository operation.
func TestDeterministicHashConsistency(t *testing.T) {
	uc, _, _, _ := setupTestUseCase(t)

	env := &domain.EventEnvelope{
		ID:           "evt-det-1",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-2",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"total": 2000, "id": "order-2"}`),
	}

	exec1, _ := uc.Process(context.Background(), env, 1, 1, "test-worker")

	// Process same event again - should get same execution and hash
	exec2, _ := uc.Process(context.Background(), env, 1, 1, "test-worker")

	if exec1.DecisionHash == "" {
		t.Error("expected non-empty decision hash")
	}
	if exec2.DecisionHash == "" {
		t.Error("expected non-empty decision hash")
	}
	if exec1.DecisionHash != exec2.DecisionHash {
		t.Errorf("expected same decision hash for same event: %s != %s", exec1.DecisionHash, exec2.DecisionHash)
	}
	if exec1.ID != exec2.ID {
		t.Errorf("expected same execution ID for same event: %s != %s", exec1.ID, exec2.ID)
	}
}

// TestEffectIDGenerationIsDeterministic provides the corresponding repository operation.
func TestEffectIDGenerationIsDeterministic(t *testing.T) {
	id1 := domain.ComputeEffectID("tenant", "event-1", "rule-set", 1, "rule-1", 0)
	id2 := domain.ComputeEffectID("tenant", "event-1", "rule-set", 1, "rule-1", 0)

	if id1 != id2 {
		t.Errorf("expected same effect ID: %s != %s", id1, id2)
	}

	id3 := domain.ComputeEffectID("tenant", "event-1", "rule-set", 1, "rule-1", 1)
	if id1 == id3 {
		t.Error("expected different effect ID for different action index")
	}
}

// TestActivateRuleUsesExplicitTenantScope provides the corresponding repository operation.
func TestActivateRuleUsesExplicitTenantScope(t *testing.T) {
	compiler := rules.NewCompiler(rules.DefaultLimits())
	ruleRepo := newMockRuleRepo()
	activations := newMockActivation()
	executions := newMockExecutionRepo()
	txManager := newMockTxManager(&mockTx{
		ruleRepo:    ruleRepo,
		activations: activations,
	})
	// Mock RepositoryFactory that returns tx-scoped repos
	newTxRepos := func(_ ports.Transaction) (ports.RuleTxRepos, error) {
		return ports.RuleTxRepos{
			Activations: activations,
			RuleRepo:    ruleRepo,
		}, nil
	}
	svc := svcrules.NewService(compiler, ruleRepo, activations, executions, domain.SystemClock{}, txManager, newTxRepos)

	_, err := svc.Activate(context.Background(), "tenant-a", "order.created", json.RawMessage(`{
		"rule_set":"order.created", "revision":1, "mode":"first_match",
		"rules":[{"id":"match", "priority":1,
		"when":{"path":"$.total", "op":"gte", "value":1},
		"then":[{"emit":{"topic":"orders.review", "data":{}}}]}]
	}`), "test")
	if err != nil {
		t.Fatalf("activate rule: %v", err)
	}
	if ruleRepo.revisions["tenant-a:order.created"] == nil {
		t.Fatal("expected rule revision to be saved under the supplied tenant scope")
	}
	if ruleRepo.revisions["order.created:order.created"] != nil {
		t.Fatal("rule revision must not derive tenant scope from the rule ID")
	}
}

// TestGivenActiveRule_WhenEventArrives_ThenExecutionAndOutboxAreCreated provides the corresponding repository operation.
func TestGivenActiveRule_WhenEventArrives_ThenExecutionAndOutboxAreCreated(t *testing.T) {
	uc, inbox, executions, outbox := setupTestUseCase(t)
	env := &domain.EventEnvelope{
		ID:           "evt-given-1",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-42",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"total": 1500, "id": "order-42"}`),
	}

	exec, err := uc.Process(context.Background(), env, 1, 1, "test-worker")
	if err != nil {
		t.Fatalf("process event: %v", err)
	}
	if exec == nil {
		t.Fatal("expected execution")
	}
	if inbox.entries["test-tenant:evt-given-1"].Status != domain.InboxStatusCommitted {
		t.Fatal("expected inbox entry to be committed")
	}
	if len(executions.executions) != 1 {
		t.Fatalf("expected one execution, got %d", len(executions.executions))
	}
	if len(outbox.effects) != 1 {
		t.Fatalf("expected one outbox effect, got %d", len(outbox.effects))
	}
}

// noSubscribers is a domain service repository with no tenant subscriptions.
type noSubscribers struct{ ports.DomainServiceRepository }

// ListSubscribersForEvent returns matching records.
func (noSubscribers) ListSubscribersForEvent(context.Context, string, string) ([]*domain.DomainService, error) {
	return nil, nil
}

// mockBinding is a no-op binding repository for tests.
type mockBinding struct{ ports.BindingRepository }

func newMockBinding() *mockBinding {
	return &mockBinding{}
}

func (mockBinding) ListByContract(context.Context, string, string, string) ([]*domain.ContractBinding, error) {
	return nil, nil
}

func (mockBinding) ListByRuleSet(context.Context, string, string) ([]*domain.ContractBinding, error) {
	return nil, nil
}

func (mockBinding) Save(context.Context, *domain.ContractBinding) error { return nil }
func (mockBinding) Get(context.Context, string, string) (*domain.ContractBinding, error) {
	return nil, nil
}
func (mockBinding) Delete(context.Context, string, string) error { return nil }
