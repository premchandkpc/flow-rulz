// Package batches provides batch service tests.
package batches

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type memBatches struct {
	mu      sync.Mutex
	entries map[string]*domain.InboxEntry
	runs    map[string]*domain.BatchRun
}

type testLeaseProvider struct{}

func (testLeaseProvider) IsOwner(uint32) bool                  { return true }
func (testLeaseProvider) GetFencingToken(uint32) (int64, bool) { return 1, true }

func newMemBatches() *memBatches {
	return &memBatches{
		entries: make(map[string]*domain.InboxEntry),
		runs:    make(map[string]*domain.BatchRun),
	}
}

func (m *memBatches) ListUnbatched(_ context.Context, limit int) ([]*domain.InboxEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*domain.InboxEntry
	for _, e := range m.entries {
		if e.Status == domain.InboxStatusCommitted && e.BatchID == "" {
			result = append(result, e)
			if len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (m *memBatches) ListUnbatchedByIDs(_ context.Context, tenantID, partitionKey, ruleSet string, eventIDs []string) ([]*domain.InboxEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	wanted := make(map[string]struct{}, len(eventIDs))
	for _, id := range eventIDs {
		wanted[id] = struct{}{}
	}
	var result []*domain.InboxEntry
	for _, e := range m.entries {
		if e.TenantID != tenantID || e.PartitionKey != partitionKey || e.RuleSet != ruleSet ||
			e.Status != domain.InboxStatusCommitted || e.BatchID != "" {
			continue
		}
		if _, ok := wanted[e.EventID]; ok {
			result = append(result, e)
		}
	}
	return result, nil
}

func (m *memBatches) MarkBatched(_ context.Context, tenantID, eventID, batchID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := tenantID + "|" + eventID
	if e, ok := m.entries[k]; ok {
		e.BatchID = batchID
	}
	return nil
}

func (m *memBatches) CompleteBatch(_ context.Context, run *domain.BatchRun, eventIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if run == nil || len(eventIDs) == 0 || len(eventIDs) != run.MemberCount {
		return errors.New("batch run and exact member IDs are required")
	}
	members := make([]*domain.InboxEntry, 0, len(eventIDs))
	for _, id := range eventIDs {
		key := run.TenantID + "|" + id
		entry := m.entries[key]
		if entry == nil || entry.Status != domain.InboxStatusCommitted {
			return errors.New("batch member missing or not committed")
		}
		if entry.BatchID != "" && entry.BatchID != run.BatchID {
			return errors.New("batch member already assigned to another batch")
		}
		members = append(members, entry)
	}
	copyRun := *run
	m.runs[run.BatchID] = &copyRun
	for _, entry := range members {
		entry.BatchID = run.BatchID
	}
	return nil
}

func (m *memBatches) SaveRun(_ context.Context, run *domain.BatchRun) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.runs[run.BatchID] = run
	return nil
}

func (m *memBatches) GetRun(_ context.Context, batchID string) (*domain.BatchRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.runs[batchID], nil
}

func (m *memBatches) GetRunForTenant(_ context.Context, tenantID, batchID string) (*domain.BatchRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	run := m.runs[batchID]
	if run == nil || run.TenantID != tenantID {
		return nil, nil
	}
	return run, nil
}

func (m *memBatches) ListRuns(_ context.Context, tenantID, ruleSet string, limit, offset int) ([]*domain.BatchRun, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []*domain.BatchRun
	for _, run := range m.runs {
		if run.TenantID != tenantID || (ruleSet != "" && run.RuleSet != ruleSet) {
			continue
		}
		result = append(result, run)
	}
	if offset >= len(result) {
		return []*domain.BatchRun{}, nil
	}
	result = result[offset:]
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

type memEventProcessor struct {
	mu            sync.Mutex
	envelopes     []*domain.EventEnvelope
	executions    map[string]*domain.Execution
	fencingTokens []int64
	shards        []uint32
	workerIDs     []string
}

func newMemEventProcessor() *memEventProcessor {
	return &memEventProcessor{
		executions: make(map[string]*domain.Execution),
	}
}

func (m *memEventProcessor) Process(_ context.Context, envelope *domain.EventEnvelope, fencingToken int64, shard uint32, workerID string) (*domain.Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.envelopes = append(m.envelopes, envelope)
	m.fencingTokens = append(m.fencingTokens, fencingToken)
	m.shards = append(m.shards, shard)
	m.workerIDs = append(m.workerIDs, workerID)
	exec := &domain.Execution{
		ID:           "exec-" + envelope.ID,
		EventID:      envelope.ID,
		TenantID:     envelope.TenantID,
		RuleSet:      envelope.Type,
		Revision:     1,
		DecisionHash: "hash-" + envelope.ID,
		Status:       domain.ExecutionStatusCompleted,
		CreatedAt:    time.Now().UTC(),
	}
	m.executions[exec.ID] = exec
	return exec, nil
}

func (m *memEventProcessor) QuarantineEvent(_ context.Context, _ string, _ string, _ string, _ domain.ErrorClass, _ string) error {
	return nil
}

func TestBatchService_Accumulate(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeAccumulate,
		MaxBatch: 3,
		Window:   0,
	}

	svc := NewServiceWithLeaseProvider(batches, events, clock, cfg, testLeaseProvider{}, "worker-1", 4096)

	ctx := context.Background()
	now := time.Now().UTC()

	for i := 1; i <= 3; i++ {
		id := "evt-" + string(rune('0'+i))
		key := "tenant-1|" + id
		batches.entries[key] = &domain.InboxEntry{
			TenantID:     "tenant-1",
			EventID:      id,
			PartitionKey: "pk-1",
			RuleSet:      "order.created",
			Status:       domain.InboxStatusCommitted,
			FirstSeenAt:  now.Add(time.Duration(i) * time.Second),
			Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
		}
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 1 {
		t.Errorf("expected 1 batch formed, got %d", formed)
	}

	if len(events.envelopes) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(events.envelopes))
	}
	if events.fencingTokens[0] <= 0 {
		t.Fatalf("fencing token = %d, want a positive lease token", events.fencingTokens[0])
	}
	if events.workerIDs[0] != "worker-1" {
		t.Fatalf("worker ID = %q, want worker-1", events.workerIDs[0])
	}
	env := events.envelopes[0]
	if env.Type != "order.created.batch" {
		t.Errorf("expected batch rule set, got %s", env.Type)
	}

	var payload domain.BatchData
	if err := json.Unmarshal(env.Data, &payload); err != nil {
		t.Fatalf("unmarshal batch data: %v", err)
	}
	if len(payload.Events) != 3 {
		t.Errorf("expected 3 batch events, got %d", len(payload.Events))
	}

	expectedBatchID := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-2", "evt-3"})
	if env.ID != expectedBatchID {
		t.Errorf("batch ID mismatch: got %s, want %s", env.ID, expectedBatchID)
	}

	for _, id := range []string{"evt-1", "evt-2", "evt-3"} {
		e := batches.entries["tenant-1|"+id]
		if e.BatchID != expectedBatchID {
			t.Errorf("event %s not marked batched: %s", id, e.BatchID)
		}
	}
}

func TestBatchService_AccumulateBelowThreshold(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeAccumulate,
		MaxBatch: 5,
		Window:   0,
	}

	svc := NewServiceWithLeaseProvider(batches, events, clock, cfg, testLeaseProvider{}, "worker-1", 4096)

	ctx := context.Background()
	now := time.Now().UTC()

	for i := 1; i <= 2; i++ {
		id := "evt-" + string(rune('0'+i))
		batches.entries["tenant-1|"+id] = &domain.InboxEntry{
			TenantID:     "tenant-1",
			EventID:      id,
			PartitionKey: "pk-1",
			RuleSet:      "order.created",
			Status:       domain.InboxStatusCommitted,
			FirstSeenAt:  now.Add(time.Duration(i) * time.Second),
			Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
		}
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 0 {
		t.Errorf("expected 0 batches formed, got %d", formed)
	}
	if len(events.envelopes) != 0 {
		t.Errorf("expected 0 envelopes, got %d", len(events.envelopes))
	}
}

func TestBatchService_Window(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeWindow,
		MaxBatch: 10,
		Window:   5 * time.Second,
	}

	svc := NewServiceWithLeaseProvider(batches, events, clock, cfg, testLeaseProvider{}, "worker-1", 4096)

	ctx := context.Background()
	now := time.Now().UTC()

	id := "evt-1"
	batches.entries["tenant-1|"+id] = &domain.InboxEntry{
		TenantID:     "tenant-1",
		EventID:      id,
		PartitionKey: "pk-1",
		RuleSet:      "order.created",
		Status:       domain.InboxStatusCommitted,
		FirstSeenAt:  now.Add(-10 * time.Second),
		Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 1 {
		t.Errorf("expected 1 batch formed, got %d", formed)
	}
	if len(events.envelopes) != 1 {
		t.Fatalf("expected 1 envelope, got %d", len(events.envelopes))
	}
}

func TestBatchService_WindowNotElapsed(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeWindow,
		MaxBatch: 10,
		Window:   5 * time.Second,
	}

	svc := NewServiceWithLeaseProvider(batches, events, clock, cfg, testLeaseProvider{}, "worker-1", 4096)

	ctx := context.Background()
	now := time.Now().UTC()

	id := "evt-1"
	batches.entries["tenant-1|"+id] = &domain.InboxEntry{
		TenantID:     "tenant-1",
		EventID:      id,
		PartitionKey: "pk-1",
		RuleSet:      "order.created",
		Status:       domain.InboxStatusCommitted,
		FirstSeenAt:  now.Add(-1 * time.Second),
		Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 0 {
		t.Errorf("expected 0 batches formed, got %d", formed)
	}
}

func TestBatchService_NoneMode(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	clock := domain.SystemClock{}

	cfg := domain.BatchConfig{
		Mode:     domain.BatchModeNone,
		MaxBatch: 10,
		Window:   5 * time.Second,
	}

	svc := NewServiceWithLeaseProvider(batches, events, clock, cfg, testLeaseProvider{}, "worker-1", 4096)

	ctx := context.Background()
	now := time.Now().UTC()

	id := "evt-1"
	batches.entries["tenant-1|"+id] = &domain.InboxEntry{
		TenantID:     "tenant-1",
		EventID:      id,
		PartitionKey: "pk-1",
		RuleSet:      "order.created",
		Status:       domain.InboxStatusCommitted,
		FirstSeenAt:  now.Add(-10 * time.Second),
		Payload:      []byte(`{"id": "` + id + `", "total": 100}`),
	}

	formed, err := svc.Tick(ctx)
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if formed != 0 {
		t.Errorf("expected 0 batches formed, got %d", formed)
	}
}

func TestComputeBatchID_Deterministic(t *testing.T) {
	eventIDs := []string{"evt-3", "evt-1", "evt-2"}
	id1 := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", eventIDs)

	eventIDs2 := []string{"evt-1", "evt-2", "evt-3"}
	id2 := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", eventIDs2)

	if id1 != id2 {
		t.Errorf("batch ID not deterministic: %s != %s", id1, id2)
	}
}

func TestComputeBatchID_DifferentPartitions(t *testing.T) {
	eventIDs := []string{"evt-1", "evt-2"}
	id1 := domain.ComputeBatchID("tenant-1", "pk-1", "order.created", eventIDs)
	id2 := domain.ComputeBatchID("tenant-1", "pk-2", "order.created", eventIDs)

	if id1 == id2 {
		t.Error("batch ID should differ for different partition keys")
	}
}

func TestBatchService_ListBatchesScopesTenantAndRuleSet(t *testing.T) {
	batches := newMemBatches()
	batches.runs["batch-a"] = &domain.BatchRun{BatchID: "batch-a", TenantID: "tenant-a", RuleSet: "orders", CreatedAt: time.Now()}
	batches.runs["batch-b"] = &domain.BatchRun{BatchID: "batch-b", TenantID: "tenant-b", RuleSet: "orders", CreatedAt: time.Now()}
	batches.runs["batch-c"] = &domain.BatchRun{BatchID: "batch-c", TenantID: "tenant-a", RuleSet: "payments", CreatedAt: time.Now()}

	svc := NewService(batches, newMemEventProcessor(), domain.SystemClock{}, domain.BatchConfig{Mode: domain.BatchModeNone})
	got, err := svc.ListBatches(context.Background(), "tenant-a", "orders", 10, 0)
	if err != nil {
		t.Fatalf("ListBatches() error = %v", err)
	}
	if len(got) != 1 || got[0].BatchID != "batch-a" {
		t.Fatalf("ListBatches() = %#v, want only tenant-a/orders batch", got)
	}
}

func TestBatchService_GetBatchScopesTenant(t *testing.T) {
	batches := newMemBatches()
	batches.runs["batch-a"] = &domain.BatchRun{BatchID: "batch-a", TenantID: "tenant-a"}

	svc := NewService(batches, newMemEventProcessor(), domain.SystemClock{}, domain.BatchConfig{Mode: domain.BatchModeNone})
	got, err := svc.GetBatch(context.Background(), "tenant-b", "batch-a")
	if err != nil {
		t.Fatalf("GetBatch() error = %v", err)
	}
	if got != nil {
		t.Fatalf("GetBatch() = %#v, want nil for another tenant", got)
	}
}

func TestBatchService_CreateBatchFailsWhenProcessorIsNotConfigured(t *testing.T) {
	svc := NewService(newMemBatches(), nil, domain.SystemClock{}, domain.BatchConfig{Mode: domain.BatchModeNone})
	_, err := svc.CreateBatch(context.Background(), "tenant-a", "partition", "orders", []string{"event-1"})
	if !errors.Is(err, domain.ErrBatchProcessingUnavailable) {
		t.Fatalf("CreateBatch() error = %v, want ErrBatchProcessingUnavailable", err)
	}
}

func TestCreateBatchRequiresEveryRequestedEvent(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	now := time.Now().UTC()
	batches.entries["tenant-1|evt-1"] = &domain.InboxEntry{
		TenantID: "tenant-1", EventID: "evt-1", PartitionKey: "pk-1",
		RuleSet: "order.created", Status: domain.InboxStatusCommitted,
		FirstSeenAt: now, Payload: []byte(`{"id":"evt-1"}`),
	}
	service := NewService(batches, events, domain.SystemClock{}, domain.BatchConfig{MaxBatch: 10})

	_, err := service.CreateBatch(context.Background(), "tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-missing"})
	if err == nil {
		t.Fatal("CreateBatch accepted a partial set of requested events")
	}
	if len(events.envelopes) != 0 {
		t.Fatalf("processed %d batch envelopes before validating membership", len(events.envelopes))
	}
}

func TestCreateBatchRejectsDuplicateEventIDs(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	service := NewService(batches, events, domain.SystemClock{}, domain.BatchConfig{MaxBatch: 10})

	_, err := service.CreateBatch(context.Background(), "tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-1"})
	if err == nil {
		t.Fatal("CreateBatch accepted duplicate event IDs")
	}
	if len(events.envelopes) != 0 {
		t.Fatalf("processed %d batch envelopes for duplicate IDs", len(events.envelopes))
	}
}

func TestCreateBatchRejectsBatchAboveConfiguredMaximum(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	service := NewService(batches, events, domain.SystemClock{}, domain.BatchConfig{MaxBatch: 1})

	_, err := service.CreateBatch(context.Background(), "tenant-1", "pk-1", "order.created", []string{"evt-1", "evt-2"})
	if err == nil {
		t.Fatal("CreateBatch accepted a request larger than MaxBatch")
	}
	if len(events.envelopes) != 0 {
		t.Fatalf("processed %d batch envelopes for an oversized request", len(events.envelopes))
	}
}

func TestCreateBatchRequiresLeaseProvider(t *testing.T) {
	batches := newMemBatches()
	events := newMemEventProcessor()
	now := time.Now().UTC()
	batches.entries["tenant-1|evt-1"] = &domain.InboxEntry{
		TenantID: "tenant-1", EventID: "evt-1", PartitionKey: "pk-1",
		RuleSet: "order.created", Status: domain.InboxStatusCommitted,
		FirstSeenAt: now, Payload: []byte(`{"id":"evt-1"}`),
	}
	service := NewService(batches, events, domain.SystemClock{}, domain.BatchConfig{MaxBatch: 10})

	_, err := service.CreateBatch(context.Background(), "tenant-1", "pk-1", "order.created", []string{"evt-1"})
	if err == nil {
		t.Fatal("CreateBatch processed an event without shard lease context")
	}
	if len(events.envelopes) != 0 {
		t.Fatalf("processed %d batch envelopes without lease context", len(events.envelopes))
	}
}

func TestCompleteBatchRejectsConflictingMembershipWithoutPartialWrites(t *testing.T) {
	batches := newMemBatches()
	now := time.Now().UTC()
	for _, id := range []string{"evt-1", "evt-2"} {
		batches.entries["tenant-1|"+id] = &domain.InboxEntry{
			TenantID: "tenant-1", EventID: id, PartitionKey: "pk-1",
			RuleSet: "orders", Status: domain.InboxStatusCommitted, FirstSeenAt: now,
		}
	}
	batches.entries["tenant-1|evt-2"].BatchID = "another-batch"
	run := &domain.BatchRun{
		BatchID: "batch-1", TenantID: "tenant-1", PartitionKey: "pk-1",
		RuleSet: "orders", Status: domain.BatchRunStatusCompleted, MemberCount: 2,
	}

	if err := batches.CompleteBatch(context.Background(), run, []string{"evt-1", "evt-2"}); err == nil {
		t.Fatal("CompleteBatch accepted a member already assigned to another batch")
	}
	if got := batches.entries["tenant-1|evt-1"].BatchID; got != "" {
		t.Fatalf("first member partially updated to %q after transaction failure", got)
	}
	if batches.runs["batch-1"] != nil {
		t.Fatal("batch run was persisted despite membership conflict")
	}
}

func TestCompleteBatchIsIdempotentForSameBatch(t *testing.T) {
	batches := newMemBatches()
	now := time.Now().UTC()
	for _, id := range []string{"evt-1", "evt-2"} {
		batches.entries["tenant-1|"+id] = &domain.InboxEntry{
			TenantID: "tenant-1", EventID: id, PartitionKey: "pk-1",
			RuleSet: "orders", Status: domain.InboxStatusCommitted, FirstSeenAt: now,
		}
	}
	run := &domain.BatchRun{
		BatchID: "batch-1", TenantID: "tenant-1", PartitionKey: "pk-1",
		RuleSet: "orders", Status: domain.BatchRunStatusCompleted, MemberCount: 2,
	}
	ctx := context.Background()
	if err := batches.CompleteBatch(ctx, run, []string{"evt-1", "evt-2"}); err != nil {
		t.Fatalf("first CompleteBatch() error = %v", err)
	}
	if err := batches.CompleteBatch(ctx, run, []string{"evt-1", "evt-2"}); err != nil {
		t.Fatalf("idempotent CompleteBatch() error = %v", err)
	}
	for _, id := range []string{"evt-1", "evt-2"} {
		if got := batches.entries["tenant-1|"+id].BatchID; got != run.BatchID {
			t.Errorf("member %s batch ID = %q, want %q", id, got, run.BatchID)
		}
	}
}
