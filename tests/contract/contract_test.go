// Package contract provides test fixtures and contract tests for storage implementations.
package contract

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
)

type TestFixtures struct{}

func (f TestFixtures) EventEnvelope() *domain.EventEnvelope {
	return &domain.EventEnvelope{
		ID:           "test-event-001",
		Type:         "order.created",
		TenantID:     "test-tenant",
		PartitionKey: "order-123",
		OccurredAt:   time.Now().UTC(),
		Data:         json.RawMessage(`{"total": 1500, "country": "US", "id": "order-123"}`),
		Headers:      map[string]string{"source": "test"},
	}
}

func (f TestFixtures) RuleSource() json.RawMessage {
	return json.RawMessage(`{
		"rule_set": "order-policy",
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
	}`)
}

func (f TestFixtures) CompileRule(t *testing.T) *domain.RuleRevision {
	t.Helper()
	compiler := rules.NewCompiler(rules.DefaultLimits())
	rev, err := compiler.Compile(f.RuleSource())
	if err != nil {
		t.Fatalf("compile rule: %v", err)
	}
	return rev
}

type InboxTestSuite struct {
	NewRepo func() InboxRepository
}

type InboxRepository interface {
	Insert(ctx context.Context, entry *domain.InboxEntry) (bool, error)
	Get(ctx context.Context, tenantID string, eventID string) (*domain.InboxEntry, error)
	MarkCommitted(ctx context.Context, tenantID string, eventID string, executionID string) error
}

func (s *InboxTestSuite) RunTests(t *testing.T) {
	ctx := context.Background()
	fixtures := TestFixtures{}

	t.Run("Insert and Get", func(t *testing.T) {
		repo := s.NewRepo()
		entry := &domain.InboxEntry{
			TenantID:    "tenant-1",
			EventID:     "event-1",
			Status:      domain.InboxStatusProcessing,
			FirstSeenAt: time.Now().UTC(),
		}

		inserted, err := repo.Insert(ctx, entry)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
		if !inserted {
			t.Fatal("expected inserted=true")
		}

		got, err := repo.Get(ctx, "tenant-1", "event-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got == nil {
			t.Fatal("expected entry")
		}
		if got.Status != domain.InboxStatusProcessing {
			t.Errorf("status: got %s, want %s", got.Status, domain.InboxStatusProcessing)
		}
	})

	t.Run("Duplicate Insert Returns False", func(t *testing.T) {
		repo := s.NewRepo()
		entry := &domain.InboxEntry{
			TenantID:    "tenant-2",
			EventID:     "event-2",
			Status:      domain.InboxStatusProcessing,
			FirstSeenAt: time.Now().UTC(),
		}

		inserted, err := repo.Insert(ctx, entry)
		if err != nil {
			t.Fatalf("first insert: %v", err)
		}
		if !inserted {
			t.Fatal("expected first insert=true")
		}

		inserted, err = repo.Insert(ctx, entry)
		if err != nil {
			t.Fatalf("second insert: %v", err)
		}
		if inserted {
			t.Fatal("expected second insert=false for duplicate")
		}
	})

	t.Run("Mark Committed", func(t *testing.T) {
		repo := s.NewRepo()
		entry := &domain.InboxEntry{
			TenantID:    "tenant-3",
			EventID:     "event-3",
			Status:      domain.InboxStatusProcessing,
			FirstSeenAt: time.Now().UTC(),
		}
		if _, err := repo.Insert(ctx, entry); err != nil {
			t.Fatalf("insert: %v", err)
		}

		err := repo.MarkCommitted(ctx, "tenant-3", "event-3", "exec-1")
		if err != nil {
			t.Fatalf("mark committed: %v", err)
		}

		got, _ := repo.Get(ctx, "tenant-3", "event-3")
		if got == nil {
			t.Fatal("expected entry")
		}
		if got.Status != domain.InboxStatusCommitted {
			t.Errorf("status: got %s, want %s", got.Status, domain.InboxStatusCommitted)
		}
	})
	_ = fixtures
}

type ExecutionTestSuite struct {
	NewRepo func() ExecutionRepository
}

type ExecutionRepository interface {
	Save(ctx context.Context, execution *domain.Execution) error
	Get(ctx context.Context, executionID string) (*domain.Execution, error)
	UpdateStatus(ctx context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error
}

func (s *ExecutionTestSuite) RunTests(t *testing.T) {
	ctx := context.Background()

	t.Run("Save and Get", func(t *testing.T) {
		repo := s.NewRepo()
		exec := &domain.Execution{
			ID:           "exec-1",
			EventID:      "event-1",
			TenantID:     "tenant-1",
			RuleSet:      "order-policy",
			Revision:     1,
			DecisionHash: "abc123",
			Status:       domain.ExecutionStatusPending,
			CreatedAt:    time.Now().UTC(),
		}

		err := repo.Save(ctx, exec)
		if err != nil {
			t.Fatalf("save: %v", err)
		}

		got, err := repo.Get(ctx, "exec-1")
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if got == nil {
			t.Fatal("expected execution")
		}
		if got.DecisionHash != "abc123" {
			t.Errorf("hash: got %s, want abc123", got.DecisionHash)
		}
	})

	t.Run("Update Status", func(t *testing.T) {
		repo := s.NewRepo()
		exec := &domain.Execution{
			ID:           "exec-2",
			EventID:      "event-2",
			TenantID:     "tenant-1",
			RuleSet:      "order-policy",
			Revision:     1,
			DecisionHash: "def456",
			Status:       domain.ExecutionStatusPending,
			CreatedAt:    time.Now().UTC(),
		}
		if err := repo.Save(ctx, exec); err != nil {
			t.Fatalf("save: %v", err)
		}

		err := repo.UpdateStatus(ctx, "exec-2", domain.ExecutionStatusCompleted, "")
		if err != nil {
			t.Fatalf("update status: %v", err)
		}

		got, _ := repo.Get(ctx, "exec-2")
		if got.Status != domain.ExecutionStatusCompleted {
			t.Errorf("status: got %s, want %s", got.Status, domain.ExecutionStatusCompleted)
		}
		if got.CompletedAt == nil {
			t.Error("expected completed_at to be set")
		}
	})
}

type OutboxTestSuite struct {
	NewRepo func() OutboxRepository
}

type OutboxRepository interface {
	Insert(ctx context.Context, effects []domain.OutboxEffect) error
	ClaimPending(ctx context.Context, batchSize int, owner string) ([]domain.OutboxEffect, error)
	MarkDelivered(ctx context.Context, effectID string) error
	ScheduleRetry(ctx context.Context, effectID string, availableAt time.Duration, attempts int, errMsg string) error
	Quarantine(ctx context.Context, effectID string, errMsg string) error
}

func (s *OutboxTestSuite) RunTests(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC()

	t.Run("Insert and Claim", func(t *testing.T) {
		repo := s.NewRepo()
		effects := []domain.OutboxEffect{
			{
				ID:          "effect-1",
				ExecutionID: "exec-1",
				Destination: "orders.review",
				Name:        "review",
				Payload:     json.RawMessage(`{"order_id": "123"}`),
				EffectType:  domain.EffectTypeEmit,
				Status:      domain.OutboxStatusPending,
				Attempts:    0,
				MaxAttempts: 5,
				AvailableAt: now,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
		}

		err := repo.Insert(ctx, effects)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}

		claimed, err := repo.ClaimPending(ctx, 10, "worker-1")
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		if len(claimed) != 1 {
			t.Fatalf("expected 1 claimed, got %d", len(claimed))
		}
		if claimed[0].ID != "effect-1" {
			t.Errorf("effect ID: got %s, want effect-1", claimed[0].ID)
		}
	})

	t.Run("Mark Delivered", func(t *testing.T) {
		repo := s.NewRepo()
		effects := []domain.OutboxEffect{
			{
				ID:          "effect-2",
				ExecutionID: "exec-2",
				Destination: "orders.review",
				Name:        "review",
				Payload:     json.RawMessage(`{}`),
				EffectType:  domain.EffectTypeEmit,
				Status:      domain.OutboxStatusPending,
				Attempts:    0,
				MaxAttempts: 5,
				AvailableAt: now,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
		}
		if err := repo.Insert(ctx, effects); err != nil {
			t.Fatalf("insert: %v", err)
		}

		err := repo.MarkDelivered(ctx, "effect-2")
		if err != nil {
			t.Fatalf("mark delivered: %v", err)
		}

		claimed, _ := repo.ClaimPending(ctx, 10, "worker-1")
		for _, c := range claimed {
			if c.ID == "effect-2" {
				t.Error("effect-2 should not be claimable after delivery")
			}
		}
	})

	t.Run("Schedule Retry", func(t *testing.T) {
		repo := s.NewRepo()
		effects := []domain.OutboxEffect{
			{
				ID:          "effect-3",
				ExecutionID: "exec-3",
				Destination: "orders.review",
				Name:        "review",
				Payload:     json.RawMessage(`{}`),
				EffectType:  domain.EffectTypeEmit,
				Status:      domain.OutboxStatusPending,
				Attempts:    0,
				MaxAttempts: 5,
				AvailableAt: now,
				CreatedAt:   now,
				UpdatedAt:   now,
			},
		}
		if err := repo.Insert(ctx, effects); err != nil {
			t.Fatalf("insert: %v", err)
		}

		err := repo.ScheduleRetry(ctx, "effect-3", 5*time.Second, 1, "temporary error")
		if err != nil {
			t.Fatalf("schedule retry: %v", err)
		}
	})
}
