package contract

import (
	"context"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

type memInbox struct {
	mu      sync.Mutex
	entries map[string]*domain.InboxEntry
}

func newMemInbox() *memInbox {
	return &memInbox{entries: make(map[string]*domain.InboxEntry)}
}

func (m *memInbox) key(tenantID, eventID string) string {
	return tenantID + ":" + eventID
}

func (m *memInbox) Insert(_ context.Context, entry *domain.InboxEntry) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(entry.TenantID, entry.EventID)
	if _, exists := m.entries[k]; exists {
		return false, nil
	}
	cp := *entry
	m.entries[k] = &cp
	return true, nil
}

func (m *memInbox) Get(_ context.Context, tenantID string, eventID string) (*domain.InboxEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[m.key(tenantID, eventID)]
	if !ok {
		return nil, nil
	}
	cp := *e
	return &cp, nil
}

func (m *memInbox) MarkCommitted(_ context.Context, tenantID string, eventID string, executionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := m.key(tenantID, eventID)
	e, ok := m.entries[k]
	if !ok {
		return nil
	}
	now := time.Now().UTC()
	e.Status = domain.InboxStatusCommitted
	e.ExecutionID = executionID
	e.CommittedAt = &now
	return nil
}

type memExecution struct {
	mu      sync.Mutex
	entries map[string]*domain.Execution
}

func newMemExecution() *memExecution {
	return &memExecution{entries: make(map[string]*domain.Execution)}
}

func (m *memExecution) Save(_ context.Context, execution *domain.Execution) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *execution
	m.entries[execution.ID] = &cp
	return nil
}

func (m *memExecution) Get(_ context.Context, executionID string) (*domain.Execution, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[executionID]
	if !ok {
		return nil, nil
	}
	cp := *e
	return &cp, nil
}

func (m *memExecution) UpdateStatus(_ context.Context, executionID string, status domain.ExecutionStatus, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[executionID]
	if !ok {
		return nil
	}
	now := time.Now().UTC()
	e.Status = status
	e.Error = errMsg
	e.CompletedAt = &now
	return nil
}

type memOutbox struct {
	mu      sync.Mutex
	effects map[string]*domain.OutboxEffect
}

func newMemOutbox() *memOutbox {
	return &memOutbox{effects: make(map[string]*domain.OutboxEffect)}
}

func (m *memOutbox) Insert(_ context.Context, effects []domain.OutboxEffect) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range effects {
		cp := effects[i]
		m.effects[effects[i].ID] = &cp
	}
	return nil
}

func (m *memOutbox) ClaimPending(_ context.Context, batchSize int, _ string) ([]domain.OutboxEffect, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var claimed []domain.OutboxEffect
	for _, e := range m.effects {
		if e.Status == domain.OutboxStatusPending && time.Now().After(e.AvailableAt) {
			e.Status = domain.OutboxStatusClaimed
			claimed = append(claimed, *e)
			if len(claimed) >= batchSize {
				break
			}
		}
	}
	return claimed, nil
}

func (m *memOutbox) MarkDelivered(_ context.Context, effectID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.effects[effectID]; ok {
		e.Status = domain.OutboxStatusDelivered
	}
	return nil
}

func (m *memOutbox) ScheduleRetry(_ context.Context, effectID string, availableAt time.Duration, attempts int, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.effects[effectID]; ok {
		e.Status = domain.OutboxStatusPending
		e.Attempts = attempts
		e.AvailableAt = time.Now().UTC().Add(availableAt)
		e.LastError = errMsg
	}
	return nil
}

func (m *memOutbox) Quarantine(_ context.Context, effectID string, errMsg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.effects[effectID]; ok {
		e.Status = domain.OutboxStatusQuarantined
		e.LastError = errMsg
	}
	return nil
}
