// Package quarantine manages failed events and effects that require manual review.
package quarantine

import (
	"context"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// Service handles quarantine operations
type Service struct {
	repo ports.QuarantineRepository
}

// NewService constructs a quarantine service.
func NewService(repo ports.QuarantineRepository) *Service {
	return &Service{repo: repo}
}

// Save records a new quarantine entry
func (s *Service) Save(ctx context.Context, entry *domain.QuarantineEntry) error {
	if entry.ID == "" {
		entry.ID = domain.NewID()
	}
	return s.repo.Save(ctx, entry)
}

// Get retrieves a quarantine entry by ID
func (s *Service) Get(ctx context.Context, tenantID, id string) (*domain.QuarantineEntry, error) {
	if tenantID == "" {
		return nil, domain.ErrQuarantineNotFound
	}
	return s.repo.Get(ctx, tenantID, id)
}

// List returns quarantine entries matching the filter
func (s *Service) List(ctx context.Context, filter ports.QuarantineFilter) ([]*domain.QuarantineEntry, error) {
	if filter.TenantID == "" {
		return nil, fmt.Errorf("tenant_id is required")
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	return s.repo.List(ctx, filter)
}

// Replay marks a quarantine entry as replayed (for audit trail)
func (s *Service) Replay(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	return s.repo.Replay(ctx, tenantID, id)
}

// Delete removes a quarantine entry
func (s *Service) Delete(ctx context.Context, tenantID, id string) error {
	if tenantID == "" {
		return domain.ErrQuarantineNotFound
	}
	return s.repo.Delete(ctx, tenantID, id)
}

// ReplayAndDelete replays and then deletes (for cleanup after successful replay)
func (s *Service) ReplayAndDelete(ctx context.Context, tenantID, id string) error {
	if err := s.Replay(ctx, tenantID, id); err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	return s.Delete(ctx, tenantID, id)
}
