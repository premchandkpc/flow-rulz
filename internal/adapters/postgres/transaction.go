// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TransactionManager is the PostgreSQL persistence adapter.
type TransactionManager struct {
	pool *pgxpool.Pool
}

// NewTransactionManager performs its documented repository operation.
func NewTransactionManager(pool *pgxpool.Pool) *TransactionManager {
	return &TransactionManager{pool: pool}
}

// Begin performs its documented repository operation.
func (m *TransactionManager) Begin(ctx context.Context) (ports.Transaction, error) {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return NewTx(ctx, tx), nil
}

// WithTransaction performs its documented repository operation.
func (m *TransactionManager) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := m.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := fn(ctx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
