// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Querier abstracts the common query interface shared by *pgxpool.Pool and *pgx.Tx.
type Querier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Tx wraps pgx.Tx and implements ports.Tx for the application layer.
type Tx struct {
	tx           pgx.Tx
	ctx          context.Context
	fencingToken int64
}

// NewTx performs the new tx operation.
func NewTx(ctx context.Context, tx pgx.Tx) *Tx {
	return &Tx{tx: tx, ctx: ctx}
}

// Exec performs the exec operation.
func (t *Tx) Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error) {
	return t.tx.Exec(ctx, sql, arguments...)
}

// Query performs the query operation.
func (t *Tx) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.tx.Query(ctx, sql, args...)
}

// QueryRow performs the query row operation.
func (t *Tx) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.tx.QueryRow(ctx, sql, args...)
}

// Commit performs the commit operation.
func (t *Tx) Commit(ctx context.Context) error {
	return t.tx.Commit(ctx)
}

// Rollback performs the rollback operation.
func (t *Tx) Rollback(ctx context.Context) error {
	return t.tx.Rollback(ctx)
}

// FencingToken performs the fencing token operation.
func (t *Tx) FencingToken() int64 {
	return t.fencingToken
}

// SetFencingToken performs the set fencing token operation.
func (t *Tx) SetFencingToken(token int64) {
	t.fencingToken = token
}

// Context performs the context operation.
func (t *Tx) Context() context.Context {
	return t.ctx
}

// Conn performs the conn operation.
func (t *Tx) Conn() pgx.Tx {
	return t.tx
}

// Querier performs the querier operation.
func (t *Tx) Querier() Querier {
	return t
}

// NewTxFromPool performs the new tx from pool operation.
func NewTxFromPool(ctx context.Context, pool *pgxpool.Pool) (*Tx, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return NewTx(ctx, tx), nil
}

// NewTxQuerier performs the new tx querier operation.
func NewTxQuerier(tx *Tx) Querier {
	return tx
}
