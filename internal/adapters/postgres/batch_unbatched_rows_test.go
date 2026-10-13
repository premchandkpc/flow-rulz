package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
)

type unbatchedRowsErrorStub struct {
	pgx.Rows
	err error
}

func (r unbatchedRowsErrorStub) Next() bool { return false }
func (r unbatchedRowsErrorStub) Err() error { return r.err }
func (r unbatchedRowsErrorStub) Close()     {}

type unbatchedRowsQuerierStub struct {
	Querier
	rows pgx.Rows
}

func (q unbatchedRowsQuerierStub) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return q.rows, nil
}

func TestListUnbatchedPropagatesIterationError(t *testing.T) {
	iterationErr := errors.New("simulated row stream failure")
	repo := NewBatchRepository(unbatchedRowsQuerierStub{
		rows: unbatchedRowsErrorStub{err: iterationErr},
	})

	_, err := repo.ListUnbatched(context.Background(), 10)
	if !errors.Is(err, iterationErr) {
		t.Fatalf("ListUnbatched() error = %v, want wrapped iteration error", err)
	}
}
