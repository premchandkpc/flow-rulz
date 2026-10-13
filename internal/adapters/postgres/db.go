// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DB defines the db operation.
type DB struct {
	pool       *pgxpool.Pool
	migrations string
}

// New performs the new operation.
func New(ctx context.Context, dsn string, migrationsDir string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}

	return &DB{pool: pool, migrations: migrationsDir}, nil
}

// Close performs the close operation.
func (d *DB) Close() {
	d.pool.Close()
}

// Pool performs the pool operation.
func (d *DB) Pool() *pgxpool.Pool {
	return d.pool
}

// RunMigrations performs the run migrations operation.
func (d *DB) RunMigrations(ctx context.Context) error {
	entries, err := os.ReadDir(d.migrations)
	if err != nil {
		return fmt.Errorf("read migrations dir: %w", err)
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)

	for _, f := range files {
		data, err := os.ReadFile(filepath.Join(d.migrations, f)) // #nosec G304 -- f is enumerated from the migration directory and filtered to .sql files.
		if err != nil {
			return fmt.Errorf("read migration %s: %w", f, err)
		}
		if _, err := d.pool.Exec(ctx, string(data)); err != nil {
			return fmt.Errorf("exec migration %s: %w", f, err)
		}
	}
	return nil
}
