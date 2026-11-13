// Package postgres provides PostgreSQL persistence adapters.
package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
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

const migrationLockID = 0x464C525A // "FLRZ"

// RunMigrations serializes startup migrations on a dedicated database session.
func (d *DB) RunMigrations(ctx context.Context) (resultErr error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		conn.Release()
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() { resultErr = errors.Join(resultErr, releaseMigrationLock(ctx, conn)) }()
	// Ensure schema_migrations table exists (for first run before any migrations)
	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     TEXT NOT NULL PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			checksum    TEXT NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}

	// Get already applied migrations
	applied := make(map[string]string) // version -> checksum
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("query applied migrations: %w", err)
	}
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			rows.Close()
			return fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate applied migrations: %w", err)
	}
	rows.Close()

	// Read and sort migration files
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
		// Extract version from filename (e.g., "001_rule_revisions.sql" -> "001")
		version := strings.TrimSuffix(f, ".sql")
		if idx := strings.Index(version, "_"); idx >= 0 {
			version = version[:idx]
		}

		// Read migration content for checksum
		data, err := os.ReadFile(filepath.Join(d.migrations, f)) // #nosec G304
		if err != nil {
			return fmt.Errorf("read migration %s: %w", f, err)
		}
		checksumBytes := sha256.Sum256(data)
		checksum := hex.EncodeToString(checksumBytes[:])

		// Check if already applied
		if existingChecksum, ok := applied[version]; ok {
			if existingChecksum != checksum {
				return fmt.Errorf("migration %s checksum mismatch: applied %s, current %s", version, existingChecksum, checksum)
			}
			continue // Already applied with same content
		}

		// Apply migration in its own transaction
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration transaction %s: %w", f, err)
		}

		if _, err := tx.Exec(ctx, string(data)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("exec migration %s: %w", f, err)
		}

		// Record migration
		name := strings.TrimSuffix(f, ".sql")
		if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`,
			version, name, checksum); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", f, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", f, err)
		}
	}
	return nil
}

// If unlock fails, close the session instead of returning a held lock to the pool.
func releaseMigrationLock(parent context.Context, conn *pgxpool.Conn) error {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer cancel()
	var unlocked bool
	err := conn.QueryRow(ctx, `SELECT pg_advisory_unlock($1)`, migrationLockID).Scan(&unlocked)
	if err != nil || !unlocked {
		closeErr := conn.Hijack().Close(ctx)
		if err != nil {
			return errors.Join(fmt.Errorf("release migration lock: %w", err), closeErr)
		}
		return errors.Join(errors.New("migration lock was not held"), closeErr)
	}
	conn.Release()
	return nil
}
