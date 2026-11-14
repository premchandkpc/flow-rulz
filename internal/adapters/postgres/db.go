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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DB defines the db operation.
type DB struct {
	pool       *pgxpool.Pool
	migrations string
}

// New performs the new operation.
func New(ctx context.Context, dsn string, migrationsDir string) (*DB, error) {
	cfg, err := poolConfig(dsn)
	if err != nil {
		return nil, err
	}
	pool, err := connectPool(ctx, cfg)
	if err != nil {
		return nil, err
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
	conn, err := acquireMigrationLock(ctx, d.pool)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, releaseMigrationLock(ctx, conn)) }()
	applied, err := loadAppliedMigrations(ctx, conn)
	if err != nil {
		return err
	}
	return d.applyPendingMigrations(ctx, conn, applied)
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

const createMigrationTableSQL = `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version     TEXT NOT NULL PRIMARY KEY,
			name        TEXT NOT NULL,
			applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			checksum    TEXT NOT NULL
		)
	`

func poolConfig(dsn string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 20
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	return cfg, nil
}

func connectPool(ctx context.Context, cfg *pgxpool.Config) (*pgxpool.Pool, error) {
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return pool, nil
}

func acquireMigrationLock(ctx context.Context, pool *pgxpool.Pool) (*pgxpool.Conn, error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire migration connection: %w", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationLockID); err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	return conn, nil
}

func loadAppliedMigrations(ctx context.Context, conn *pgxpool.Conn) (map[string]string, error) {
	if _, err := conn.Exec(ctx, createMigrationTableSQL); err != nil {
		return nil, fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	rows, err := conn.Query(ctx, `SELECT version, checksum FROM schema_migrations`)
	if err != nil {
		return nil, fmt.Errorf("query applied migrations: %w", err)
	}
	defer rows.Close()
	return scanAppliedMigrations(rows)
}

func scanAppliedMigrations(rows pgx.Rows) (map[string]string, error) {
	applied := make(map[string]string)
	for rows.Next() {
		var version, checksum string
		if err := rows.Scan(&version, &checksum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		applied[version] = checksum
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return applied, nil
}

func migrationFiles(directory string) ([]string, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, fmt.Errorf("read migrations dir: %w", err)
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			files = append(files, entry.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

func (d *DB) applyPendingMigrations(ctx context.Context, conn *pgxpool.Conn, applied map[string]string) error {
	files, err := migrationFiles(d.migrations)
	if err != nil {
		return err
	}
	for _, file := range files {
		if err := d.applyMigration(ctx, conn, applied, file); err != nil {
			return err
		}
	}
	return nil
}

type migrationFile struct {
	filename, version, name, checksum string
	data                              []byte
}

func readMigration(directory, filename string) (migrationFile, error) {
	data, err := os.ReadFile(filepath.Join(directory, filename)) // #nosec G304
	if err != nil {
		return migrationFile{}, fmt.Errorf("read migration %s: %w", filename, err)
	}
	checksum := sha256.Sum256(data)
	name := strings.TrimSuffix(filename, ".sql")
	version, _, _ := strings.Cut(name, "_")
	return migrationFile{filename: filename, version: version, name: name,
		checksum: hex.EncodeToString(checksum[:]), data: data}, nil
}

func (m migrationFile) applyUnlessRecorded(ctx context.Context, conn *pgxpool.Conn, applied map[string]string) error {
	if checksum, ok := applied[m.version]; ok {
		if checksum != m.checksum {
			return fmt.Errorf("migration %s checksum mismatch: applied %s, current %s", m.version, checksum, m.checksum)
		}
		return nil
	}
	return m.apply(ctx, conn)
}

func (m migrationFile) apply(ctx context.Context, conn *pgxpool.Conn) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration transaction %s: %w", m.filename, err)
	}
	if err := m.executeAndRecord(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", m.filename, err)
	}
	return nil
}

func (m migrationFile) executeAndRecord(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, string(m.data)); err != nil {
		return fmt.Errorf("exec migration %s: %w", m.filename, err)
	}
	_, err := tx.Exec(ctx, `INSERT INTO schema_migrations (version, name, checksum) VALUES ($1, $2, $3)`, m.version, m.name, m.checksum)
	if err != nil {
		return fmt.Errorf("record migration %s: %w", m.filename, err)
	}
	return nil
}

func (d *DB) applyMigration(ctx context.Context, conn *pgxpool.Conn, applied map[string]string, file string) error {
	migration, err := readMigration(d.migrations, file)
	if err != nil {
		return err
	}
	return migration.applyUnlessRecorded(ctx, conn, applied)
}
