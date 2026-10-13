// cmd/migrate is the database migration CLI for FlowRulz.
// It applies pending PostgreSQL migrations from the migrations directory.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	postgresadapter "github.com/flowrulz/flow-rulz/internal/adapters/postgres"
	"github.com/flowrulz/flow-rulz/internal/config"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		os.Exit(1)
	}
}

// run owns resources so errors return through deferred cleanup before main exits.
func run() error {
	if len(os.Args) != 2 {
		printUsage()
		return errors.New("expected exactly one command")
	}

	command := os.Args[1]
	if command != "up" {
		printUsage()
		return fmt.Errorf("unsupported command %q; only up is implemented", command)
	}

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx := context.Background()
	db, err := postgresadapter.New(ctx, cfg.Database.DSN, cfg.Database.MigrationsDir)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if err := db.RunMigrations(ctx); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}
	_, err = fmt.Fprintln(os.Stdout, "Migrations applied successfully")
	return err
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: migrate up")
	fmt.Fprintln(os.Stderr, "  up    Apply pending migrations")
}
