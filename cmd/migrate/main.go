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
	if err := validateCommand(os.Args[1:]); err != nil {
		return err
	}
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return applyMigrations(context.Background(), cfg.Database)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: migrate up")
	fmt.Fprintln(os.Stderr, "  up    Apply pending migrations")
}

func validateCommand(args []string) error {
	if len(args) != 1 {
		printUsage()
		return errors.New("expected exactly one command")
	}
	if args[0] != "up" {
		printUsage()
		return fmt.Errorf("unsupported command %q; only up is implemented", args[0])
	}
	return nil
}

func applyMigrations(ctx context.Context, cfg config.DatabaseConfig) error {
	db, err := postgresadapter.New(ctx, cfg.DSN, cfg.MigrationsDir)
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
