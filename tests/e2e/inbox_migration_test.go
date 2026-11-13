//go:build e2e

package e2e

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	pgadapter "github.com/flowrulz/flow-rulz/internal/adapters/postgres"
	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestPostgresInboxCommitsOnlyTheMatchingRuleset(t *testing.T) {
	env := StartStack(t)
	repo := pgadapter.NewInboxRepository(env.PostgresPool)
	ctx := context.Background()
	for _, ruleSet := range []string{"fraud", "payment"} {
		entry := &domain.InboxEntry{TenantID: "tenant", EventID: "event", RuleSet: ruleSet, Status: domain.InboxStatusProcessing, FirstSeenAt: time.Now()}
		if inserted, err := repo.Insert(ctx, entry); err != nil || !inserted {
			t.Fatalf("insert %s: inserted=%v error=%v", ruleSet, inserted, err)
		}
	}
	for _, ruleSet := range []string{"fraud", "payment"} {
		if err := repo.MarkCommitted(ctx, "tenant", "event", ruleSet, "exec-"+ruleSet); err != nil {
			t.Fatal(err)
		}
	}
	assertPostgresInboxEntries(t, repo)
}

func assertPostgresInboxEntries(t *testing.T, repo *pgadapter.InboxRepository) {
	t.Helper()
	ctx := context.Background()
	for _, ruleSet := range []string{"fraud", "payment"} {
		entry, err := repo.GetByRuleSet(ctx, "tenant", "event", ruleSet)
		if err != nil || entry == nil || entry.Status != domain.InboxStatusCommitted || entry.ExecutionID != "exec-"+ruleSet {
			t.Fatalf("ruleset %s entry=%#v error=%v", ruleSet, entry, err)
		}
	}
	if err := repo.MarkCommitted(ctx, "tenant", "event", "fraud", "other"); !errors.Is(err, domain.ErrInboxInvalidStatus) {
		t.Fatalf("second commit should fail: %v", err)
	}
}

func TestLegacyInboxMigrationUsesOnlyMatchingExecution(t *testing.T) {
	env := StartStack(t)
	ctx := context.Background()
	_, err := env.PostgresPool.Exec(ctx, `
		INSERT INTO executions (execution_id,event_id,tenant_id,rule_set,revision,decision_hash)
		VALUES ('valid-exec','valid-event','tenant','orders',1,'hash'),
		       ('wrong-exec','other-event','tenant','wrong',1,'hash');
		INSERT INTO inbox (tenant_id,event_id,rule_set,status,execution_id)
		VALUES ('tenant','valid-event','','committed','valid-exec'),
		       ('tenant','unresolved','','committed','wrong-exec')`)
	if err != nil {
		t.Fatal(err)
	}
	applyLegacyInboxMigration(t, env)
	assertLegacyInboxMigration(t, env)
}

func applyLegacyInboxMigration(t *testing.T, env *TestEnv) {
	t.Helper()
	sql, err := os.ReadFile("../../migrations/025_recover_legacy_inbox_rulesets.sql")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := env.PostgresPool.Exec(context.Background(), string(sql)); err != nil {
			t.Fatal(err)
		}
	}
}

func assertLegacyInboxMigration(t *testing.T, env *TestEnv) {
	t.Helper()
	repo := pgadapter.NewInboxRepository(env.PostgresPool)
	for event, ruleSet := range map[string]string{"valid-event": "orders", "unresolved": ""} {
		entry, err := repo.GetByRuleSet(context.Background(), "tenant", event, ruleSet)
		if err != nil || entry == nil {
			t.Fatalf("event %s ruleset %s entry=%#v error=%v", event, ruleSet, entry, err)
		}
	}
}

func TestConcurrentMigrationStartup(t *testing.T) {
	env := StartStack(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "999_lock_probe.sql")
	if err := os.WriteFile(path, []byte("SELECT pg_sleep(0.5); CREATE TABLE migration_lock_probe (id integer PRIMARY KEY);"), 0600); err != nil {
		t.Fatal(err)
	}
	dsn, err := env.PostgresContainer.ConnectionString(context.Background(), "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	assertConcurrentMigrations(t, dsn, dir)
}

func assertConcurrentMigrations(t *testing.T, dsn, dir string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start, results := make(chan struct{}), make(chan error, 2)
	for range 2 {
		go runMigrationAfterSignal(ctx, dsn, dir, start, results)
	}
	close(start)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("concurrent migration failed: %v", err)
		}
	}
}

func runMigrationAfterSignal(ctx context.Context, dsn, dir string, start <-chan struct{}, result chan<- error) {
	db, err := pgadapter.New(ctx, dsn, dir)
	if err != nil {
		result <- err
		return
	}
	defer db.Close()
	<-start
	result <- db.RunMigrations(ctx)
}
