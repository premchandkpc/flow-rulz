// Package storage provides storage backend abstraction for FlowRulz.
// It manages database connections, repositories, and transactions.
package storage

import (
	"context"
	"fmt"
	"strings"

	mongoadapter "github.com/flowrulz/flow-rulz/internal/adapters/mongo"
	postgresadapter "github.com/flowrulz/flow-rulz/internal/adapters/postgres"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Repositories holds all repository implementations.
type Repositories struct {
	Inbox                  ports.InboxRepository
	Activations            ports.ActivationRepository
	Rules                  ports.RuleRepository
	Executions             ports.ExecutionRepository
	Outbox                 ports.OutboxRepository
	ShardLeases            ports.ShardLeaseRepository
	Workflows              ports.WorkflowRepository
	WorkflowDefinitions    ports.WorkflowDefinitionRepository
	ScheduledEvents        ports.ScheduledEventRepository
	Batches                ports.BatchRepository
	Quarantine             ports.QuarantineRepository
	Contracts              ports.ContractRegistry
	DomainServices         ports.DomainServiceRepository
	Transactions           ports.TransactionManager
	NewEventTxRepositories ports.EventTxRepositoryFactory
	NewRuleTxRepositories  ports.RuleTxRepositoryFactory
}

// Storage manages database connections and repositories.
type Storage struct {
	backend      string
	repositories Repositories
	initialize   func(context.Context) error
	close        func()
	ping         func(context.Context) error
}

// Backend returns the storage backend name
func (s *Storage) Backend() string {
	return s.backend
}

// Repositories returns the repository instances
func (s *Storage) Repositories() Repositories {
	return s.repositories
}

// Initialize initializes the storage (runs migrations, etc.)
func (s *Storage) Initialize(ctx context.Context) error {
	return s.initialize(ctx)
}

// Ping verifies the backing database is reachable.
func (s *Storage) Ping(ctx context.Context) error {
	return s.ping(ctx)
}

// Close closes the storage connection
func (s *Storage) Close() {
	s.close()
}

// Open creates a new Storage instance with the configured backend.
func Open(ctx context.Context, cfg config.DatabaseConfig) (*Storage, error) {
	backend := strings.ToLower(strings.TrimSpace(cfg.StorageBackend))
	if backend == "" {
		backend = "postgres"
	}

	switch backend {
	case "postgres":
		db, err := postgresadapter.New(ctx, cfg.DSN, cfg.MigrationsDir)
		if err != nil {
			return nil, fmt.Errorf("open postgres storage: %w", err)
		}
		pool := db.Pool()
		return &Storage{
			backend:      backend,
			repositories: buildPostgresRepos(pool),
			initialize:   db.RunMigrations,
			close:        db.Close,
			ping:         pool.Ping,
		}, nil

	case "mongodb":
		db, err := mongoadapter.New(ctx, mongoadapter.Config{
			URI:      cfg.MongoDBURI,
			Database: cfg.MongoDBDatabase,
		})
		if err != nil {
			return nil, fmt.Errorf("open mongodb storage: %w", err)
		}
		q := mongoadapter.NewQuerier(db.Database())
		repositories, initialize := buildMongoRepos(q, db)
		return &Storage{
			backend:      backend,
			repositories: repositories,
			initialize:   initialize,
			close:        db.Close,
			ping:         func(ctx context.Context) error { return db.Client().Ping(ctx, nil) },
		}, nil

	default:
		return nil, fmt.Errorf("unsupported storage backend %q", backend)
	}
}

func buildPostgresRepos(pool *pgxpool.Pool) Repositories {
	return Repositories{
		Inbox:                  postgresadapter.NewInboxRepository(pool),
		Activations:            postgresadapter.NewActivationRepository(pool),
		Rules:                  postgresadapter.NewRuleRepository(pool),
		Executions:             postgresadapter.NewExecutionRepository(pool),
		Outbox:                 postgresadapter.NewOutboxRepository(pool),
		ShardLeases:            postgresadapter.NewShardLeaseRepository(pool),
		Workflows:              postgresadapter.NewWorkflowRepository(pool),
		WorkflowDefinitions:    postgresadapter.NewWorkflowDefinitionRepository(pool),
		ScheduledEvents:        postgresadapter.NewScheduledEventRepository(pool),
		Batches:                postgresadapter.NewBatchRepository(pool),
		Quarantine:             postgresadapter.NewQuarantineRepository(pool),
		Contracts:              postgresadapter.NewContractRegistry(pool),
		DomainServices:         postgresadapter.NewDomainServiceRepository(pool),
		Transactions:           postgresadapter.NewTransactionManager(pool),
		NewEventTxRepositories: postgresadapter.NewEventTxRepositories,
		NewRuleTxRepositories:  postgresadapter.NewRuleTxRepositories,
	}
}

func buildMongoRepos(q mongoadapter.Querier, db *mongoadapter.DB) (Repositories, func(context.Context) error) {
	quarantineRepo := mongoadapter.NewQuarantineRepository(q)
	return Repositories{
		Inbox:                  mongoadapter.NewInboxRepository(q),
		Activations:            mongoadapter.NewActivationRepository(q),
		Rules:                  mongoadapter.NewRuleRepository(q),
		Executions:             mongoadapter.NewExecutionRepository(q),
		Outbox:                 mongoadapter.NewOutboxRepository(q),
		ShardLeases:            mongoadapter.NewShardLeaseRepository(q),
		Workflows:              mongoadapter.NewWorkflowRepository(q),
		WorkflowDefinitions:    mongoadapter.NewWorkflowDefinitionRepository(q),
		ScheduledEvents:        mongoadapter.NewScheduledEventRepository(q),
		Batches:                mongoadapter.NewBatchRepository(q),
		Quarantine:             quarantineRepo,
		Contracts:              mongoadapter.NewContractRegistry(q),
		DomainServices:         mongoadapter.NewDomainServiceRepository(q),
		Transactions:           mongoadapter.NewTransactionManager(db.Client(), db.Database()),
		NewEventTxRepositories: mongoadapter.NewEventTxRepositories,
		NewRuleTxRepositories:  mongoadapter.NewRuleTxRepositories,
	}, quarantineRepo.BackfillEffectTenantIDs
}
