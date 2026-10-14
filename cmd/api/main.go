// Package main starts the FlowRulz HTTP API service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/flowrulz/flow-rulz/internal/adapters/http"
	natsadapter "github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/auth"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
	svcbatches "github.com/flowrulz/flow-rulz/internal/services/batches"
	svcdomains "github.com/flowrulz/flow-rulz/internal/services/domainservices"
	svcquarantine "github.com/flowrulz/flow-rulz/internal/services/quarantine"
	svcrules "github.com/flowrulz/flow-rulz/internal/services/rules"
	svcworkflow "github.com/flowrulz/flow-rulz/internal/services/workflow"
	"github.com/flowrulz/flow-rulz/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Printf("api stopped: %v", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	ctx, stop := signal.NotifyContext(
		context.Background(),
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	defer stop()

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Initialize(ctx); err != nil {
		return err
	}

	natsCfg := natsadapter.Config{
		NatsURL:    cfg.NATS.URL,
		Stream:     cfg.NATS.Stream,
		Consumer:   cfg.NATS.Consumer,
		Subjects:   []string{"events"},
		RuleSets:   cfg.NATS.RuleSets,
		AckWait:    cfg.NATSAckWait(),
		MaxDeliver: cfg.Worker.NATSMaxDeliver,
		NumShards:  cfg.Worker.NumShards,
	}
	publisher, err := natsadapter.NewPublisher(natsCfg)
	if err != nil {
		return fmt.Errorf("initialize NATS event publisher: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close NATS event publisher: %w", err))
		}
	}()

	compiler := rules.NewCompiler(rules.DefaultLimits())
	repositories := db.Repositories()

	rulesSvc := svcrules.NewService(
		compiler,
		repositories.Rules,
		repositories.Activations,
		repositories.Executions,
		domain.SystemClock{},
		repositories.Transactions,
		repositories.NewRuleTxRepositories,
	)

	workflowSvc := svcworkflow.NewService(
		repositories.Workflows,
		domain.SystemClock{},
	)

	batchSvc := svcbatches.NewService(
		repositories.Batches,
		nil, // events processor - not needed for query API
		domain.SystemClock{},
		domain.BatchConfig{Mode: domain.BatchModeNone},
	)

	quarantineSvc := svcquarantine.NewService(repositories.Quarantine)
	domainServicesSvc := svcdomains.NewService(
		repositories.DomainServices,
		domain.SystemClock{},
	)

	workflowDefRepo := repositories.WorkflowDefinitions

	// Configure first-party JWT validation when a secret is supplied.
	var authValidator *auth.Validator
	if cfg.Auth.JWTSecret != "" {
		authValidator = auth.NewValidator([]byte(cfg.Auth.JWTSecret), cfg.Auth.JWTIssuer, cfg.Auth.JWTAudience)
	}

	router := http.NewRouterWithPublisher(
		rulesSvc,
		workflowSvc,
		workflowDefRepo,
		repositories.Contracts,
		batchSvc,
		quarantineSvc,
		repositories.ScheduledEvents,
		publisher,
		cfg.Worker.NumShards,
		http.RouterConfig{
			AuthValidator: authValidator, RequireAuth: cfg.Auth.RequireAuth, RequiredScopes: cfg.Auth.RequiredScopes,
			InboxRepo: repositories.Inbox, ExecutionRepo: repositories.Executions, OutboxRepo: repositories.Outbox,
			DomainServices:  domainServicesSvc,
			ReadinessChecks: map[string]func(context.Context) error{"database": db.Ping, "nats": publisher.Ping},
		},
	)

	log.Printf("API server listening on %s", cfg.App.HTTPAddr)
	return router.Run(ctx, cfg.App.HTTPAddr)
}
