// Package main starts the FlowRulz control-plane HTTP API service.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	httpapi "github.com/flowrulz/flow-rulz/internal/adapters/http"
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

func run() error {
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

	repositories := db.Repositories()
	compiler := rules.NewCompiler(rules.DefaultLimits())
	rulesSvc := svcrules.NewService(
		compiler,
		repositories.Rules,
		repositories.Activations,
		repositories.Executions,
		domain.SystemClock{},
		repositories.Transactions,
		repositories.NewRuleTxRepositories,
	).WithContractRegistry(repositories.Contracts)

	workflowSvc := svcworkflow.NewService(repositories.Workflows, domain.SystemClock{})
	batchSvc := svcbatches.NewService(
		repositories.Batches,
		nil, // The API exposes batch status only; event processing is worker-owned.
		domain.SystemClock{},
		domain.BatchConfig{Mode: domain.BatchModeNone},
	)
	quarantineSvc := svcquarantine.NewService(repositories.Quarantine)
	domainServicesSvc := svcdomains.NewService(repositories.DomainServices, domain.SystemClock{}).WithRuleService(rulesSvc)

	var authValidator *auth.Validator
	if cfg.Auth.JWTSecret != "" {
		authValidator = auth.NewValidator([]byte(cfg.Auth.JWTSecret), cfg.Auth.JWTIssuer, cfg.Auth.JWTAudience)
	}

	router := httpapi.NewRouterWithConfig(
		rulesSvc,
		workflowSvc,
		repositories.WorkflowDefinitions,
		repositories.Contracts,
		batchSvc,
		quarantineSvc,
		httpapi.RouterConfig{
			AuthValidator:  authValidator,
			RequireAuth:    cfg.Auth.RequireAuth,
			RequiredScopes: cfg.Auth.RequiredScopes,
			NumShards:      cfg.Worker.NumShards,
			InboxRepo:      repositories.Inbox,
			ExecutionRepo:  repositories.Executions,
			OutboxRepo:     repositories.Outbox,
			DomainServices: domainServicesSvc,
			ReadinessChecks: map[string]func(context.Context) error{
				"database": db.Ping,
			},
		},
	)

	log.Printf("API server listening on %s", cfg.App.HTTPAddr)
	return router.Run(ctx, cfg.App.HTTPAddr)
}
