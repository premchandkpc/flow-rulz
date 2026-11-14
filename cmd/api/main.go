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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	return runAPI(ctx, cfg)
}

func runAPI(ctx context.Context, cfg *config.Config) error {
	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.Initialize(ctx); err != nil {
		return err
	}
	router := newAPIRouter(cfg, db)
	log.Printf("API server listening on %s", cfg.App.HTTPAddr)
	return router.Run(ctx, cfg.App.HTTPAddr)
}

func newAPIRulesService(repositories storage.Repositories) *svcrules.Service {
	compiler := rules.NewCompiler(rules.DefaultLimits())
	return svcrules.NewService(compiler, repositories.Rules, repositories.Activations,
		repositories.Executions, domain.SystemClock{}, repositories.Transactions,
		repositories.NewRuleTxRepositories).WithContractRegistry(repositories.Contracts)
}

func newAPIRouter(cfg *config.Config, db *storage.Storage) *httpapi.Router {
	repositories := db.Repositories()
	rulesSvc := newAPIRulesService(repositories)
	workflowSvc := svcworkflow.NewService(repositories.Workflows, domain.SystemClock{})
	// The API exposes batch status only; event processing is worker-owned.
	batchSvc := svcbatches.NewService(repositories.Batches, nil, domain.SystemClock{}, domain.BatchConfig{Mode: domain.BatchModeNone})
	routerCfg := apiRouterConfig(cfg, repositories, rulesSvc)
	routerCfg.ReadinessChecks = map[string]func(context.Context) error{"database": db.Ping}
	return httpapi.NewRouterWithConfig(rulesSvc, workflowSvc, repositories.WorkflowDefinitions,
		repositories.Contracts, batchSvc, svcquarantine.NewService(repositories.Quarantine), routerCfg)
}

func apiRouterConfig(cfg *config.Config, repositories storage.Repositories, rulesSvc *svcrules.Service) httpapi.RouterConfig {
	return httpapi.RouterConfig{
		AuthValidator:  apiAuthValidator(cfg.Auth),
		RequireAuth:    cfg.Auth.RequireAuth,
		RequiredScopes: cfg.Auth.RequiredScopes,
		NumShards:      cfg.Worker.NumShards,
		InboxRepo:      repositories.Inbox,
		ExecutionRepo:  repositories.Executions,
		OutboxRepo:     repositories.Outbox,
		DomainServices: svcdomains.NewService(repositories.DomainServices, domain.SystemClock{}).WithRuleService(rulesSvc),
	}
}

func apiAuthValidator(cfg config.AuthConfig) *auth.Validator {
	if cfg.JWTSecret == "" {
		return nil
	}
	return auth.NewValidator([]byte(cfg.JWTSecret), cfg.JWTIssuer, cfg.JWTAudience)
}
