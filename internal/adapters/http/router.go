package http

import (
	"context"
	"net/http"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/handlers"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	"github.com/flowrulz/flow-rulz/internal/auth"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// RouterConfig holds authentication settings for the HTTP API.
type RouterConfig struct {
	AuthValidator  *auth.Validator
	RequireAuth    bool
	RequiredScopes []string
	InboxRepo      ports.InboxRepository
	ExecutionRepo  ports.ExecutionRepository
	OutboxRepo     ports.OutboxRepository
	DomainServices ports.DomainServiceUseCase
	// ReadinessChecks are named dependency probes served by GET /ready.
	ReadinessChecks map[string]func(context.Context) error
}

// Router holds all HTTP handlers and middleware
type Router struct {
	handler http.Handler
}

// NewRouter creates a management API router without event publication configured.
func NewRouter(
	ruleSvc ports.RuleService,
	workflowSvc ports.WorkflowService,
	workflowDefRepo ports.WorkflowDefinitionRepository,
	contractRegistry ports.ContractRegistry,
	batchSvc ports.BatchService,
	quarantineSvc ports.QuarantineService,
	scheduledRepo ports.ScheduledEventRepository,
) *Router {
	return NewRouterWithPublisher(ruleSvc, workflowSvc, workflowDefRepo, contractRegistry, batchSvc, quarantineSvc, scheduledRepo, nil, config.DefaultNumShards, RouterConfig{})
}

// NewRouterWithPublisher also registers the SDK-compatible event ingestion endpoint.
func NewRouterWithPublisher(
	ruleSvc ports.RuleService,
	workflowSvc ports.WorkflowService,
	workflowDefRepo ports.WorkflowDefinitionRepository,
	contractRegistry ports.ContractRegistry,
	batchSvc ports.BatchService,
	quarantineSvc ports.QuarantineService,
	scheduledRepo ports.ScheduledEventRepository,
	publisher ports.BrokerPublisher,
	numShards uint32,
	config RouterConfig,
) *Router {
	mux := http.NewServeMux()

	// Apply middleware chain. Required authentication must fail closed even if
	// the router is constructed incorrectly and no validator was supplied.
	handler := middleware.Recovery(mux)
	handler = middleware.TenantExtractor(handler)
	if config.RequireAuth {
		if config.AuthValidator == nil {
			handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Keep the liveness endpoint available during misconfiguration;
				// all application routes remain unavailable rather than public.
				if r.URL.Path == "/health" {
					mux.ServeHTTP(w, r)
					return
				}
				responses.WriteError(w, http.StatusServiceUnavailable, "required authentication is not configured", responses.CodeInternalError)
			})
		} else {
			handler = auth.Middleware(config.AuthValidator, config.RequiredScopes...)(handler)
		}
	} else if config.AuthValidator != nil {
		handler = auth.OptionalAuthMiddleware(config.AuthValidator)(handler)
	}

	// Health endpoint
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		responses.WriteOK(w, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /ready", readinessHandler(config.ReadinessChecks))

	// Rule handlers
	ruleHandler := handlers.NewRuleHandler(ruleSvc)
	ruleHandler.RegisterRoutes(mux)

	// Workflow handlers
	workflowHandler := handlers.NewWorkflowHandler(workflowSvc, nil, workflowDefRepo)
	workflowHandler.RegisterRoutes(mux)

	// Contract handlers
	contractHandler := handlers.NewContractHandler(contractRegistry)
	contractHandler.RegisterRoutes(mux)

	// Batch handlers
	batchHandler := handlers.NewBatchHandler(batchSvc)
	batchHandler.RegisterRoutes(mux)

	// Quarantine handlers
	quarantineHandler := handlers.NewQuarantineHandler(quarantineSvc)
	quarantineHandler.RegisterRoutes(mux)

	// Scheduled event handlers
	scheduledHandler := handlers.NewScheduledHandler(scheduledRepo)
	scheduledHandler.RegisterRoutes(mux)
	domainServicesHandler := handlers.NewDomainServiceHandler(config.DomainServices)
	domainServicesHandler.RegisterRoutes(mux)

	// Event ingestion used by the Go SDK's HTTP transport.
	eventHandler := handlers.NewEventIngestionHandler(publisher, numShards).WithContractRegistry(contractRegistry)
	eventHandler.RegisterRoutes(mux)
	if config.InboxRepo != nil && config.ExecutionRepo != nil && config.OutboxRepo != nil {
		traceHandler := handlers.NewEventTraceHandler(config.InboxRepo, config.ExecutionRepo, config.OutboxRepo, numShards)
		traceHandler.RegisterRoutes(mux)
	}

	return &Router{handler: handler}
}

// ServeHTTP implements http.Handler
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.handler.ServeHTTP(w, req)
}

// Run starts the HTTP server
func (r *Router) Run(ctx context.Context, addr string) error {
	server := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in a goroutine
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	// Wait for context cancellation or server error
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErr:
		return err
	}
}
