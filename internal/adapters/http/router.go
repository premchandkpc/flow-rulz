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

// RouterConfig contains management API security, query and readiness dependencies.
// It intentionally has no broker publisher: event ingestion belongs to the data plane.
type RouterConfig struct {
	AuthValidator  *auth.Validator
	RequireAuth    bool
	RequiredScopes []string
	NumShards      uint32
	InboxRepo      ports.InboxRepository
	ExecutionRepo  ports.ExecutionRepository
	OutboxRepo     ports.OutboxRepository
	DomainServices ports.DomainServiceUseCase
	// ReadinessChecks are named dependency probes served by GET /ready.
	ReadinessChecks map[string]func(context.Context) error
}

// Router holds all HTTP handlers and middleware.
type Router struct {
	handler http.Handler
}

// NewRouter creates the management API router with default settings.
func NewRouter(
	ruleSvc ports.RuleService,
	workflowSvc ports.WorkflowService,
	workflowDefRepo ports.WorkflowDefinitionRepository,
	contractRegistry ports.ContractRegistry,
	batchSvc ports.BatchService,
	quarantineSvc ports.QuarantineService,
) *Router {
	return NewRouterWithConfig(
		ruleSvc, workflowSvc, workflowDefRepo, contractRegistry,
		batchSvc, quarantineSvc, RouterConfig{},
	)
}

// NewRouterWithConfig creates the control-plane HTTP router. It does not register
// event ingestion or create a broker connection; producers publish to the broker
// independently and workers own event processing.
func NewRouterWithConfig(
	ruleSvc ports.RuleService,
	workflowSvc ports.WorkflowService,
	workflowDefRepo ports.WorkflowDefinitionRepository,
	contractRegistry ports.ContractRegistry,
	batchSvc ports.BatchService,
	quarantineSvc ports.QuarantineService,
	cfg RouterConfig,
) *Router {
	mux := http.NewServeMux()

	handler := middleware.Recovery(mux)
	handler = middleware.TenantExtractor(handler)
	if cfg.RequireAuth {
		if cfg.AuthValidator == nil {
			handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/health" {
					mux.ServeHTTP(w, r)
					return
				}
				responses.WriteError(w, http.StatusServiceUnavailable, "required authentication is not configured", responses.CodeInternalError)
			})
		} else {
			handler = auth.Middleware(cfg.AuthValidator, cfg.RequiredScopes...)(handler)
		}
	} else if cfg.AuthValidator != nil {
		handler = auth.OptionalAuthMiddleware(cfg.AuthValidator)(handler)
	}

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		responses.WriteOK(w, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", readinessHandler(cfg.ReadinessChecks))

	handlers.NewRuleHandler(ruleSvc).RegisterRoutes(mux)
	handlers.NewWorkflowHandler(workflowSvc, nil, workflowDefRepo).RegisterRoutes(mux)
	handlers.NewContractHandler(contractRegistry).RegisterRoutes(mux)
	handlers.NewBatchHandler(batchSvc).RegisterRoutes(mux)
	handlers.NewQuarantineHandler(quarantineSvc).RegisterRoutes(mux)
	handlers.NewDomainServiceHandler(cfg.DomainServices).RegisterRoutes(mux)

	// Event trace is a read-only management view over durable runtime records.
	numShards := cfg.NumShards
	if numShards == 0 {
		numShards = config.DefaultNumShards
	}
	if cfg.InboxRepo != nil && cfg.ExecutionRepo != nil && cfg.OutboxRepo != nil {
		handlers.NewEventTraceHandler(cfg.InboxRepo, cfg.ExecutionRepo, cfg.OutboxRepo, numShards).RegisterRoutes(mux)
	}

	return &Router{handler: handler}
}

// ServeHTTP implements http.Handler.
func (r *Router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.handler.ServeHTTP(w, req)
}

// Run starts the HTTP server.
func (r *Router) Run(ctx context.Context, addr string) error {
	server := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		return server.Shutdown(shutdownCtx)
	case err := <-serverErr:
		return err
	}
}
