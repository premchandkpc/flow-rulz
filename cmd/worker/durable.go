package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/effects"
	"github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/observability"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/rules"
	"github.com/flowrulz/flow-rulz/internal/runtime/scheduler"
	"github.com/flowrulz/flow-rulz/internal/runtime/shard"
	"github.com/flowrulz/flow-rulz/internal/services/batches"
	svceffects "github.com/flowrulz/flow-rulz/internal/services/effects"
	svcevents "github.com/flowrulz/flow-rulz/internal/services/events"
	"github.com/flowrulz/flow-rulz/internal/storage"
)

// runDurable starts worker telemetry and state dependencies, runs the durable
// scheduler and worker until shutdown, and joins teardown errors into the
// returned error.
func runDurable(ctx context.Context, cfg *config.Config) (runErr error) {
	infra, err := startWorkerTelemetry(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, infra.Close()) }()
	if err := infra.startDependencies(ctx, cfg); err != nil {
		return err
	}
	worker, sched, workerID, err := newDurableRuntime(cfg, infra)
	if err != nil {
		return err
	}
	return runUntilShutdown(ctx, cfg, infra.obs, workerID, worker, sched)
}

// startWorkerTelemetry starts observability and the optional process-local
// metrics listener, registering their shutdown steps on the infrastructure.
// A metrics failure closes observability before the error is returned.
func startWorkerTelemetry(ctx context.Context, cfg *config.Config) (*durableInfra, error) {
	metricsAddr := strings.TrimSpace(os.Getenv("FLOWRULZ_METRICS_ADDR"))
	obs, stopObservability, err := startWorkerObservability(ctx, cfg, metricsAddr)
	if err != nil {
		return nil, err
	}
	infra := &durableInfra{obs: obs, stops: []func() error{stopObservability}}
	obs.Logger.Info("worker starting", "environment", cfg.App.Env)
	if err := infra.startMetrics(ctx, metricsAddr); err != nil {
		return nil, errors.Join(err, infra.Close())
	}
	return infra, nil
}

// startWorkerObservability initializes logging and metrics and returns a
// bounded shutdown step for the teardown stack.
func startWorkerObservability(ctx context.Context, cfg *config.Config, metricsAddr string) (*observability.Observability, func() error, error) {
	obsConfig := observability.DefaultConfig()
	obsConfig.ServiceName = serviceName
	if metricsAddr != "" {
		// The process-local listener owns this endpoint; disable the generic
		// observability listener to avoid binding the same port twice.
		obsConfig.MetricsPort = 0
	}
	obs, err := observability.New(ctx, obsConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("initialize observability: %w", err)
	}
	return obs, newObservabilityShutdown(ctx, obs, cfg.ShutdownTimeoutDuration()), nil
}

// newObservabilityShutdown returns a teardown step that shuts observability
// down with a bounded context that survives caller cancellation.
func newObservabilityShutdown(ctx context.Context, obs *observability.Observability, timeout time.Duration) func() error {
	return func() error {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		if err := obs.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown observability: %w", err)
		}
		return nil
	}
}

// durableInfra owns the durable worker's process-level resources. Stops are
// registered in creation order; Close runs them in reverse.
type durableInfra struct {
	obs          *observability.Observability
	repositories storage.Repositories
	consumer     *nats.Consumer
	publisher    *nats.Publisher
	stops        []func() error
}

// Close runs every registered shutdown step in reverse creation order and
// joins the errors so one failure never skips the remaining resources.
func (infra *durableInfra) Close() error {
	var errs []error
	for i := len(infra.stops) - 1; i >= 0; i-- {
		if err := infra.stops[i](); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// startDependencies opens storage before NATS so a database failure never
// leaves broker connections behind, registering each teardown step as it goes.
func (infra *durableInfra) startDependencies(ctx context.Context, cfg *config.Config) error {
	if err := infra.openStorage(ctx, cfg); err != nil {
		return err
	}
	return infra.connectNATS(ctx, cfg)
}

// startMetrics starts the process-local metrics listener when its address is
// configured; an empty address registers no listener and no shutdown step.
func (infra *durableInfra) startMetrics(ctx context.Context, metricsAddr string) error {
	if metricsAddr == "" {
		return nil
	}
	shutdown, err := startMetricsServer(ctx, metricsAddr, os.Getenv("FLOWRULZ_CONSOLE_ORIGIN"))
	if err != nil {
		return err
	}
	infra.stops = append(infra.stops, shutdown)
	infra.obs.Logger.Info("worker metrics endpoint enabled", "address", metricsAddr)
	return nil
}

// openStorage opens and migrates the database, registering Close before
// Initialize so every later startup failure still releases the pool.
func (infra *durableInfra) openStorage(ctx context.Context, cfg *config.Config) error {
	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	// Storage.Close has no error return; keep it registered so all startup
	// failure paths release the database resources.
	infra.stops = append(infra.stops, func() error { db.Close(); return nil })
	if err := db.Initialize(ctx); err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	infra.repositories = db.Repositories()
	return nil
}

// connectNATS initializes the JetStream consumer and publisher, registering
// their teardown steps in creation order for reverse-order close.
func (infra *durableInfra) connectNATS(ctx context.Context, cfg *config.Config) error {
	natsCfg := newNATSConfig(cfg)
	consumer, err := nats.NewConsumer(ctx, natsCfg)
	if err != nil {
		return fmt.Errorf("initialize NATS JetStream consumer: %w", err)
	}
	infra.stops = append(infra.stops, closeStep("NATS consumer", consumer.Close))
	publisher, err := nats.NewPublisher(natsCfg)
	if err != nil {
		return fmt.Errorf("initialize NATS JetStream publisher: %w", err)
	}
	infra.stops = append(infra.stops, closeStep("NATS publisher", publisher.Close))
	infra.consumer, infra.publisher = consumer, publisher
	return nil
}

// closeStep wraps a resource closer into a teardown step that names the
// resource when closing fails.
func closeStep(name string, closer func() error) func() error {
	return func() error {
		if err := closer(); err != nil {
			return fmt.Errorf("close %s: %w", name, err)
		}
		return nil
	}
}

// runUntilShutdown logs lifecycle transitions, starts the scheduler, and runs
// the worker until ctx is cancelled, wrapping failures with operation context.
func runUntilShutdown(ctx context.Context, cfg *config.Config, obs *observability.Observability, workerID string, worker *application.Worker, sched *scheduler.Scheduler) error {
	obs.Logger.Info("worker initialized", "worker_id", workerID, "shards", cfg.Worker.NumShards)
	sched.Start(ctx)
	defer sched.Stop()
	if err := worker.Run(ctx); err != nil {
		return fmt.Errorf("run worker: %w", err)
	}
	obs.Logger.Info("worker stopped", "worker_id", workerID)
	return nil
}

// newDurableRuntime composes the event pipeline, processing services, and
// scheduler that back the durable worker, returning its worker identity for
// lifecycle logging.
func newDurableRuntime(cfg *config.Config, infra *durableInfra) (*application.Worker, *scheduler.Scheduler, string, error) {
	workerID := cfg.WorkerID()
	clock := domain.SystemClock{}
	eventPublisher, effectSender, err := newEventPipeline(cfg, infra.publisher, infra.repositories)
	if err != nil {
		return nil, nil, "", err
	}
	eventsSvc, effectsSvc, leaseManager, batchSvc := newProcessingServices(cfg, infra.repositories, effectSender, clock, workerID)
	worker, err := application.NewWorker(infra.consumer, eventsSvc, effectsSvc, batchSvc, cfg.SchedulerPollInterval(), clock, leaseManager, workerID, cfg.Worker.NumShards, newWorkerConfig(cfg))
	if err != nil {
		return nil, nil, "", fmt.Errorf("construct worker: %w", err)
	}
	sched := scheduler.NewEnvelopeScheduler(infra.repositories.ScheduledEvents, eventPublisher, clock, cfg.SchedulerPollInterval(), cfg.Worker.SchedulerBatchSize)
	return worker, sched, workerID, nil
}

// newEventPipeline builds the JetStream event publisher and the effect router
// that sends child events, commands, and tenant-service deliveries.
func newEventPipeline(cfg *config.Config, publisher *nats.Publisher, repositories storage.Repositories) (*nats.EventPublisher, *svceffects.Router, error) {
	eventPublisher := nats.NewEventPublisher(publisher, eventSubject, cfg.Worker.NumShards)
	commandSender, err := newCommandSender(publisher)
	if err != nil {
		return nil, nil, err
	}
	// Each registered service gets a unique subject for durable fan-out.
	serviceSender := effects.NewDomainServiceSender(repositories.DomainServices, publisher)
	scheduledEventSender := effects.NewScheduledEventSender(repositories.ScheduledEvents)
	effectSender := svceffects.NewEnvelopeRouter(eventPublisher, commandSender, serviceSender, scheduledEventSender)
	return eventPublisher, effectSender, nil
}

// newProcessingServices builds the transactional event and outbox services
// plus the shard lease manager and the lease-backed batch service that share
// one clock and worker identity.
func newProcessingServices(cfg *config.Config, repositories storage.Repositories, effectSender *svceffects.Router, clock ports.Clock, workerID string) (*svcevents.Service, *svceffects.Service, *shard.LeaseManager, *batches.Service) {
	compiler := rules.NewCompiler(rules.DefaultLimits())
	evaluator := rules.NewEvaluator(clock)
	eventsSvc := svcevents.NewService(compiler, evaluator, repositories.Quarantine, clock, repositories.Transactions, repositories.NewEventTxRepositories).WithContractRegistry(repositories.Contracts)
	effectsSvc := svceffects.NewService(repositories.Outbox, effectSender, repositories.Quarantine, clock, workerID)
	leaseManager := shard.NewLeaseManagerWithReplicaCountAndRenewInterval(repositories.ShardLeases, clock, workerID, cfg.Worker.NumShards, cfg.Worker.WorkerReplicas, cfg.LeaseTTL(), cfg.LeaseRenewalInterval())
	batchCfg := domain.BatchConfig{Mode: domain.BatchMode(cfg.Worker.BatchMode), MaxBatch: cfg.Worker.BatchMax, Window: cfg.BatchWindow()}
	batchSvc := batches.NewServiceWithLeaseProvider(repositories.Batches, eventsSvc, clock, batchCfg, leaseManager, workerID, cfg.Worker.NumShards)
	return eventsSvc, effectsSvc, leaseManager, batchSvc
}
