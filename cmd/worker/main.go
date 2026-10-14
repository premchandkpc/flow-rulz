package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/flowrulz/flow-rulz/internal/adapters/effects"
	"github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/observability"
	"github.com/flowrulz/flow-rulz/internal/rules"
	"github.com/flowrulz/flow-rulz/internal/runtime/scheduler"
	"github.com/flowrulz/flow-rulz/internal/runtime/shard"
	"github.com/flowrulz/flow-rulz/internal/services/batches"
	svceffects "github.com/flowrulz/flow-rulz/internal/services/effects"
	svcevents "github.com/flowrulz/flow-rulz/internal/services/events"
	"github.com/flowrulz/flow-rulz/internal/storage"
)

const (
	serviceName  = "flowrulz-worker"
	eventSubject = "events"
)

// main owns process exit behavior. run returns errors so deferred cleanup executes.
func main() {
	if err := run(); err != nil {
		log.Printf("%s stopped with error: %v", serviceName, err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	obsConfig := observability.DefaultConfig()
	obsConfig.ServiceName = serviceName
	obs, err := observability.New(obsConfig)
	if err != nil {
		return fmt.Errorf("initialize observability: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeoutDuration())
		defer cancel()
		if err := obs.Shutdown(shutdownCtx); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("shutdown observability: %w", err))
		}
	}()

	obs.Logger.Info("worker starting", "environment", cfg.App.Env)

	if metricsAddr := strings.TrimSpace(os.Getenv("FLOWRULZ_METRICS_ADDR")); metricsAddr != "" {
		shutdownMetrics, err := startMetricsServer(metricsAddr, os.Getenv("FLOWRULZ_CONSOLE_ORIGIN"))
		if err != nil {
			return err
		}
		defer func() {
			if err := shutdownMetrics(); err != nil {
				runErr = errors.Join(runErr, err)
			}
		}()
		obs.Logger.Info("worker metrics endpoint enabled", "address", metricsAddr)
	}

	db, err := storage.Open(ctx, cfg.Database)
	if err != nil {
		return fmt.Errorf("open storage: %w", err)
	}
	// Storage.Close currently has no error return; keep it deferred so all
	// startup failure paths release the database resources.
	defer db.Close()

	if err := db.Initialize(ctx); err != nil {
		return fmt.Errorf("initialize storage: %w", err)
	}
	repositories := db.Repositories()

	natsCfg := newNATSConfig(cfg)
	consumer, err := nats.NewConsumer(ctx, natsCfg)
	if err != nil {
		return fmt.Errorf("initialize NATS JetStream consumer: %w", err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close NATS consumer: %w", err))
		}
	}()
	publisher, err := nats.NewPublisher(natsCfg)
	if err != nil {
		return fmt.Errorf("initialize NATS JetStream publisher: %w", err)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close NATS publisher: %w", err))
		}
	}()

	compiler := rules.NewCompiler(rules.DefaultLimits())
	clock := domain.SystemClock{}
	evaluator := rules.NewEvaluator(clock)

	commandSender, err := newCommandSender(publisher)
	if err != nil {
		return err
	}

	// Route child events, commands, and tenant-service deliveries through NATS JetStream.
	// Each registered service has a unique subject for durable fan-out.
	serviceSender := effects.NewDomainServiceSender(repositories.DomainServices, publisher)
	scheduledEventSender := effects.NewScheduledEventSender(repositories.ScheduledEvents)
	effectSender := svceffects.NewRouterWithConfig(
		publisher, commandSender, eventSubject, cfg.Worker.NumShards, serviceSender, scheduledEventSender,
	)
	workerID := cfg.WorkerID()
	eventsSvc := svcevents.NewService(
		compiler,
		evaluator,
		repositories.Quarantine,
		clock,
		repositories.Transactions,
		repositories.NewEventTxRepositories,
	).WithContractRegistry(repositories.Contracts)
	effectsSvc := svceffects.NewService(
		repositories.Outbox,
		effectSender,
		repositories.Quarantine,
		clock,
		workerID,
	)

	batchCfg := domain.BatchConfig{
		Mode:     domain.BatchMode(cfg.Worker.BatchMode),
		MaxBatch: cfg.Worker.BatchMax,
		Window:   cfg.BatchWindow(),
	}
	leaseManager := shard.NewLeaseManagerWithReplicaCountAndRenewInterval(
		repositories.ShardLeases,
		clock,
		workerID,
		cfg.Worker.NumShards,
		cfg.Worker.WorkerReplicas,
		cfg.LeaseTTL(),
		cfg.LeaseRenewalInterval(),
	)
	batchSvc := batches.NewServiceWithLeaseProvider(
		repositories.Batches,
		eventsSvc,
		clock,
		batchCfg,
		leaseManager,
		workerID,
		cfg.Worker.NumShards,
	)
	workerConfig := newWorkerConfig(cfg)

	worker, err := application.NewWorker(
		consumer,
		eventsSvc,
		effectsSvc,
		batchSvc,
		cfg.SchedulerPollInterval(),
		clock,
		leaseManager,
		workerID,
		cfg.Worker.NumShards,
		workerConfig,
	)
	if err != nil {
		return fmt.Errorf("construct worker: %w", err)
	}
	sched := scheduler.NewScheduler(
		repositories.ScheduledEvents,
		publisher,
		clock,
		cfg.SchedulerPollInterval(),
		cfg.Worker.SchedulerBatchSize,
		cfg.Worker.NumShards,
	)

	obs.Logger.Info("worker initialized", "worker_id", workerID, "shards", cfg.Worker.NumShards)
	sched.Start(ctx)
	defer sched.Stop()

	if err := worker.Run(ctx); err != nil {
		return fmt.Errorf("run worker: %w", err)
	}
	obs.Logger.Info("worker stopped", "worker_id", workerID)
	return nil
}
