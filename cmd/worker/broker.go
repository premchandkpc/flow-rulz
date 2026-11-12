package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/brokerstore"
	"github.com/flowrulz/flow-rulz/internal/adapters/effects"
	"github.com/flowrulz/flow-rulz/internal/adapters/embedded"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/rules"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
	brokerservice "github.com/flowrulz/flow-rulz/internal/services/broker"
	svceffects "github.com/flowrulz/flow-rulz/internal/services/effects"
)

// runBroker composes the shared broker/rule core with selected transport adapters.
func runBroker(ctx context.Context, cfg *config.Config) (runErr error) {
	revisions, err := loadDirectRules(cfg.Worker.RulesDir)
	if err != nil {
		return err
	}
	var store broker.Store
	if cfg.Broker.Storage == "file" {
		disk, err := brokerstore.Open(cfg.Broker.DataDir)
		if err != nil {
			return err
		}
		defer func() { runErr = errors.Join(runErr, disk.Close()) }()
		store = disk
	}
	ackWait := time.Duration(cfg.Broker.AckWaitMS) * time.Millisecond
	b, err := broker.New(broker.Config{Partitions: cfg.Worker.NumShards, MaxMessages: cfg.Broker.MaxMessages, MaxBytes: cfg.Broker.MaxBytes, AckWait: ackWait}, store)
	if err != nil {
		return err
	}
	consumer, err := embedded.NewConsumer(b)
	if err != nil {
		return err
	}
	publisher, err := newBrokerPublisher(cfg, b)
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, publisher.Close()) }()
	sender := svceffects.NewRouterWithConfig(publisher, effects.NewTopicDestination(publisher), "events", cfg.Worker.NumShards)
	worker, err := application.NewDirectWorker(consumer, rules.NewEvaluator(domain.SystemClock{}), sender, revisions, cfg.EventRetryDelay(), ackWait/2)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	protocol := brokerservice.New(b, cfg.Broker.Token)
	endpoints, err := newBrokerTransports(runCtx, cfg, protocol)
	if err != nil {
		cancel()
		shutdownCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer stop()
		return errors.Join(err, closeBrokerTransports(shutdownCtx, endpoints))
	}
	log.Printf("FlowRulz broker initialized transports=%v output=%s storage=%s partitions=%d", cfg.Broker.Transports, cfg.Broker.OutputTransport, cfg.Broker.Storage, cfg.Worker.NumShards)
	return runBrokerTasks(runCtx, cancel, worker, endpoints)
}

func runBrokerTasks(ctx context.Context, cancel context.CancelFunc, worker *application.DirectWorker, endpoints []brokerTransport) error {
	results := make(chan error, len(endpoints)+1)
	for _, endpoint := range endpoints {
		go func() {
			err := endpoint.run(ctx)
			if err != nil {
				err = fmt.Errorf("broker %s transport: %w", endpoint.name, err)
			}
			results <- err
		}()
	}
	go func() { results <- worker.Run(ctx) }()
	remaining := len(endpoints) + 1
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-results:
		remaining--
		runErr = err
	}
	cancel()
	shutdownCtx, stop := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer stop()
	runErr = errors.Join(runErr, closeBrokerTransports(shutdownCtx, endpoints))
	for remaining > 0 {
		select {
		case err := <-results:
			remaining--
			runErr = errors.Join(runErr, err)
		case <-shutdownCtx.Done():
			return errors.Join(runErr, errors.New("broker shutdown timed out"))
		}
	}
	return runErr
}
