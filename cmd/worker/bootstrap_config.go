// Package main configures dependencies for the FlowRulz worker service.
package main

import (
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/adapters/effects"
	"github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

func newNATSConfig(cfg *config.Config) nats.Config {
	return nats.Config{
		NatsURL:    cfg.NATS.URL,
		Stream:     cfg.NATS.Stream,
		Consumer:   cfg.NATS.Consumer,
		Subjects:   []string{eventSubject},
		RuleSets:   cfg.NATS.RuleSets,
		AckWait:    cfg.NATSAckWait(),
		MaxDeliver: cfg.Worker.NATSMaxDeliver,
		NumShards:  cfg.Worker.NumShards,
	}
}

func newExternalSender(cfg *config.Config) (ports.EffectSender, error) {
	switch {
	case cfg.Worker.EffectDestinationURL != "":
		return effects.NewHTTPDestination(cfg.Worker.EffectDestinationURL), nil
	case cfg.App.Env == "development" || cfg.App.Env == "test":
		return effects.NewFakeDestination(), nil
	default:
		return nil, fmt.Errorf("EFFECT_DESTINATION_URL is required when APP_ENV=%q", cfg.App.Env)
	}
}

func newWorkerConfig(cfg *config.Config) application.WorkerConfig {
	workerConfig := application.DefaultWorkerConfig()
	workerConfig.MaxGlobalInFlight = cfg.Worker.MaxGlobalInFlight
	workerConfig.FetchBatchSize = cfg.Worker.FetchBatchSize
	workerConfig.EffectPublishInterval = cfg.EffectPublishInterval()
	workerConfig.EffectPublishBatchSize = cfg.Worker.EffectPublishBatchSize
	workerConfig.RetryDelay = cfg.EventRetryDelay()
	workerConfig.MaxPerKeyQueue = cfg.Worker.MaxPerKeyQueue
	workerConfig.KeyQueueWorkers = cfg.Worker.KeyQueueWorkers
	workerConfig.HotKeyThreshold = cfg.Worker.HotKeyThreshold
	workerConfig.HotKeyCheckInterval = cfg.HotKeyCheckInterval()
	return workerConfig
}
