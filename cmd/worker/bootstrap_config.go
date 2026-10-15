// Package main configures dependencies for the FlowRulz worker service.
package main

import (
	"errors"

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
		MaxAge:     cfg.NATS.MaxAge,
		MaxBytes:   cfg.NATS.MaxBytes,
		MaxMsgs:    cfg.NATS.MaxMsgs,
	}
}

func newCommandSender(publisher ports.SubjectPublisher) (ports.EffectSender, error) {
	if publisher == nil {
		return nil, errors.New("NATS publisher is required for command effects")
	}
	return effects.NewNATSDestination(publisher), nil
}

func newWorkerConfig(cfg *config.Config) application.WorkerConfig {
	workerConfig := application.DefaultWorkerConfig()
	workerConfig.ServiceID = cfg.Worker.ServiceID
	workerConfig.DeploymentRevision = cfg.Worker.DeploymentRevision
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
