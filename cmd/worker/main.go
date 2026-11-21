package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/flowrulz/flow-rulz/internal/config"
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

// run loads configuration, installs signal handling, and dispatches to the
// configured worker mode.
func run() error {
	cfg, err := config.LoadWorker()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	if cfg.Worker.Mode == "broker" {
		return runBroker(ctx, cfg)
	}
	if cfg.Worker.Mode == "direct" {
		return runDirect(ctx, cfg)
	}
	return runDurable(ctx, cfg)
}
