package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/flowrulz/flow-rulz/internal/observability"
)

// startMetricsServer opts a worker into a private process-local Prometheus
// endpoint. Binding is synchronous so address conflicts fail worker startup.
func startMetricsServer(addr, allowedOrigin string) (func() error, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen for worker metrics on %q: %w", addr, err)
	}

	server := observability.NewMetricsServer(allowedOrigin)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("worker metrics server stopped: %v", err)
		}
	}()

	return func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			return fmt.Errorf("shutdown worker metrics server: %w", err)
		}
		return nil
	}, nil
}
