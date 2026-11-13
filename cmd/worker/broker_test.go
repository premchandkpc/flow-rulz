package main

import (
	"context"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
	brokerservice "github.com/flowrulz/flow-rulz/internal/services/broker"
)

func TestBrokerRunsWithEveryNetworkTransportDisabled(t *testing.T) {
	cfg := &config.Config{
		Worker: config.WorkerConfig{RulesDir: "../../examples/direct-worker/rules", NumShards: 4, RetryDelayMS: 10},
		Broker: config.BrokerConfig{Transports: []string{"none"}, Addr: "unused", TCPAddr: "unused", Storage: "memory", MaxMessages: 100, MaxBytes: 1 << 20, AckWaitMS: 1000, OutputTransport: "local"},
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runBroker(ctx, cfg) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("in-process broker failed to stop")
	}
}

func TestTCPOnlyBrokerDoesNotInitializeHTTPOrNATS(t *testing.T) {
	b, err := broker.New(broker.Config{Partitions: 1, MaxMessages: 10, MaxBytes: 1024, AckWait: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{Broker: config.BrokerConfig{Transports: []string{"tcp"}, Addr: "unused", TCPAddr: "127.0.0.1:0"}}
	endpoints, err := newBrokerTransports(t.Context(), cfg, brokerservice.New(b, ""))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := closeBrokerTransports(t.Context(), endpoints); err != nil {
			t.Error(err)
		}
	})
	if len(endpoints) != 1 || endpoints[0].name != "tcp" {
		t.Fatalf("unexpected endpoints: %+v", endpoints)
	}
}
