//go:build e2e

package e2e

import (
	"context"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/adapters/remote"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/tests/brokertest"
	"github.com/google/uuid"
	testcontainers "github.com/testcontainers/testcontainers-go"
	natstest "github.com/testcontainers/testcontainers-go/modules/nats"
)

func TestBrokerHTTPAndTCPAndNATSMatrix(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	container, err := natstest.RunContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Error(err)
		}
	})
	url, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, inputTransport := range []string{"http", "tcp", "nats"} {
		for _, outputTransport := range []string{"http", "tcp", "nats"} {
			t.Run(inputTransport+"-to-"+outputTransport, func(t *testing.T) {
				source, target := brokertest.New(t), brokertest.New(t)
				input := brokerMatrixClient(t, ctx, source, inputTransport, url)
				peer := brokerMatrixClient(t, ctx, target, outputTransport, url)
				source.StartRules(t, &remote.Publisher{Client: peer, Partitions: 4})
				target.StartRules(t, target.LocalPublisher())
				brokertest.CheckChain(t, input, target.Client(t, "tcp"))
			})
		}
	}
}

func brokerMatrixClient(t *testing.T, ctx context.Context, node *brokertest.Node, transport, url string) ports.BrokerCaller {
	t.Helper()
	if transport != "nats" {
		return node.Client(t, transport)
	}
	subject := "flowrulz.test." + uuid.NewString()
	endpoint, err := nats.NewBrokerEndpoint(ctx, url, subject, node.Protocol)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := endpoint.Close(); err != nil {
			t.Error(err)
		}
	})
	client, err := nats.NewBrokerClient(url, subject, brokertest.Token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client
}
