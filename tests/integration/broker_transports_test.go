package integration

import (
	"context"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	brokerhttp "github.com/flowrulz/flow-rulz/internal/adapters/http/broker"
	"github.com/flowrulz/flow-rulz/internal/adapters/remote"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/tests/brokertest"
)

func TestBrokerRejectsUnsupportedServiceTargets(t *testing.T) {
	node := brokertest.New(t)
	client := node.Client(t, "inprocess")
	for name, event := range map[string]*domain.EventEnvelope{
		"header": {Headers: map[string]string{"flow.service_id": "orders"}},
		"meta":   {Meta: map[string]any{"target_service_id": "orders"}},
	} {
		t.Run(name, func(t *testing.T) {
			event.ID, event.Type, event.TenantID, event.PartitionKey = "id-"+name, "orders", "tenant-a", "key"
			event.OccurredAt, event.Data = time.Now(), []byte(`{}`)
			response, err := client.Call(context.Background(), ports.BrokerRequest{Operation: "publish", TenantID: event.TenantID, Event: event})
			if err != nil || response.Code != "invalid" {
				t.Fatalf("targeted publish response=%+v err=%v, want invalid", response, err)
			}
		})
	}
}

func TestBrokerOptionalTransportMatrix(t *testing.T) {
	for _, inputTransport := range []string{"inprocess", "http", "tcp"} {
		for _, outputTransport := range []string{"local", "http", "tcp"} {
			t.Run(inputTransport+"-to-"+outputTransport, func(t *testing.T) {
				source := brokertest.New(t)
				if outputTransport == "local" {
					source.StartRules(t, source.LocalPublisher())
					brokertest.CheckChain(t, source.Client(t, inputTransport), source.Client(t, "tcp"))
					return
				}
				target := brokertest.New(t)
				peer := target.Client(t, outputTransport)
				source.StartRules(t, &remote.Publisher{Client: peer, Partitions: 4})
				target.StartRules(t, target.LocalPublisher())
				brokertest.CheckChain(t, source.Client(t, inputTransport), target.Client(t, "http"))
			})
		}
	}
}

type rejectFirstAppend struct {
	delegate ports.BrokerProtocol
	attempts atomic.Int32
}

func (p *rejectFirstAppend) Execute(ctx context.Context, request ports.BrokerRequest) ports.BrokerResponse {
	if request.Operation == "append" && p.attempts.Add(1) == 1 {
		return ports.BrokerResponse{Version: ports.BrokerProtocolVersion, ID: request.ID, Code: "unavailable"}
	}
	return p.delegate.Execute(ctx, request)
}

func TestRejectedHTTPPeerPublicationRetriesInput(t *testing.T) {
	source, target := brokertest.New(t), brokertest.New(t)
	fault := &rejectFirstAppend{delegate: target.Protocol}
	server := httptest.NewServer(brokerhttp.NewProtocolHandler(fault))
	t.Cleanup(server.Close)
	peer, err := brokerhttp.NewClient(server.URL, brokertest.Token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := peer.Close(); err != nil {
			t.Error(err)
		}
	})
	source.StartRules(t, &remote.Publisher{Client: peer, Partitions: 4})
	target.StartRules(t, target.LocalPublisher())
	brokertest.CheckChain(t, source.Client(t, "tcp"), target.Client(t, "http"))
	if fault.attempts.Load() < 2 {
		t.Fatal("failed publication was acknowledged without retry")
	}
}
