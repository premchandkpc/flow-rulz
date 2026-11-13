// Package brokertest owns shared broker transport fixtures for integration/E2E tests.
package brokertest

import (
	"context"
	"encoding/json"
	"net"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/effects"
	"github.com/flowrulz/flow-rulz/internal/adapters/embedded"
	brokerhttp "github.com/flowrulz/flow-rulz/internal/adapters/http/broker"
	brokertcp "github.com/flowrulz/flow-rulz/internal/adapters/tcp"
	"github.com/flowrulz/flow-rulz/internal/application"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/rules"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
	brokerservice "github.com/flowrulz/flow-rulz/internal/services/broker"
	svceffects "github.com/flowrulz/flow-rulz/internal/services/effects"
)

// Token is a fixture credential shared only by test clients and servers.
const Token = "broker-transport-test-token"

// Publisher captures the two output capabilities used by rule routing.
type Publisher interface {
	ports.EventPublisher
	ports.SubjectPublisher
}

// Node owns one shared broker with HTTP and TCP adapters for transport comparisons.
type Node struct {
	Broker   *broker.Broker
	Protocol ports.BrokerProtocol
	HTTPURL  string
	TCPAddr  string
}

// New creates real local HTTP/TCP endpoints around one in-memory broker.
func New(t *testing.T) *Node {
	t.Helper()
	b, err := broker.New(broker.Config{Partitions: 4, MaxMessages: 100, MaxBytes: 1 << 20, AckWait: 3 * time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{Broker: b, Protocol: brokerservice.New(b, Token)}
	httpServer := httptest.NewServer(brokerhttp.NewProtocolHandler(n.Protocol))
	n.HTTPURL = httpServer.URL
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		httpServer.Close()
		t.Fatal(err)
	}
	n.TCPAddr = listener.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- (&brokertcp.Server{Protocol: n.Protocol}).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		httpServer.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("TCP fixture failed to stop")
		}
	})
	return n
}

// LocalPublisher supplies the default in-process destination.
func (n *Node) LocalPublisher() Publisher {
	return &embedded.Publisher{Broker: n.Broker, Partitions: 4}
}

// StartRules runs the sample event-to-child-to-command chain with selected outputs.
func (n *Node) StartRules(t *testing.T, publisher Publisher) {
	t.Helper()
	consumer, err := embedded.NewConsumer(n.Broker)
	if err != nil {
		t.Fatal(err)
	}
	var revisions []*domain.RuleRevision
	for _, name := range []string{"a.json", "processed.json"} {
		source, err := os.ReadFile("../../examples/direct-worker/rules/" + name) // #nosec G304 -- name comes from the fixed fixture list above
		if err != nil {
			t.Fatal(err)
		}
		revision, err := rules.NewCompiler(rules.DefaultLimits()).Compile(source)
		if err != nil {
			t.Fatal(err)
		}
		revisions = append(revisions, revision)
	}
	sender := svceffects.NewRouterWithConfig(publisher, effects.NewTopicDestination(publisher), "events", 4)
	worker, err := application.NewDirectWorker(consumer, rules.NewEvaluator(domain.SystemClock{}), sender, revisions, 10*time.Millisecond, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("rule fixture failed to stop")
		}
	})
}

// CheckChain publishes through one transport and settles the output through another.
func CheckChain(t *testing.T, input, output ports.BrokerCaller) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	event := &domain.EventEnvelope{ID: "transport-event", Type: "a.created", TenantID: "tenant-a", PartitionKey: "key-1", OccurredAt: time.Now().UTC(), Data: []byte(`{"id":"123"}`)}
	response, err := input.Call(ctx, ports.BrokerRequest{Operation: "publish", TenantID: event.TenantID, Event: event})
	if err != nil || response.Code != "accepted" {
		t.Fatalf("publish response=%+v err=%v", response, err)
	}
	response, err = output.Call(ctx, ports.BrokerRequest{Operation: "consume", TenantID: event.TenantID, Topic: "commands.b.handle", Group: "service-b", WaitMS: 2000})
	if err != nil || response.Code != "message" {
		t.Fatalf("consume response=%+v err=%v", response, err)
	}
	checkAndSettle(ctx, t, output, response)
}

func checkAndSettle(ctx context.Context, t *testing.T, client ports.BrokerCaller, response ports.BrokerResponse) {
	t.Helper()
	var claim broker.Claim
	if err := json.Unmarshal(response.Data, &claim); err != nil {
		t.Fatal(err)
	}
	var effect domain.Effect
	if err := json.Unmarshal(claim.Record.Payload, &effect); err != nil {
		t.Fatal(err)
	}
	if effect.Destination != "b" || effect.Name != "handle" || effect.TenantID != "tenant-a" || effect.IdempotencyKey == "" {
		t.Fatalf("unexpected effect: %+v", effect)
	}
	request := ports.BrokerRequest{Operation: "ack", TenantID: "tenant-b", Topic: claim.Record.Topic, Group: claim.Group, Partition: claim.Record.Partition, Receipt: claim.Receipt}
	response, err := client.Call(ctx, request)
	if err != nil || response.Code != "conflict" {
		t.Fatalf("cross-tenant ACK response=%+v err=%v", response, err)
	}
	request.TenantID = "tenant-a"
	response, err = client.Call(ctx, request)
	if err != nil || response.Code != "ok" {
		t.Fatalf("ACK response=%+v err=%v", response, err)
	}
	response, err = client.Call(ctx, ports.BrokerRequest{Operation: "consume", TenantID: "tenant-a", Topic: claim.Record.Topic, Group: claim.Group})
	if err != nil || response.Code != "empty" {
		t.Fatalf("ACKed output redelivered: %+v %v", response, err)
	}
}

type localClient struct{ protocol ports.BrokerProtocol }

func (c localClient) Call(ctx context.Context, request ports.BrokerRequest) (ports.BrokerResponse, error) {
	request.Version = ports.BrokerProtocolVersion
	request.Token = Token
	return c.protocol.Execute(ctx, request), nil
}
func (localClient) Close() error { return nil }

// Client returns a registered fixture client for an enabled local transport.
func (n *Node) Client(t *testing.T, transport string) ports.BrokerCaller {
	t.Helper()
	var client ports.BrokerCaller
	switch transport {
	case "inprocess":
		client = localClient{protocol: n.Protocol}
	case "http":
		var err error
		client, err = brokerhttp.NewClient(n.HTTPURL, Token)
		if err != nil {
			t.Fatal(err)
		}
	case "tcp":
		client = &brokertcp.Client{Addr: n.TCPAddr, Token: Token}
	default:
		t.Fatalf("unknown fixture client %q", transport)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	return client
}
