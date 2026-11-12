package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/google/uuid"
	natslib "github.com/nats-io/nats.go"
)

// BrokerEndpoint exposes the shared protocol on one broker-specific RPC subject.
type BrokerEndpoint struct {
	conn     *natslib.Conn
	sub      *natslib.Subscription
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closing  bool
	workers  sync.WaitGroup
	slots    chan struct{}
	once     sync.Once
	closeErr error
}

func validBrokerSubject(subject string) bool {
	if subject == "" || len(subject) > 128 {
		return false
	}
	for _, token := range strings.Split(subject, ".") {
		if token == "" || strings.ContainsAny(token, "*> \t\r\n") {
			return false
		}
	}
	return true
}

// NewBrokerEndpoint subscribes to requests. Use a unique subject per broker instance.
func NewBrokerEndpoint(ctx context.Context, url, subject string, protocol ports.BrokerProtocol) (*BrokerEndpoint, error) {
	if !validBrokerSubject(subject) || protocol == nil {
		return nil, errors.New("broker NATS subject and protocol are required")
	}
	conn, err := natslib.Connect(url, natslib.Timeout(5*time.Second), natslib.MaxReconnects(5), natslib.ReconnectWait(500*time.Millisecond))
	if err != nil {
		return nil, fmt.Errorf("connect broker NATS endpoint: %w", err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	s := &BrokerEndpoint{conn: conn, ctx: runCtx, cancel: cancel, slots: make(chan struct{}, 64)}
	s.sub, err = conn.Subscribe(subject, func(message *natslib.Msg) { s.receive(message, protocol) })
	if err == nil {
		err = s.sub.SetPendingLimits(128, 64<<20)
	}
	if err == nil {
		readyCtx, readyCancel := context.WithTimeout(ctx, 5*time.Second)
		err = conn.FlushWithContext(readyCtx)
		readyCancel()
	}
	if err != nil {
		cancel()
		conn.Close()
		return nil, err
	}
	return s, nil
}

func (s *BrokerEndpoint) receive(message *natslib.Msg, protocol ports.BrokerProtocol) {
	if message.Reply == "" {
		return
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	select {
	case s.slots <- struct{}{}:
		s.workers.Add(1)
		s.mu.Unlock()
	default:
		s.mu.Unlock()
		s.respond(message, ports.BrokerResponse{Version: ports.BrokerProtocolVersion, Code: "unavailable"})
		return
	}
	go func() {
		defer s.workers.Done()
		defer func() { <-s.slots }()
		response := ports.BrokerResponse{Version: ports.BrokerProtocolVersion, Code: "invalid"}
		if len(message.Data) > 0 && len(message.Data) <= 1<<20 {
			var request ports.BrokerRequest
			decoder := json.NewDecoder(bytes.NewReader(message.Data))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&request); err == nil {
				if err := decoder.Decode(new(any)); errors.Is(err, io.EOF) {
					ctx, cancel := context.WithTimeout(s.ctx, 25*time.Second)
					response = protocol.Execute(ctx, request)
					cancel()
				}
			}
		}
		s.respond(message, response)
	}()
}

func (s *BrokerEndpoint) respond(message *natslib.Msg, response ports.BrokerResponse) {
	data, err := json.Marshal(response)
	if err == nil && int64(len(data)) > s.conn.MaxPayload() {
		data, err = json.Marshal(ports.BrokerResponse{Version: ports.BrokerProtocolVersion, ID: response.ID, Code: "too_large"})
	}
	if err == nil {
		err = message.Respond(data)
	}
	if err != nil {
		log.Printf("respond to broker NATS request: %v", err)
	}
}

// Serve waits for cancellation while the NATS subscription handles bounded work.
func (s *BrokerEndpoint) Serve(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return s.Close()
		case <-s.ctx.Done():
			return s.Close()
		case <-ticker.C:
			if s.conn.IsClosed() {
				return errors.Join(errors.New("broker NATS connection closed"), s.Close())
			}
		}
	}
}

// Close prevents new handlers, cancels outstanding requests and joins owned work.
func (s *BrokerEndpoint) Close() error {
	s.once.Do(func() {
		s.mu.Lock()
		s.closing = true
		s.mu.Unlock()
		s.cancel()
		s.closeErr = s.sub.Unsubscribe()
		s.workers.Wait()
		s.conn.Close()
	})
	return s.closeErr
}

// BrokerClient executes broker RPCs over a reusable NATS connection.
type BrokerClient struct {
	conn    *natslib.Conn
	subject string
	token   string
}

// NewBrokerClient connects only when NATS is explicitly selected.
func NewBrokerClient(url, subject, token string) (*BrokerClient, error) {
	if !validBrokerSubject(subject) {
		return nil, errors.New("invalid broker NATS subject")
	}
	conn, err := natslib.Connect(url, natslib.Timeout(5*time.Second), natslib.MaxReconnects(5))
	if err != nil {
		return nil, err
	}
	return &BrokerClient{conn: conn, subject: subject, token: token}, nil
}

// Call sends one request and verifies its response version and correlation ID.
func (c *BrokerClient) Call(ctx context.Context, request ports.BrokerRequest) (ports.BrokerResponse, error) {
	request.Version = ports.BrokerProtocolVersion
	request.Token = c.token
	if request.ID == "" {
		request.ID = uuid.NewString()
	}
	data, err := json.Marshal(request)
	if err != nil {
		return ports.BrokerResponse{}, err
	}
	if len(data) > 1<<20 || int64(len(data)) > c.conn.MaxPayload() {
		return ports.BrokerResponse{}, errors.New("broker NATS request exceeds 1 MiB")
	}
	callCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	message, err := c.conn.RequestWithContext(callCtx, c.subject, data)
	if err != nil {
		return ports.BrokerResponse{}, err
	}
	if len(message.Data) > 2<<20 {
		return ports.BrokerResponse{}, errors.New("broker NATS response exceeds 2 MiB")
	}
	var response ports.BrokerResponse
	if err := json.Unmarshal(message.Data, &response); err != nil {
		return response, err
	}
	if response.Version != ports.BrokerProtocolVersion || response.ID != request.ID {
		return response, errors.New("broker NATS response does not match request")
	}
	return response, nil
}

// Close releases the NATS connection.
func (c *BrokerClient) Close() error { c.conn.Close(); return nil }
