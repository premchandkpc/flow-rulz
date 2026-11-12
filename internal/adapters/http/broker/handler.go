// Package broker translates HTTP requests to shared broker operations.
package broker

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	runtimebroker "github.com/flowrulz/flow-rulz/internal/runtime/broker"
	brokerservice "github.com/flowrulz/flow-rulz/internal/services/broker"
)

type handler struct {
	protocol ports.BrokerProtocol
	slots    chan struct{}
}

// NewHandler preserves the existing embedded HTTP entry point.
func NewHandler(b *runtimebroker.Broker, token string) http.Handler {
	return NewProtocolHandler(brokerservice.New(b, token))
}

// NewProtocolHandler exposes the same operations as the TCP and NATS adapters.
func NewProtocolHandler(protocol ports.BrokerProtocol) http.Handler {
	h := &handler{protocol: protocol, slots: make(chan struct{}, 64)}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /v1/broker/events", h.publish)
	mux.HandleFunc("POST /v1/events/{eventType}", h.publish)
	mux.HandleFunc("POST /v1/broker/messages", h.append)
	mux.HandleFunc("POST /v1/broker/consume", h.consume)
	mux.HandleFunc("POST /v1/broker/ack", h.settle)
	mux.HandleFunc("POST /v1/broker/retry", h.settle)
	mux.HandleFunc("POST /v1/broker/rpc", h.rpc)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			mux.ServeHTTP(w, r)
			return
		}
		select {
		case h.slots <- struct{}{}:
			defer func() { <-h.slots }()
		default:
			http.Error(w, "broker request capacity reached", http.StatusTooManyRequests)
			return
		}
		mux.ServeHTTP(w, r)
	})
}

func decode(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		http.Error(w, "invalid JSON request", http.StatusBadRequest)
		return false
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		http.Error(w, "expected one JSON document", http.StatusBadRequest)
		return false
	}
	return true
}

func requestFor(r *http.Request, operation string) ports.BrokerRequest {
	var token string
	if authorization := r.Header.Get("Authorization"); strings.HasPrefix(authorization, "Bearer ") {
		token = strings.TrimPrefix(authorization, "Bearer ")
	}
	return ports.BrokerRequest{Version: ports.BrokerProtocolVersion, Operation: operation, TenantID: r.Header.Get("X-Tenant-ID"), Token: token}
}

func (h *handler) execute(w http.ResponseWriter, r *http.Request, request ports.BrokerRequest) {
	response := h.protocol.Execute(r.Context(), request)
	var status int
	switch response.Code {
	case "accepted":
		status = http.StatusAccepted
	case "message":
		status = http.StatusOK
	case "empty", "ok":
		w.WriteHeader(http.StatusNoContent)
		return
	case "invalid":
		status = http.StatusBadRequest
	case "unauthorized":
		status = http.StatusUnauthorized
	case "forbidden":
		status = http.StatusForbidden
	case "conflict":
		status = http.StatusConflict
	default:
		status = http.StatusServiceUnavailable
	}
	if status >= 400 {
		http.Error(w, response.Code, status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(response.Data); err != nil {
		log.Printf("write broker response: %v", err)
	}
}

func (h *handler) publish(w http.ResponseWriter, r *http.Request) {
	var event domain.EventEnvelope
	if !decode(w, r, &event) {
		return
	}
	if eventType := r.PathValue("eventType"); eventType != "" && event.Type != eventType {
		http.Error(w, "event type mismatch", http.StatusBadRequest)
		return
	}
	request := requestFor(r, "publish")
	request.Event = &event
	h.execute(w, r, request)
}

type appendRequest struct {
	Topic   string          `json:"topic"`
	Key     string          `json:"key"`
	Payload json.RawMessage `json:"payload"`
}

func (h *handler) append(w http.ResponseWriter, r *http.Request) {
	var body appendRequest
	if !decode(w, r, &body) {
		return
	}
	request := requestFor(r, "append")
	request.Topic, request.Key, request.Payload = body.Topic, body.Key, body.Payload
	h.execute(w, r, request)
}

type consumeRequest struct {
	Topic  string `json:"topic"`
	Group  string `json:"group"`
	WaitMS int    `json:"wait_ms"`
}

func (h *handler) consume(w http.ResponseWriter, r *http.Request) {
	var body consumeRequest
	if !decode(w, r, &body) {
		return
	}
	request := requestFor(r, "consume")
	request.Topic, request.Group, request.WaitMS = body.Topic, body.Group, body.WaitMS
	h.execute(w, r, request)
}

type settleRequest struct {
	Topic     string `json:"topic"`
	Group     string `json:"group"`
	Partition uint32 `json:"partition"`
	Receipt   string `json:"receipt"`
	DelayMS   int    `json:"delay_ms,omitempty"`
}

func (h *handler) settle(w http.ResponseWriter, r *http.Request) {
	var body settleRequest
	if !decode(w, r, &body) {
		return
	}
	operation := "ack"
	if r.URL.Path == "/v1/broker/retry" {
		operation = "retry"
	}
	request := requestFor(r, operation)
	request.Topic, request.Group, request.Partition, request.Receipt, request.DelayMS = body.Topic, body.Group, body.Partition, body.Receipt, body.DelayMS
	h.execute(w, r, request)
}

func (h *handler) rpc(w http.ResponseWriter, r *http.Request) {
	var request ports.BrokerRequest
	if !decode(w, r, &request) {
		return
	}
	response := h.protocol.Execute(r.Context(), request)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("write broker RPC response: %v", err)
	}
}
