package handlers

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// EventIngestionHandler handles HTTP requests for event ingestion.
type EventIngestionHandler struct {
	publisher ports.BrokerPublisher
	numShards uint32
	contracts ports.ContractRegistry
}

// NewEventIngestionHandler creates a new EventIngestionHandler.
func NewEventIngestionHandler(publisher ports.BrokerPublisher, numShards uint32) *EventIngestionHandler {
	if numShards == 0 {
		numShards = config.DefaultNumShards
	}
	return &EventIngestionHandler{publisher: publisher, numShards: numShards}
}

// WithContractRegistry configures contract validation for incoming events.
func (h *EventIngestionHandler) WithContractRegistry(registry ports.ContractRegistry) *EventIngestionHandler {
	h.contracts = registry
	return h
}

// RegisterRoutes registers the HTTP routes for the EventIngestionHandler.
func (h *EventIngestionHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/events/{eventType}", h.PublishEvent)
}

type eventIngestionRequest struct {
	ID            string              `json:"id"`
	Type          string              `json:"type"`
	Meta          map[string]any     `json:"meta"`
	WorkflowID    string              `json:"workflow_id,omitempty"`
	OccurredAt    time.Time           `json:"occurred_at"`
	Data          json.RawMessage     `json:"data"`
	Headers       map[string]string   `json:"headers,omitempty"`
	CorrelationID string              `json:"correlation_id,omitempty"`
	CausationID   string              `json:"causation_id,omitempty"`
	SchemaVersion string              `json:"schema_version,omitempty"`
	Contract      *domain.ContractRef `json:"contract,omitempty"`
	MessageKind   string              `json:"message_kind,omitempty"`
}

// PublishEvent handles POST /v1/events/{eventType} requests.
func (h *EventIngestionHandler) PublishEvent(w http.ResponseWriter, r *http.Request) {
	if h.publisher == nil {
		responses.WriteError(w, http.StatusServiceUnavailable, "event ingestion is not configured", responses.CodeInternalError)
		return
	}
	request, ok := decodeEventIngestionRequest(w, r)
	if !ok {
		return
	}
	eventType := strings.TrimSpace(r.PathValue("eventType"))
	tenantID := middleware.GetTenantID(r.Context())
	if !validateEventIngestionRequest(w, request, eventType, tenantID) {
		return
	}

	event := buildEventEnvelope(request, eventType, tenantID)
	if request.Contract != nil {
		if h.contracts == nil {
			responses.WriteError(w, http.StatusServiceUnavailable, "contract registry is not configured", responses.CodeInternalError)
			return
		}
		if request.Contract.Name != eventType {
			responses.WriteError(w, http.StatusBadRequest, "contract name must match event type", responses.CodeValidationFailed)
			return
		}
		schema, err := h.contracts.Get(r.Context(), request.Contract.Name, request.Contract.Version)
		if err != nil {
			responses.WriteError(w, http.StatusServiceUnavailable, "contract lookup failed", responses.CodeInternalError)
			return
		}
		if schema == nil {
			responses.WriteError(w, http.StatusBadRequest, "event contract not found", responses.CodeValidationFailed)
			return
		}
		if schema.Deprecated {
			responses.WriteError(w, http.StatusBadRequest, "event contract is deprecated", responses.CodeValidationFailed)
			return
		}
		if err := schema.NormalizeAndValidate(); err != nil {
			responses.WriteError(w, http.StatusInternalServerError, "registered event contract is invalid", responses.CodeInternalError)
			return
		}
		if request.Contract.SchemaHash != "" && request.Contract.SchemaHash != schema.SchemaHash {
			responses.WriteError(w, http.StatusBadRequest, "event contract schema hash mismatch", responses.CodeValidationFailed)
			return
		}
		if err := schema.ValidatePayload(event.Data); err != nil {
			responses.WriteError(w, http.StatusBadRequest, "event payload does not match contract: "+err.Error(), responses.CodeValidationFailed)
			return
		}
		event.Contract = &domain.ContractRef{Name: schema.Name, Version: schema.Version, SchemaHash: schema.SchemaHash}
		event.MessageKind = request.MessageKind
		if event.MessageKind == "" {
			event.MessageKind = "event"
		}
	}
	if err := event.Validate(); err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}
	payload, err := json.Marshal(event)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, "failed to encode event envelope", responses.CodeInternalError)
		return
	}
	shard := event.VirtualShard(h.numShards)
	if err := h.publisher.PublishToShard(r.Context(), "events", shard, payload); err != nil {
		responses.WriteError(w, http.StatusServiceUnavailable, "event publication failed", responses.CodeInternalError)
		return
	}
	writeEventAcceptedResponse(w, event.ID, shard)
}

func decodeEventIngestionRequest(w http.ResponseWriter, r *http.Request) (eventIngestionRequest, bool) {
	var request eventIngestionRequest
	if err := decodeRequestBody(r, &request); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid event envelope: "+err.Error(), responses.CodeInvalidJSON)
		return eventIngestionRequest{}, false
	}
	return request, true
}

func validateEventIngestionRequest(
	w http.ResponseWriter,
	request eventIngestionRequest,
	eventType string,
	tenantID string,
) bool {
	if eventType == "" || request.Type != eventType {
		responses.WriteError(w, http.StatusBadRequest, "event type must match the eventType path parameter", responses.CodeValidationFailed)
		return false
	}
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, messageTenantRequired, responses.CodeTenantRequired)
		return false
	}
	metaTenantID, ok := request.Meta["tenant_id"].(string)
	if !ok || strings.TrimSpace(metaTenantID) == "" {
		responses.WriteError(w, http.StatusBadRequest, "meta.tenant_id is required", responses.CodeValidationFailed)
		return false
	}
	if metaTenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, "meta.tenant_id does not match authenticated tenant", responses.CodeForbidden)
		return false
	}
	partitionKey, ok := request.Meta["partition_key"].(string)
	if !ok || strings.TrimSpace(partitionKey) == "" {
		responses.WriteError(w, http.StatusBadRequest, "meta.partition_key is required", responses.CodeValidationFailed)
		return false
	}
	if len(request.Data) == 0 || !json.Valid(request.Data) {
		responses.WriteError(w, http.StatusBadRequest, "data must contain a valid JSON value", responses.CodeValidationFailed)
		return false
	}
	return true
}

func buildEventEnvelope(
	request eventIngestionRequest,
	eventType string,
	tenantID string,
) *domain.EventEnvelope {
	headers := buildEventHeaders(request)
	return &domain.EventEnvelope{
		ID: strings.TrimSpace(request.ID), Type: eventType, TenantID: tenantID,
		PartitionKey: strings.TrimSpace(request.Meta["partition_key"].(string)), WorkflowID: request.WorkflowID,
		OccurredAt: request.OccurredAt, Data: request.Data, Headers: headers,
		Contract: request.Contract, MessageKind: request.MessageKind, CorrelationID: request.CorrelationID, CausationID: request.CausationID,
		Meta: request.Meta,
	}
}

func buildEventHeaders(request eventIngestionRequest) map[string]string {
	headers := make(map[string]string, len(request.Headers)+3)
	for key, value := range request.Headers {
		headers[key] = value
	}
	if request.CorrelationID != "" {
		headers["Correlation-ID"] = request.CorrelationID
	}
	if request.CausationID != "" {
		headers["Causation-ID"] = request.CausationID
	}
	if request.SchemaVersion != "" {
		headers["Schema-Version"] = request.SchemaVersion
	}
	return headers
}

func writeEventAcceptedResponse(w http.ResponseWriter, eventID string, shard uint32) {
	responses.WriteSuccess(w, http.StatusAccepted, map[string]interface{}{
		"event_id": eventID, "status": "accepted", "shard": shard,
	})
}
