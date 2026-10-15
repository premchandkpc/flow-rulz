package handlers

import (
	"encoding/json"
	"fmt"
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
	ID         string                     `json:"id"`
	Type       string                     `json:"type"`
	OccurredAt time.Time                  `json:"occurred_at"`
	Data       json.RawMessage            `json:"data"`
	Contract   *domain.ContractRef        `json:"contract,omitempty"`
	Meta       map[string]json.RawMessage `json:"meta"`
}

func (r eventIngestionRequest) metaString(key string) string {
	var value string
	_ = json.Unmarshal(r.Meta[key], &value)
	return value
}

func validateEventMetadata(meta map[string]json.RawMessage) error {
	for _, reserved := range []string{"flow.service_id", "flow.deployment_revision"} {
		if _, exists := meta[reserved]; exists {
			return fmt.Errorf("meta.%s is reserved for trusted routing infrastructure", reserved)
		}
	}
	stringFields := []string{"tenant_id", "partition_key", "workflow_id", "message_kind", "correlation_id", "causation_id", "traceparent"}
	for _, key := range stringFields {
		raw, exists := meta[key]
		if !exists {
			continue
		}
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return fmt.Errorf("meta.%s must be a string", key)
		}
	}
	if raw, exists := meta["headers"]; exists {
		var headers map[string]string
		if err := json.Unmarshal(raw, &headers); err != nil {
			return fmt.Errorf("meta.headers must be an object with string values")
		}
	}
	for _, key := range []string{"hop"} {
		if raw, exists := meta[key]; exists {
			var value int64
			if err := json.Unmarshal(raw, &value); err != nil || value < 0 {
				return fmt.Errorf("meta.%s must be a non-negative integer", key)
			}
		}
	}
	return nil
}

func (r eventIngestionRequest) metaHeaders() map[string]string {
	var headers map[string]string
	_ = json.Unmarshal(r.Meta["headers"], &headers)
	return headers
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

	event := buildEventEnvelope(request, eventType)
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
		event.MessageKind = request.metaString("message_kind")
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
	if err := validateEventMetadata(request.Meta); err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return false
	}
	if tenantID == "" {
		responses.WriteError(w, http.StatusUnauthorized, messageTenantRequired, responses.CodeTenantRequired)
		return false
	}
	eventTenantID := request.metaString("tenant_id")
	if eventTenantID == "" {
		responses.WriteError(w, http.StatusBadRequest, "meta.tenant_id is required", responses.CodeValidationFailed)
		return false
	}
	if request.metaString("partition_key") == "" {
		responses.WriteError(w, http.StatusBadRequest, "meta.partition_key is required", responses.CodeValidationFailed)
		return false
	}
	if eventTenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, "meta.tenant_id does not match authenticated event scope", responses.CodeForbidden)
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
) *domain.EventEnvelope {
	headers := buildEventHeaders(request)
	return &domain.EventEnvelope{
		ID: strings.TrimSpace(request.ID), Type: eventType,
		TenantID:     strings.TrimSpace(request.metaString("tenant_id")),
		PartitionKey: strings.TrimSpace(request.metaString("partition_key")),
		WorkflowID:   request.metaString("workflow_id"),
		OccurredAt:   request.OccurredAt, Data: append(json.RawMessage(nil), request.Data...),
		Headers: headers, Meta: unknownEventMetadata(request.Meta),
		Contract: request.Contract, MessageKind: request.metaString("message_kind"),
		CorrelationID: request.metaString("correlation_id"), CausationID: request.metaString("causation_id"),
	}
}

func unknownEventMetadata(source map[string]json.RawMessage) map[string]json.RawMessage {
	clone := make(map[string]json.RawMessage, len(source))
	for key, value := range source {
		clone[key] = append(json.RawMessage(nil), value...)
	}
	for _, key := range []string{"tenant_id", "partition_key", "workflow_id", "message_kind", "correlation_id", "causation_id", "headers"} {
		delete(clone, key)
	}
	return clone
}

func buildEventHeaders(request eventIngestionRequest) map[string]string {
	metaHeaders := request.metaHeaders()
	headers := make(map[string]string, len(metaHeaders)+3)
	for key, value := range metaHeaders {
		headers[key] = value
	}
	if value := request.metaString("traceparent"); value != "" {
		headers["traceparent"] = value
	}
	if value := request.metaString("correlation_id"); value != "" {
		headers["Correlation-ID"] = value
	}
	if value := request.metaString("causation_id"); value != "" {
		headers["Causation-ID"] = value
	}
	if value := request.metaString("schema_version"); value != "" {
		headers["Schema-Version"] = value
	}
	return headers
}

func writeEventAcceptedResponse(w http.ResponseWriter, eventID string, shard uint32) {
	responses.WriteSuccess(w, http.StatusAccepted, map[string]interface{}{
		"event_id": eventID, "status": "accepted", "shard": shard,
	})
}
