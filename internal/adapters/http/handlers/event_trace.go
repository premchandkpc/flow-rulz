package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

const (
	maxTraceDepth = 5
	maxTraceNodes = 100
)

// EventTraceHandler handles HTTP requests for event tracing.
type EventTraceHandler struct {
	inbox      ports.InboxRepository
	executions ports.ExecutionRepository
	outbox     ports.OutboxReader
	numShards  uint32
}

// EventTrace represents the trace of an event through the system.
type EventTrace struct {
	EventID      string          `json:"event_id"`
	Type         string          `json:"type"`
	PartitionKey string          `json:"partition_key"`
	Shard        uint32          `json:"shard"`
	Status       string          `json:"status"`
	Execution    *TraceExecution `json:"execution,omitempty"`
	Effects      []TraceEffect   `json:"effects"`
	Truncated    bool            `json:"truncated,omitempty"`
}

// TraceExecution represents an execution in an event trace.
type TraceExecution struct {
	RuleSet     string                 `json:"rule_set"`
	Revision    int64                  `json:"revision"`
	Status      domain.ExecutionStatus `json:"status"`
	CreatedAt   string                 `json:"created_at"`
	CompletedAt *string                `json:"completed_at,omitempty"`
}

// TraceEffect represents an effect in an event trace.
type TraceEffect struct {
	EffectID    string              `json:"effect_id"`
	Type        domain.EffectType   `json:"effect_type"`
	Name        string              `json:"name"`
	Destination string              `json:"destination"`
	Status      domain.OutboxStatus `json:"status"`
	Child       *EventTrace         `json:"child,omitempty"`
}

type traceBudget struct {
	remaining int
	truncated bool
}

// NewEventTraceHandler creates a new EventTraceHandler.
func NewEventTraceHandler(inbox ports.InboxRepository, executions ports.ExecutionRepository, outbox ports.OutboxReader, numShards uint32) *EventTraceHandler {
	return &EventTraceHandler{
		inbox: inbox, executions: executions, outbox: outbox, numShards: numShards,
	}
}

// RegisterRoutes registers the HTTP routes for the EventTraceHandler.
func (h *EventTraceHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/events/{eventID}/trace", h.GetTrace)
}

// GetTrace handles GET /v1/events/{eventID}/trace requests.
func (h *EventTraceHandler) GetTrace(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	tenantID := middleware.GetTenantID(r.Context())
	if eventID == "" || tenantID == "" {
		responses.WriteError(w, http.StatusBadRequest, "event ID and tenant are required", responses.CodeBadRequest)
		return
	}
	if h.inbox == nil || h.executions == nil || h.outbox == nil {
		responses.WriteError(w, http.StatusServiceUnavailable, "event tracing is not configured", responses.CodeInternalError)
		return
	}

	budget := &traceBudget{remaining: maxTraceNodes}
	trace, err := h.traceEvent(r.Context(), tenantID, eventID, 0, budget)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	trace.Truncated = budget.truncated
	responses.WriteOK(w, trace)
}

func (h *EventTraceHandler) traceEvent(ctx context.Context, tenantID, eventID string, depth int, budget *traceBudget) (*EventTrace, error) {
	if budget.remaining <= 0 || depth > maxTraceDepth {
		budget.truncated = true
		return nil, nil
	}
	budget.remaining--

	trace := &EventTrace{
		EventID: eventID,
		Status:  "queued",
		Effects: make([]TraceEffect, 0),
	}
	inboxEntry, err := h.inbox.Get(ctx, tenantID, eventID)
	if err != nil {
		return nil, fmt.Errorf("get inbox entry for trace event %q: %w", eventID, err)
	}
	if inboxEntry == nil {
		return trace, nil
	}
	trace.Type = inboxEntry.RuleSet
	trace.PartitionKey = inboxEntry.PartitionKey
	if h.numShards > 0 && trace.PartitionKey != "" {
		trace.Shard = (&domain.EventEnvelope{TenantID: tenantID, PartitionKey: trace.PartitionKey}).VirtualShard(h.numShards)
	}
	trace.Status = string(inboxEntry.Status)
	if inboxEntry.ExecutionID == "" {
		return trace, nil
	}

	execution, err := h.executions.Get(ctx, inboxEntry.ExecutionID)
	if err != nil {
		return nil, fmt.Errorf("get execution for trace event %q: %w", eventID, err)
	}
	if execution == nil || execution.TenantID != tenantID {
		return trace, nil
	}
	trace.Status = string(execution.Status)
	trace.Execution = &TraceExecution{
		RuleSet: execution.RuleSet, Revision: execution.Revision, Status: execution.Status,
		CreatedAt: execution.CreatedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00"),
	}
	if execution.CompletedAt != nil {
		completedAt := execution.CompletedAt.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
		trace.Execution.CompletedAt = &completedAt
	}

	effects, err := h.outbox.GetByExecution(ctx, execution.ID)
	if err != nil {
		return nil, fmt.Errorf("get outbox effects for trace event %q: %w", eventID, err)
	}
	for _, effect := range effects {
		traceEffect := TraceEffect{
			EffectID: effect.ID, Type: effect.EffectType, Name: effect.Name,
			Destination: effect.Destination, Status: effect.Status,
		}
		if effect.EffectType == domain.EffectTypeEmitEvent {
			var child domain.EventEnvelope
			if err := json.Unmarshal(effect.Payload, &child); err != nil {
				return nil, fmt.Errorf("decode child event for trace effect %q: %w", effect.ID, err)
			}
			if child.TenantID != tenantID {
				return nil, fmt.Errorf("trace effect %q crossed tenant boundary", effect.ID)
			}
			traceEffect.Child, err = h.traceEvent(ctx, tenantID, child.ID, depth+1, budget)
			if err != nil {
				return nil, err
			}
			if traceEffect.Child != nil {
				traceEffect.Child.Type = child.Type
				traceEffect.Child.PartitionKey = child.PartitionKey
				traceEffect.Child.Shard = child.VirtualShard(h.numShards)
			}
		}
		trace.Effects = append(trace.Effects, traceEffect)
	}
	return trace, nil
}
