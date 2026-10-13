package handlers

import (
	"net/http"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// ScheduledHandler handles HTTP requests for scheduled operations.
type ScheduledHandler struct {
	repo ports.ScheduledEventRepository
}

// NewScheduledHandler constructs an HTTP handler with its required service dependencies.
func NewScheduledHandler(repo ports.ScheduledEventRepository) *ScheduledHandler {
	return &ScheduledHandler{repo: repo}
}

// RegisterRoutes registers the handler's HTTP routes on mux.
func (h *ScheduledHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/scheduled-events", h.ScheduleEvent)
	mux.HandleFunc("GET /v1/scheduled-events/{eventID}", h.GetScheduledEvent)
	mux.HandleFunc("GET /v1/scheduled-events", h.ListScheduledEvents)
	mux.HandleFunc("DELETE /v1/scheduled-events/{eventID}", h.CancelScheduledEvent)
}

// ScheduleEvent validates and stores a scheduled event.
func (h *ScheduledHandler) ScheduleEvent(w http.ResponseWriter, r *http.Request) {
	var evt domain.ScheduledEvent
	if err := decodeRequestBody(r, &evt); err != nil {
		responses.WriteError(w, http.StatusBadRequest, messageInvalidBodyPrefix+err.Error(), responses.CodeInvalidJSON)
		return
	}

	tenantID := middleware.GetTenantID(r.Context())
	if evt.TenantID != "" && evt.TenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, messageTenantMismatch, responses.CodeForbidden)
		return
	}
	evt.TenantID = tenantID
	if evt.EventType == "" || evt.PartitionKey == "" {
		responses.WriteError(w, http.StatusBadRequest, "event_type and partition_key are required", responses.CodeValidationFailed)
		return
	}
	if evt.ScheduledAt.IsZero() {
		responses.WriteError(w, http.StatusBadRequest, "scheduled_at is required", responses.CodeValidationFailed)
		return
	}
	if evt.EventID == "" {
		evt.EventID = domain.NewID()
	}

	evt.Status = domain.ScheduledEventStatusPending
	evt.CreatedAt = time.Now().UTC()
	if err := h.repo.Save(r.Context(), &evt); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	responses.WriteCreated(w, evt)
}

// GetScheduledEvent returns a scheduled event visible to the current tenant.
func (h *ScheduledHandler) GetScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		responses.WriteError(w, http.StatusBadRequest, "eventID is required", responses.CodeBadRequest)
		return
	}

	evt, err := h.repo.Get(r.Context(), middleware.GetTenantID(r.Context()), eventID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if evt == nil {
		responses.WriteError(w, http.StatusNotFound, messageNotFound, responses.CodeNotFound)
		return
	}
	responses.WriteOK(w, evt)
}

// ListScheduledEvents lists scheduled events for the current tenant.
func (h *ScheduledHandler) ListScheduledEvents(w http.ResponseWriter, r *http.Request) {
	pagination, err := parsePagination(r, defaultPageLimit, maxPageLimit, maxPageOffset)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}
	limit, offset := pagination.Limit, pagination.Offset

	events, err := h.repo.List(r.Context(), middleware.GetTenantID(r.Context()), limit, offset)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	responses.WriteOK(w, map[string]interface{}{
		"events": events,
		"total":  len(events),
		"limit":  limit,
		"offset": offset,
	})
}

// CancelScheduledEvent cancels a pending scheduled event.
func (h *ScheduledHandler) CancelScheduledEvent(w http.ResponseWriter, r *http.Request) {
	eventID := r.PathValue("eventID")
	if eventID == "" {
		responses.WriteError(w, http.StatusBadRequest, "eventID is required", responses.CodeBadRequest)
		return
	}
	if err := h.repo.Cancel(r.Context(), middleware.GetTenantID(r.Context()), eventID); err != nil {
		responses.WriteError(w, http.StatusConflict, "event was not found for this tenant or is no longer pending", responses.CodeConflict)
		return
	}
	responses.WriteNoContent(w)
}
