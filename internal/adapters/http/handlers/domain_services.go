package handlers

import (
	"errors"
	"net/http"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/services/domainservices"
)

// DomainServiceHandler handles HTTP requests for domain and service operations.
type DomainServiceHandler struct {
	service ports.DomainServiceUseCase
}

// NewDomainServiceHandler creates a new DomainServiceHandler.
func NewDomainServiceHandler(service ports.DomainServiceUseCase) *DomainServiceHandler {
	return &DomainServiceHandler{service: service}
}

// RegisterRoutes registers the HTTP routes for the DomainServiceHandler.
func (h *DomainServiceHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/domains", h.ListDomains)
	mux.HandleFunc("POST /v1/domains", h.CreateDomain)
	mux.HandleFunc("GET /v1/domains/{domainID}", h.GetDomain)
	mux.HandleFunc("PUT /v1/domains/{domainID}", h.UpdateDomain)
	mux.HandleFunc("DELETE /v1/domains/{domainID}", h.DeleteDomain)
	mux.HandleFunc("GET /v1/services", h.ListServices)
	mux.HandleFunc("POST /v1/services", h.CreateService)
	mux.HandleFunc("GET /v1/services/{serviceID}", h.GetService)
	mux.HandleFunc("PUT /v1/services/{serviceID}", h.UpdateService)
	mux.HandleFunc("DELETE /v1/services/{serviceID}", h.DeleteService)
}

// ListDomains handles GET /v1/domains requests.
func (h *DomainServiceHandler) ListDomains(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	items, err := h.service.ListDomains(r.Context(), middleware.GetTenantID(r.Context()))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, map[string]any{"domains": items, "total": len(items)})
}

// CreateDomain handles POST /v1/domains requests.
func (h *DomainServiceHandler) CreateDomain(w http.ResponseWriter, r *http.Request) {
	var item domain.TenantDomain
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	saved, err := h.service.SaveDomain(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteCreated(w, saved)
}

// GetDomain handles GET /v1/domains/{domainID} requests.
func (h *DomainServiceHandler) GetDomain(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	item, err := h.service.GetDomain(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("domainID"))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, item)
}

// UpdateDomain handles PUT /v1/domains/{domainID} requests.
func (h *DomainServiceHandler) UpdateDomain(w http.ResponseWriter, r *http.Request) {
	var item domain.TenantDomain
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	item.ID = r.PathValue("domainID")
	saved, err := h.service.SaveDomain(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, saved)
}

// DeleteDomain handles DELETE /v1/domains/{domainID} requests.
func (h *DomainServiceHandler) DeleteDomain(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	if err := h.service.DeleteDomain(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("domainID")); err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteNoContent(w)
}

// ListServices handles GET /v1/services requests.
func (h *DomainServiceHandler) ListServices(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	items, err := h.service.ListServices(r.Context(), middleware.GetTenantID(r.Context()))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, map[string]any{"services": items, "total": len(items)})
}

// CreateService handles POST /v1/services requests.
func (h *DomainServiceHandler) CreateService(w http.ResponseWriter, r *http.Request) {
	var item domain.DomainService
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	saved, err := h.service.SaveService(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteCreated(w, saved)
}

// GetService handles GET /v1/services/{serviceID} requests.
func (h *DomainServiceHandler) GetService(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	item, err := h.service.GetService(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("serviceID"))
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, item)
}

// UpdateService handles PUT /v1/services/{serviceID} requests.
func (h *DomainServiceHandler) UpdateService(w http.ResponseWriter, r *http.Request) {
	var item domain.DomainService
	if !h.decode(w, r, &item) {
		return
	}
	if !h.ready(w, r) {
		return
	}
	item.ID = r.PathValue("serviceID")
	saved, err := h.service.SaveService(r.Context(), middleware.GetTenantID(r.Context()), &item)
	if err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteOK(w, saved)
}

// DeleteService handles DELETE /v1/services/{serviceID} requests.
func (h *DomainServiceHandler) DeleteService(w http.ResponseWriter, r *http.Request) {
	if !h.ready(w, r) {
		return
	}
	if err := h.service.DeleteService(r.Context(), middleware.GetTenantID(r.Context()), r.PathValue("serviceID")); err != nil {
		writeDomainServiceError(w, err)
		return
	}
	responses.WriteNoContent(w)
}

func (h *DomainServiceHandler) ready(w http.ResponseWriter, r *http.Request) bool {
	if h.service == nil {
		responses.WriteError(w, http.StatusServiceUnavailable, "domain and service management is not configured", responses.CodeInternalError)
		return false
	}
	if middleware.GetTenantID(r.Context()) == "" {
		responses.WriteError(w, http.StatusUnauthorized, "authenticated tenant identity is required", responses.CodeTenantRequired)
		return false
	}
	return true
}

func (h *DomainServiceHandler) decode(w http.ResponseWriter, r *http.Request, item any) bool {
	if err := decodeRequestBody(r, item); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return false
	}
	return true
}

func writeDomainServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domainservices.ErrInvalidInput):
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
	case errors.Is(err, domainservices.ErrConflict):
		responses.WriteError(w, http.StatusConflict, err.Error(), responses.CodeConflict)
	case errors.Is(err, domainservices.ErrNotFound):
		responses.WriteError(w, http.StatusNotFound, err.Error(), responses.CodeNotFound)
	default:
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
	}
}
