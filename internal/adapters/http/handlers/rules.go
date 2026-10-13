package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// RuleHandler handles HTTP requests for rule operations.
type RuleHandler struct {
	ruleSvc ports.RuleService
}

// NewRuleHandler constructs an HTTP handler with its required service dependencies.
func NewRuleHandler(ruleSvc ports.RuleService) *RuleHandler {
	return &RuleHandler{ruleSvc: ruleSvc}
}

// RegisterRoutes registers the handler's HTTP routes on mux.
func (h *RuleHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/rules/check", h.CheckRule)
	mux.HandleFunc("POST /v1/rules/{ruleSet}/revisions", h.CreateRevision)
	mux.HandleFunc("GET /v1/rules/{ruleSet}/revisions", h.ListRevisions)
	mux.HandleFunc("GET /v1/rules/{ruleSet}/revisions/{revision}", h.GetRevision)
	mux.HandleFunc("DELETE /v1/rules/{ruleSet}/revisions/{revision}", h.Deactivate)
	mux.HandleFunc("GET /v1/rules/active", h.ListActiveRules)
}

// CreateRevision compiles and activates a rule revision from one JSON or YAML document.
func (h *RuleHandler) CreateRevision(w http.ResponseWriter, r *http.Request) {
	ruleSet := r.PathValue("ruleSet")
	if ruleSet == "" {
		responses.WriteError(w, http.StatusBadRequest, "ruleSet is required", responses.CodeBadRequest)
		return
	}

	source, err := decodeRawRequestBody(r)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid rule source: "+err.Error(), responses.CodeInvalidJSON)
		return
	}
	var metadata struct {
		RuleSet  string `json:"rule_set"`
		Revision int64  `json:"revision"`
	}
	if err := json.Unmarshal(source, &metadata); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid rule source metadata: "+err.Error(), responses.CodeInvalidJSON)
		return
	}
	if metadata.RuleSet == "" || metadata.RuleSet != ruleSet {
		responses.WriteError(w, http.StatusBadRequest, "rule_set must match the ruleSet path parameter", responses.CodeValidationFailed)
		return
	}
	if metadata.Revision <= 0 {
		responses.WriteError(w, http.StatusBadRequest, "revision must be greater than zero", responses.CodeValidationFailed)
		return
	}

	activation, err := h.ruleSvc.CompileAndActivate(r.Context(), middleware.GetTenantID(r.Context()), ruleSet, source, "api")
	if err != nil {
		responses.WriteError(w, http.StatusUnprocessableEntity, err.Error(), responses.CodeValidationFailed)
		return
	}
	responses.WriteCreated(w, activation)
}

// ListRevisions lists revisions visible to the current tenant.
func (h *RuleHandler) ListRevisions(w http.ResponseWriter, r *http.Request) {
	ruleSet := r.PathValue("ruleSet")
	tenantScope := middleware.GetTenantID(r.Context())

	ctx := r.Context()
	revisions, err := h.ruleSvc.ListRevisions(ctx, tenantScope)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	// Filter by ruleSet if provided
	if ruleSet != "" {
		filtered := make([]*domain.RuleRevision, 0)
		for _, rev := range revisions {
			if rev.RuleID == ruleSet {
				filtered = append(filtered, rev)
			}
		}
		revisions = filtered
	}

	responses.WriteOK(w, map[string]interface{}{
		"revisions": revisions,
		"total":     len(revisions),
	})
}

// GetRevision returns a specific rule revision.
func (h *RuleHandler) GetRevision(w http.ResponseWriter, r *http.Request) {
	ruleSet := r.PathValue("ruleSet")
	revisionStr := r.PathValue("revision")
	tenantScope := middleware.GetTenantID(r.Context())

	revision, err := strconv.ParseInt(revisionStr, 10, 64)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid revision", responses.CodeInvalidVersion)
		return
	}

	ctx := r.Context()
	rev, err := h.ruleSvc.GetRevision(ctx, tenantScope, ruleSet, revision)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if rev == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, rev)
}

// Deactivate deactivates the requested rule set.
func (h *RuleHandler) Deactivate(w http.ResponseWriter, r *http.Request) {
	ruleSet := r.PathValue("ruleSet")
	tenantScope := middleware.GetTenantID(r.Context())

	ctx := r.Context()
	if err := h.ruleSvc.Deactivate(ctx, tenantScope, ruleSet); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, map[string]string{"status": "deactivated"})
}

// ListActiveRules lists active rules for the current tenant.
func (h *RuleHandler) ListActiveRules(w http.ResponseWriter, r *http.Request) {
	tenantScope := middleware.GetTenantID(r.Context())

	ctx := r.Context()
	active, err := h.ruleSvc.ListActiveRules(ctx, tenantScope)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, map[string]interface{}{
		"active_rules": active,
		"total":        len(active),
	})
}

// CheckRule validates a rule source and optionally evaluates it against sample event
// data. The body is JSON: {"source": "<YAML or JSON rule document>", "event_type": "...", "event": {...}}.
func (h *RuleHandler) CheckRule(w http.ResponseWriter, r *http.Request) {
	checker, ok := h.ruleSvc.(ports.RuleChecker)
	if !ok {
		responses.WriteError(w, http.StatusNotImplemented, "rule checking is not available", responses.CodeInternalError)
		return
	}
	body, err := readRequestBody(r)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}
	var req struct {
		Source    string          `json:"source"`
		EventType string          `json:"event_type"`
		Event     json.RawMessage `json:"event"`
	}
	if err := json.Unmarshal(body, &req); err != nil || req.Source == "" {
		responses.WriteError(w, http.StatusBadRequest, "body must be JSON with a non-empty source", responses.CodeValidationFailed)
		return
	}
	var document any
	if err := decodeSingleYAMLDocument([]byte(req.Source), &document); err != nil {
		responses.WriteOK(w, &ports.RuleCheckResult{Error: "source is not valid YAML or JSON: " + err.Error()})
		return
	}
	source, err := json.Marshal(document)
	if err != nil {
		responses.WriteOK(w, &ports.RuleCheckResult{Error: err.Error()})
		return
	}
	responses.WriteOK(w, checker.CheckRule(r.Context(), middleware.GetTenantID(r.Context()), req.EventType, source, req.Event))
}
