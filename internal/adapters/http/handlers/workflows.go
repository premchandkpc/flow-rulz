package handlers

import (
	"context"
	"net/http"
	"strconv"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/middleware"
	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	authn "github.com/flowrulz/flow-rulz/internal/auth"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// WorkflowHandler handles HTTP requests for workflow operations.
type WorkflowHandler struct {
	workflowSvc     ports.WorkflowService
	ruleSvc         ports.RuleService
	workflowDefRepo ports.WorkflowDefinitionRepository
}

// NewWorkflowHandler constructs an HTTP handler with its required service dependencies.
func NewWorkflowHandler(workflowSvc ports.WorkflowService, ruleSvc ports.RuleService, workflowDefRepo ports.WorkflowDefinitionRepository) *WorkflowHandler {
	return &WorkflowHandler{
		workflowSvc:     workflowSvc,
		ruleSvc:         ruleSvc,
		workflowDefRepo: workflowDefRepo,
	}
}

// RegisterControlPlaneRoutes registers configuration and read-only workflow views.
// Runtime instance creation and transitions are worker-owned.
func (h *WorkflowHandler) RegisterRoutes(mux *http.ServeMux) {
	h.RegisterControlPlaneRoutes(mux)
}

func (h *WorkflowHandler) RegisterControlPlaneRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /v1/workflows/{workflowID}", h.GetWorkflow)
	mux.HandleFunc("GET /v1/workflows", h.ListWorkflows)
	mux.HandleFunc("GET /v1/workflows/definitions", h.ListWorkflowDefinitions)
	mux.HandleFunc("GET /v1/workflows/definitions/{workflowType}", h.GetWorkflowDefinition)
	mux.HandleFunc("GET /v1/workflows/definitions/{workflowType}/{version}", h.GetWorkflowDefinition)
	mux.Handle("POST /v1/workflows/definitions", http.HandlerFunc(h.createWorkflowDefinitionAsAdmin))
}

// RegisterRoutes preserves the complete handler route set for focused handler tests.
// The production API router must use RegisterControlPlaneRoutes.
func (h *WorkflowHandler) RegisterAllRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/workflows", h.CreateWorkflow)
	mux.HandleFunc("GET /v1/workflows/{workflowID}", h.GetWorkflow)
	mux.HandleFunc("POST /v1/workflows/{workflowID}/transition", h.TransitionWorkflow)
	mux.HandleFunc("GET /v1/workflows", h.ListWorkflows)
	mux.HandleFunc("GET /v1/workflows/definitions", h.ListWorkflowDefinitions)
	mux.HandleFunc("GET /v1/workflows/definitions/{workflowType}", h.GetWorkflowDefinition)
	mux.HandleFunc("GET /v1/workflows/definitions/{workflowType}/{version}", h.GetWorkflowDefinition)
}

// createWorkflowDefinitionAsAdmin restricts global workflow configuration to platform admins.
func (h *WorkflowHandler) createWorkflowDefinitionAsAdmin(w http.ResponseWriter, r *http.Request) {
	if !authn.HasScope(r.Context(), "admin") && !authn.HasScope(r.Context(), "flowrulz:admin") {
		responses.WriteError(w, http.StatusForbidden, "platform admin scope is required", responses.CodeForbidden)
		return
	}
	h.CreateWorkflowDefinition(w, r)
}

// CreateWorkflow creates a workflow instance for the current tenant.
func (h *WorkflowHandler) CreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var wf domain.WorkflowInstance
	if err := decodeRequestBody(r, &wf); err != nil {
		responses.WriteError(w, http.StatusBadRequest, messageInvalidBodyPrefix+err.Error(), responses.CodeInvalidJSON)
		return
	}

	tenantID := middleware.GetTenantID(r.Context())
	if wf.TenantID != "" && wf.TenantID != tenantID {
		responses.WriteError(w, http.StatusForbidden, messageTenantMismatch, responses.CodeForbidden)
		return
	}
	wf.TenantID = tenantID
	if wf.TenantID == "" {
		responses.WriteError(w, http.StatusBadRequest, "tenant_id is required", responses.CodeTenantRequired)
		return
	}
	if wf.WorkflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflow_type is required", responses.CodeWorkflowTypeReq)
		return
	}
	if wf.State == "" {
		wf.State = domain.WorkflowStatusPending
	}

	ctx := r.Context()
	if err := h.workflowSvc.Create(ctx, &wf); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteCreated(w, wf)
}

// GetWorkflow returns a workflow instance visible to the current tenant.
func (h *WorkflowHandler) GetWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowID is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	wf, err := h.workflowSvc.Get(ctx, middleware.GetTenantID(ctx), workflowID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if wf == nil || wf.TenantID != middleware.GetTenantID(r.Context()) {
		responses.WriteError(w, http.StatusNotFound, messageNotFound, responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, wf)
}

type workflowTransitionRequest struct {
	FromState string `json:"from_state"`
	ToState   string `json:"to_state"`
}

// TransitionWorkflow transitions a workflow instance to a new state.
func (h *WorkflowHandler) TransitionWorkflow(w http.ResponseWriter, r *http.Request) {
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowID is required", responses.CodeBadRequest)
		return
	}
	request, err := decodeWorkflowTransitionRequest(r)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}
	if request.ToState == "" {
		responses.WriteError(w, http.StatusBadRequest, "to_state is required", responses.CodeBadRequest)
		return
	}

	ctx := r.Context()
	tenantID := middleware.GetTenantID(ctx)
	workflow, err := h.loadWorkflowForTransition(ctx, tenantID, workflowID)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if workflow == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}
	if request.FromState != "" && request.FromState != workflow.State {
		responses.WriteError(w, http.StatusConflict, "from_state does not match current workflow state", responses.CodeConflict)
		return
	}
	if err := h.applyWorkflowTransition(ctx, tenantID, workflowID, request); err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeTransitionFailed)
		return
	}
	responses.WriteOK(w, map[string]string{"status": "ok"})
}

func decodeWorkflowTransitionRequest(r *http.Request) (workflowTransitionRequest, error) {
	var request workflowTransitionRequest
	if err := decodeRequestBody(r, &request); err != nil {
		return workflowTransitionRequest{}, err
	}
	return request, nil
}

func (h *WorkflowHandler) loadWorkflowForTransition(
	ctx context.Context,
	tenantID string,
	workflowID string,
) (*domain.WorkflowInstance, error) {
	workflow, err := h.workflowSvc.Get(ctx, tenantID, workflowID)
	if err != nil {
		return nil, err
	}
	if workflow == nil || workflow.TenantID != tenantID {
		return nil, nil
	}
	return workflow, nil
}

func (h *WorkflowHandler) applyWorkflowTransition(
	ctx context.Context,
	tenantID string,
	workflowID string,
	request workflowTransitionRequest,
) error {
	if request.FromState != "" {
		return h.workflowSvc.Transition(ctx, tenantID, workflowID, request.FromState, request.ToState)
	}
	return h.workflowSvc.UpdateState(ctx, tenantID, workflowID, request.ToState, 0)
}

// ListWorkflows lists workflow instances for the current tenant.
func (h *WorkflowHandler) ListWorkflows(w http.ResponseWriter, r *http.Request) {
	tenantID := middleware.GetTenantID(r.Context())
	state := r.URL.Query().Get(queryParamState)
	pagination, err := parsePagination(r, defaultPageLimit, maxWorkflowPageLimit, maxPageOffset)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}
	limit, offset := pagination.Limit, pagination.Offset

	ctx := r.Context()
	var workflows []*domain.WorkflowInstance

	fetchLimit := limit + offset
	if state != "" {
		workflows, err = h.workflowSvc.GetByTenantAndState(ctx, tenantID, state, fetchLimit)
	} else {
		workflows, err = h.workflowSvc.GetByTenantAndState(ctx, tenantID, "", fetchLimit)
	}
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	if offset > 0 && offset < len(workflows) {
		workflows = workflows[offset:]
	} else if offset >= len(workflows) {
		workflows = []*domain.WorkflowInstance{}
	}

	responses.WriteOK(w, map[string]interface{}{
		"workflows": workflows,
		"total":     len(workflows),
		"limit":     limit,
		"offset":    offset,
	})
}

// ListWorkflowDefinitions lists registered workflow definitions.
func (h *WorkflowHandler) ListWorkflowDefinitions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	defs, err := h.workflowDefRepo.List(ctx)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, map[string]interface{}{
		"definitions": defs,
		"total":       len(defs),
	})
}

// CreateWorkflowDefinition creates a global platform workflow definition. The control-plane
// router exposes this operation only to callers with a platform admin scope.
func (h *WorkflowHandler) CreateWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	var def domain.WorkflowDefinition
	if err := decodeRequestBody(r, &def); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return
	}

	if def.WorkflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflow_type is required", responses.CodeWorkflowTypeReq)
		return
	}
	if def.Version <= 0 {
		responses.WriteError(w, http.StatusBadRequest, "version must be > 0", responses.CodeInvalidVersion)
		return
	}

	ctx := r.Context()
	if err := h.workflowDefRepo.Save(ctx, &def); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeSaveFailed)
		return
	}

	responses.WriteCreated(w, def)
}

// GetWorkflowDefinition returns a registered workflow definition.
func (h *WorkflowHandler) GetWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflowType")
	versionStr := r.PathValue("version")
	if versionStr == "" {
		versionStr = r.URL.Query().Get(queryParamVersion)
	}

	if workflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowType is required", responses.CodeBadRequest)
		return
	}

	var version int64
	if versionStr != "" {
		var err error
		version, err = strconv.ParseInt(versionStr, 10, 64)
		if err != nil {
			responses.WriteError(w, http.StatusBadRequest, "invalid version", responses.CodeInvalidVersion)
			return
		}
	}

	ctx := r.Context()
	var def *domain.WorkflowDefinition
	var err error

	if version > 0 {
		def, err = h.workflowDefRepo.Get(ctx, workflowType, version)
	} else {
		defs, listErr := h.workflowDefRepo.List(ctx)
		if listErr != nil {
			err = listErr
		} else {
			for _, candidate := range defs {
				if candidate.WorkflowType == workflowType && (def == nil || candidate.Version > def.Version) {
					def = candidate
				}
			}
		}
	}
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if def == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, def)
}

// DeleteWorkflowDefinition is intentionally not registered on the tenant-facing router.
func (h *WorkflowHandler) DeleteWorkflowDefinition(w http.ResponseWriter, r *http.Request) {
	workflowType := r.PathValue("workflowType")
	versionStr := r.PathValue("version")

	if workflowType == "" {
		responses.WriteError(w, http.StatusBadRequest, "workflowType is required", responses.CodeBadRequest)
		return
	}

	version, err := strconv.ParseInt(versionStr, 10, 64)
	if err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid version", responses.CodeInvalidVersion)
		return
	}

	ctx := r.Context()
	if err := h.workflowDefRepo.Delete(ctx, workflowType, version); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeDeleteFailed)
		return
	}

	responses.WriteOK(w, map[string]string{"status": "deleted"})
}
