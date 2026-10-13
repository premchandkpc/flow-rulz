// Package responses writes standardized HTTP API responses.
package responses

import (
	"encoding/json"
	"net/http"
)

// ErrorResponse represents a standardized API error response
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}

// SuccessResponse represents a standardized success response
type SuccessResponse struct {
	Status string      `json:"status"`
	Data   interface{} `json:"data,omitempty"`
}

// WriteError writes a standardized error response
func WriteError(w http.ResponseWriter, status int, message string, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: message, Code: code})
}

// WriteSuccess writes a standardized success response
func WriteSuccess(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(SuccessResponse{Status: "ok", Data: data})
}

// WriteCreated writes a 201 Created response
func WriteCreated(w http.ResponseWriter, data interface{}) {
	WriteSuccess(w, http.StatusCreated, data)
}

// WriteOK writes a 200 OK response
func WriteOK(w http.ResponseWriter, data interface{}) {
	WriteSuccess(w, http.StatusOK, data)
}

// WriteNoContent writes a 204 No Content response
func WriteNoContent(w http.ResponseWriter) {
	w.WriteHeader(http.StatusNoContent)
}

// Common error codes
const (
	CodeInvalidJSON       = "INVALID_JSON"
	CodeNotFound          = "NOT_FOUND"
	CodeInternalError     = "INTERNAL_ERROR"
	CodeBadRequest        = "BAD_REQUEST"
	CodeUnauthorized      = "UNAUTHORIZED"
	CodeForbidden         = "FORBIDDEN"
	CodeConflict          = "CONFLICT"
	CodeTenantRequired    = "TENANT_REQUIRED"
	CodeValidationFailed  = "VALIDATION_FAILED"
	CodeActivationFailed  = "ACTIVATION_FAILED"
	CodeTransitionFailed  = "TRANSITION_FAILED"
	CodeSaveFailed        = "SAVE_FAILED"
	CodeDeleteFailed      = "DELETE_FAILED"
	CodeNotImplemented    = "NOT_IMPLEMENTED"
	CodeInvalidVersion    = "INVALID_VERSION"
	CodeWorkflowTypeReq   = "WORKFLOW_TYPE_REQUIRED"
	CodeInvalidTransition = "INVALID_TRANSITION"
)
