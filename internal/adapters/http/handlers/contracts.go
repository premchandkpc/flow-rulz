package handlers

import (
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
	"github.com/flowrulz/flow-rulz/internal/auth"
	"github.com/flowrulz/flow-rulz/internal/codegen"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

// ContractHandler handles HTTP requests for contract operations.
type ContractHandler struct {
	registry ports.ContractRegistry
}

// NewContractHandler creates a new ContractHandler.
func NewContractHandler(registry ports.ContractRegistry) *ContractHandler {
	return &ContractHandler{registry: registry}
}

// RegisterRoutes registers the HTTP routes for the ContractHandler.
func (h *ContractHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/contracts", h.CreateContract)
	mux.HandleFunc("GET /v1/contracts", h.ListContracts)
	mux.HandleFunc("GET /v1/contracts/{name}", h.GetContract)
	mux.HandleFunc("GET /v1/contracts/{name}/{version}", h.GetContractVersion)
	mux.HandleFunc("DELETE /v1/contracts/{name}/{version}", h.DeleteContract)
	mux.HandleFunc("POST /v1/contracts/{name}/{version}/generate", h.GenerateContract)
}

// CreateContract handles POST /v1/contracts requests.
func (h *ContractHandler) CreateContract(w http.ResponseWriter, r *http.Request) {
	if !requireContractAdmin(w, r) {
		return
	}
	var schema domain.ContractSchema
	if err := decodeRequestBody(r, &schema); err != nil {
		responses.WriteError(w, http.StatusBadRequest, messageInvalidBodyPrefix+err.Error(), responses.CodeInvalidJSON)
		return
	}

	if schema.Name == "" {
		responses.WriteError(w, http.StatusBadRequest, "name is required", responses.CodeValidationFailed)
		return
	}
	if schema.Version == "" {
		responses.WriteError(w, http.StatusBadRequest, "version is required", responses.CodeValidationFailed)
		return
	}
	if err := schema.NormalizeAndValidate(); err != nil {
		responses.WriteError(w, http.StatusBadRequest, err.Error(), responses.CodeValidationFailed)
		return
	}

	if err := h.registry.Register(r.Context(), &schema); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteCreated(w, schema)
}

// ListContracts handles GET /v1/contracts requests.
func (h *ContractHandler) ListContracts(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")

	schemas, err := h.registry.List(r.Context(), name)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteOK(w, schemas)
}

// GetContract handles GET /v1/contracts/{name} requests.
func (h *ContractHandler) GetContract(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.URL.Query().Get(queryParamVersion)

	if version == "" {
		responses.WriteError(w, http.StatusBadRequest, "version query parameter is required", responses.CodeValidationFailed)
		return
	}

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if schema == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, schema)
}

// GetContractVersion handles GET /v1/contracts/{name}/{version} requests.
func (h *ContractHandler) GetContractVersion(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	version := r.PathValue("version")

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if schema == nil {
		responses.WriteError(w, http.StatusNotFound, "not found", responses.CodeNotFound)
		return
	}

	responses.WriteOK(w, schema)
}

// DeleteContract handles DELETE /v1/contracts/{name}/{version} requests.
func (h *ContractHandler) DeleteContract(w http.ResponseWriter, r *http.Request) {
	if !requireContractAdmin(w, r) {
		return
	}
	name := r.PathValue("name")
	version := r.PathValue("version")

	if err := h.registry.Delete(r.Context(), name, version); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}

	responses.WriteNoContent(w)
}

type generateContractRequest struct {
	Language string `json:"language"`
	Output   string `json:"output_dir,omitempty"`
}

var errUnsupportedContractLanguage = errors.New("unsupported language")

// GenerateContract handles POST /v1/contracts/{name}/{version}/generate requests.
func (h *ContractHandler) GenerateContract(w http.ResponseWriter, r *http.Request) {
	if !requireContractAdmin(w, r) {
		return
	}
	name, version := r.PathValue("name"), r.PathValue("version")
	req, ok := decodeGenerateContractRequest(w, r)
	if !ok {
		return
	}
	if !validateGenerateContractRequest(w, req) {
		return
	}

	schema, err := h.registry.Get(r.Context(), name, version)
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	if schema == nil {
		responses.WriteError(w, http.StatusNotFound, "contract not found", responses.CodeNotFound)
		return
	}

	outputDir, ok := prepareGeneratedOutput(w)
	if !ok {
		return
	}
	generatedFiles, err := generateContractFiles(codegen.NewGenerator(schema), req.Language, outputDir)
	if errors.Is(err, errUnsupportedContractLanguage) {
		responses.WriteError(w, http.StatusBadRequest, "unsupported language", responses.CodeValidationFailed)
		return
	}
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, err.Error(), responses.CodeInternalError)
		return
	}
	writeGeneratedContractResponse(w, name, version, req.Language, outputDir, generatedFiles)
}

func decodeGenerateContractRequest(w http.ResponseWriter, r *http.Request) (generateContractRequest, bool) {
	var req generateContractRequest
	if err := decodeRequestBody(r, &req); err != nil {
		responses.WriteError(w, http.StatusBadRequest, "invalid request body: "+err.Error(), responses.CodeInvalidJSON)
		return generateContractRequest{}, false
	}
	return req, true
}

func validateGenerateContractRequest(w http.ResponseWriter, req generateContractRequest) bool {
	if req.Language == "" {
		responses.WriteError(w, http.StatusBadRequest, "language is required ("+codegen.SupportedTargetsText+")", responses.CodeValidationFailed)
		return false
	}
	if req.Output != "" {
		responses.WriteError(w, http.StatusBadRequest, "output_dir is not supported; generated files use a server-managed directory", responses.CodeValidationFailed)
		return false
	}
	return true
}

func prepareGeneratedOutput(w http.ResponseWriter) (string, bool) {
	allowedBase := filepath.Join(os.TempDir(), generatedOutputRootName)
	if err := os.MkdirAll(allowedBase, 0o700); err != nil {
		responses.WriteError(w, http.StatusInternalServerError, "could not prepare output root", responses.CodeInternalError)
		return "", false
	}
	rootInfo, err := os.Lstat(allowedBase)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		responses.WriteError(w, http.StatusInternalServerError, "generation output root is invalid", responses.CodeInternalError)
		return "", false
	}
	if err := os.Chmod(allowedBase, 0o700); err != nil { // #nosec G302 -- directory requires execute permission; 0700 restricts access to its owner.
		responses.WriteError(w, http.StatusInternalServerError, "could not secure output root", responses.CodeInternalError)
		return "", false
	}
	outputDir, err := os.MkdirTemp(allowedBase, "generation-")
	if err != nil {
		responses.WriteError(w, http.StatusInternalServerError, "could not prepare output directory", responses.CodeInternalError)
		return "", false
	}
	return outputDir, true
}

func generateContractFiles(generator *codegen.Generator, language, outputDir string) ([]string, error) {
	switch strings.ToLower(language) {
	case codegen.TargetGo:
		return generator.GenerateGo(outputDir)
	case codegen.TargetJava:
		return generator.GenerateJava(outputDir)
	case codegen.TargetProtobuf:
		return generator.GenerateProtobuf(outputDir)
	case codegen.TargetJSONSchema:
		return generator.GenerateJSONSchema(outputDir)
	default:
		return nil, errUnsupportedContractLanguage
	}
}

func writeGeneratedContractResponse(
	w http.ResponseWriter,
	name, version, language, outputDir string,
	generatedFiles []string,
) {
	responses.WriteOK(w, map[string]interface{}{
		"contract":        name,
		"version":         version,
		"language":        language,
		"output_dir":      outputDir,
		"generated_files": generatedFiles,
	})
}

// requireContractAdmin protects global contract mutations and code generation.
// Tenant identity alone is not sufficient authorization for platform-wide data.
func requireContractAdmin(w http.ResponseWriter, r *http.Request) bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("APP_ENV")))
	if env == "" || env == "development" || env == "dev" || env == "test" {
		return true
	}
	if auth.HasScope(r.Context(), "contracts:admin") ||
		auth.HasScope(r.Context(), "flowrulz:admin") ||
		auth.HasScope(r.Context(), "admin") {
		return true
	}
	responses.WriteError(w, http.StatusForbidden, "contracts:admin scope is required for global contract mutations and code generation", responses.CodeForbidden)
	return false
}
