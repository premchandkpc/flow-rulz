package handlers

// Query parameter names are part of the HTTP API contract.
const (
	queryParamLimit      = "limit"
	queryParamOffset     = "offset"
	queryParamFrom       = "from"
	queryParamTo         = "to"
	queryParamRuleSet    = "rule_set"
	queryParamSourceType = "source_type"
	queryParamErrorClass = "error_class"
	queryParamState      = "state"
	queryParamVersion    = "version"
)

const (
	defaultPageLimit         = 100
	maxPageLimit             = 500
	maxWorkflowPageLimit     = 1000
	maxPageOffset            = 10000
	maxRequestBodyBytes      = 1 << 20
	headerContentType        = "Content-Type"
	mediaTypeYAML            = "yaml"
	mediaTypeYML             = "yml"
	messageNotFound          = "not found"
	messageTenantRequired    = "authenticated tenant identity is required"
	messageTenantMismatch    = "tenant_id must match the request tenant"
	messageInvalidBodyPrefix = "invalid request body: "
	generatedOutputRootName  = "flowrulz-generated"
)
