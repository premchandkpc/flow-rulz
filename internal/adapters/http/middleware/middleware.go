// Package middleware provides HTTP middleware for request context and authentication.
package middleware

import (
	"context"
	"crypto/subtle"
	"net/http"
	"os"
	"strings"
	"unicode"

	"github.com/flowrulz/flow-rulz/internal/auth"
)

// TenantContextKey is the context key for tenant information.
type TenantContextKey string

const (
	// TenantIDKey identifies the tenant value stored in request context.
	TenantIDKey TenantContextKey = "tenant_id"

	trustedProxyTokenEnv    = "FLOWRULZ_TRUSTED_PROXY_TOKEN"
	trustedProxyTokenHeader = "X-Proxy-Auth" //nolint:gosec // header name, not credentials
	trustedTenantHeader     = "X-Authenticated-Tenant-ID"
)

// TenantExtractor establishes tenant context. Development and test environments
// may use caller-provided tenant IDs for local testing only. Production requires
// a trusted proxy that authenticates callers and overwrites the trusted headers.
func TenantExtractor(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}
		if isGlobalConfigurationRoute(r.URL.Path) {
			// Global configuration still requires control-plane authentication, but
			// does not require a tenant identity. Runtime tenant identity is on events.
			if status := globalConfigurationAuthStatus(r); status != http.StatusOK {
				if status == http.StatusServiceUnavailable {
					http.Error(w, "global configuration authentication is not configured", status)
				} else {
					http.Error(w, "authenticated control-plane identity is required", status)
				}
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		tenantID, status := tenantForRequest(r)
		if status != http.StatusOK {
			if status == http.StatusServiceUnavailable {
				http.Error(w, "tenant authentication is not configured", status)
				return
			}
			http.Error(w, "authenticated tenant identity is required", status)
			return
		}

		ctx := context.WithValue(r.Context(), TenantIDKey, tenantID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func globalConfigurationAuthStatus(r *http.Request) int {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("APP_ENV")))
	if env == "development" || env == "dev" || env == "test" {
		return http.StatusOK
	}
	// A validated JWT is accepted when auth middleware is enabled. This legacy
	// validator requires a tenant claim; the handler itself does not consume it.
	if _, ok := auth.TenantFromContext(r.Context()); ok {
		return http.StatusOK
	}
	expected := os.Getenv(trustedProxyTokenEnv)
	if expected == "" {
		return http.StatusServiceUnavailable
	}
	provided := r.Header.Get(trustedProxyTokenHeader)
	if len(provided) != len(expected) || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		return http.StatusUnauthorized
	}
	return http.StatusOK
}

func isGlobalConfigurationRoute(path string) bool {
	return path == "/v1/domains" || strings.HasPrefix(path, "/v1/domains/") ||
		path == "/v1/subscriptions" || strings.HasPrefix(path, "/v1/subscriptions/")
}

func tenantForRequest(r *http.Request) (string, int) {
	// A validated JWT claim is the authoritative tenant identity. Caller-supplied
	// headers/query parameters may not override it.
	if tenantID, ok := auth.TenantFromContext(r.Context()); ok {
		tenantID = strings.TrimSpace(tenantID)
		if !validTenantID(tenantID) {
			return "", http.StatusUnauthorized
		}
		if supplied := strings.TrimSpace(r.Header.Get("X-Tenant-ID")); supplied != "" && supplied != tenantID {
			return "", http.StatusForbidden
		}
		if supplied := strings.TrimSpace(r.URL.Query().Get("tenant_id")); supplied != "" && supplied != tenantID {
			return "", http.StatusForbidden
		}
		return tenantID, http.StatusOK
	}
	env := strings.ToLower(strings.TrimSpace(os.Getenv("APP_ENV")))
	if env == "development" || env == "dev" || env == "test" {
		tenantID := strings.TrimSpace(r.Header.Get("X-Tenant-ID"))
		if tenantID == "" {
			tenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
		}
		if tenantID == "" {
			tenantID = "default"
		}
		if !validTenantID(tenantID) {
			return "", http.StatusBadRequest
		}
		return tenantID, http.StatusOK
	}

	expected := os.Getenv(trustedProxyTokenEnv)
	if expected == "" {
		return "", http.StatusServiceUnavailable
	}
	provided := r.Header.Get(trustedProxyTokenHeader)
	if len(provided) != len(expected) || subtle.ConstantTimeCompare([]byte(provided), []byte(expected)) != 1 {
		return "", http.StatusUnauthorized
	}

	tenantID := strings.TrimSpace(r.Header.Get(trustedTenantHeader))
	if !validTenantID(tenantID) {
		return "", http.StatusUnauthorized
	}
	return tenantID, http.StatusOK
}

func validTenantID(tenantID string) bool {
	if tenantID == "" || len(tenantID) > 128 {
		return false
	}
	for _, r := range tenantID {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// GetTenantID returns the tenant established by middleware. An empty result
// means no trusted tenant context was established; it never invents a default.
func GetTenantID(ctx context.Context) string {
	if tenant, ok := ctx.Value(TenantIDKey).(string); ok {
		return tenant
	}
	return ""
}

// Recovery middleware recovers from panics.
func Recovery(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if recover() != nil {
				http.Error(w, "{\"error\":\"internal server error\"}", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
