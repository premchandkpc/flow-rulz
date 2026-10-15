package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/auth"
)

func TestTenantExtractorDevelopmentUsesEventTenantHeader(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("X-Tenant-ID", "tenant-a")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetTenantID(r.Context()); got != "tenant-a" {
			t.Fatalf("GetTenantID() = %q, want tenant-a", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestTenantExtractorMissingAppEnvFailsClosedForRuntimeRoute(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("X-Tenant-ID", "attacker-selected-tenant")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not be called when APP_ENV is unset")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestTenantExtractorProductionRejectsCallerTenantHeaderForRuntimeRoute(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("X-Tenant-ID", "victim")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not be called without trusted proxy credentials")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestTenantExtractorProductionUsesTrustedTenantHeaderForRuntimeRoute(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set(trustedProxyTokenHeader, "proxy-secret")
	req.Header.Set(trustedTenantHeader, "tenant-a")
	req.Header.Set("X-Tenant-ID", "attacker-selected-tenant")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetTenantID(r.Context()); got != "tenant-a" {
			t.Fatalf("GetTenantID() = %q, want tenant-a", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestTenantExtractorProductionFailsClosedWhenRuntimeTenantAuthNotConfigured(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not be called when production tenant auth is unconfigured")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestTenantExtractorJWTClaimOverridesCallerTenantForRuntimeRoute(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req = req.WithContext(auth.ContextWithTenant(req.Context(), "tenant-authenticated"))
	req.Header.Set("X-Tenant-ID", "tenant-attacker")
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("handler must not be called when caller tenant conflicts with JWT")
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestTenantExtractorUsesJWTTenantWithoutProxyFallbackForRuntimeRoute(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req = req.WithContext(auth.ContextWithTenant(req.Context(), "tenant-authenticated"))
	rec := httptest.NewRecorder()

	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetTenantID(r.Context()); got != "tenant-authenticated" {
			t.Fatalf("GetTenantID() = %q, want tenant-authenticated", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestGlobalConfigurationRoutesDoNotRequireTenantIdentity(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	t.Setenv(trustedProxyTokenEnv, "")
	for _, path := range []string{
		"/v1/domains",
		"/v1/domains/domain-orders",
		"/v1/subscriptions",
		"/v1/subscriptions/audit-consumer",
		"/v1/rules",
		"/v1/rules/order.created/revisions",
		"/v1/contracts",
		"/v1/contracts/OrderCreated/v1",
	} {
		t.Run(path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := GetTenantID(r.Context()); got != "" {
					t.Fatalf("global configuration route unexpectedly received tenant %q", got)
				}
				w.WriteHeader(http.StatusNoContent)
			})).ServeHTTP(rec, req)
			if rec.Code != http.StatusNoContent {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
			}
		})
	}
}

func TestGlobalConfigurationRoutesRequireTrustedAuthButNotTenantHeader(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", nil)
	req.Header.Set(trustedProxyTokenHeader, "proxy-secret")
	rec := httptest.NewRecorder()
	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetTenantID(r.Context()); got != "" {
			t.Fatalf("global configuration unexpectedly received tenant %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func TestGlobalConfigurationRoutesFailClosedWithoutControlPlaneAuth(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", nil)
	rec := httptest.NewRecorder()
	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("global configuration handler must not run without control-plane authentication")
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestGlobalConfigurationRejectsTenantOnlyJWT(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/domains", nil)
	claims := &auth.Claims{TenantID: "tenant-a"}
	req = req.WithContext(auth.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	TenantExtractor(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("tenant-only JWT must not authorize global configuration")
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestGlobalConfigurationAcceptsAdminScopeWithoutTenantRoutingContext(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "")
	req := httptest.NewRequest(http.MethodPost, "/v1/rules/order-review/revisions", nil)
	claims := &auth.Claims{TenantID: "admin-principal", Scopes: []string{"flowrulz:admin"}}
	req = req.WithContext(auth.ContextWithClaims(req.Context(), claims))
	rec := httptest.NewRecorder()
	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := GetTenantID(r.Context()); got != "" {
			t.Fatalf("global configuration unexpectedly received event tenant %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}
