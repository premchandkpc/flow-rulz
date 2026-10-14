package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/auth"
)

func TestRequireContractAdminRejectsTenantOnlyIdentityInProduction(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	ctx := auth.ContextWithTenant(httptest.NewRequest(http.MethodPost, "/v1/contracts", nil).Context(), "tenant-a")
	req := httptest.NewRequest(http.MethodPost, "/v1/contracts", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	if requireContractAdmin(rec, req) {
		t.Fatal("requireContractAdmin() = true without admin scope")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestRequireContractAdminAcceptsExplicitAdminScopeInProduction(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	ctx := auth.ContextWithClaims(httptest.NewRequest(http.MethodPost, "/v1/contracts", nil).Context(), &auth.Claims{
		TenantID: "tenant-a",
		Scopes:   []string{"contracts:admin"},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/contracts", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	if !requireContractAdmin(rec, req) {
		t.Fatalf("requireContractAdmin() = false, status=%d", rec.Code)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want no error response", rec.Code)
	}
}

func TestRequireContractAdminAllowsLocalDevelopment(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	req := httptest.NewRequest(http.MethodPost, "/v1/contracts", nil)
	rec := httptest.NewRecorder()

	if !requireContractAdmin(rec, req) {
		t.Fatalf("requireContractAdmin() = false in development, status=%d", rec.Code)
	}
}

func TestRequireContractAdminFailsClosedWhenAppEnvIsUnset(t *testing.T) {
	t.Setenv("APP_ENV", "")
	req := httptest.NewRequest(http.MethodPost, "/v1/contracts", nil)
	rec := httptest.NewRecorder()

	if requireContractAdmin(rec, req) {
		t.Fatal("requireContractAdmin() = true when APP_ENV is unset")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}
